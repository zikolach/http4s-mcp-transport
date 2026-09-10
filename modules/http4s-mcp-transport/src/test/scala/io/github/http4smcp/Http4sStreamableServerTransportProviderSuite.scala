package io.github.http4smcp

import cats.effect.Deferred
import cats.effect.IO
import cats.effect.Ref
import cats.effect.kernel.Outcome
import cats.effect.std.Semaphore
import cats.syntax.all._
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import fs2.Chunk
import fs2.Stream
import io.github.http4smcp.internal.ReactorInterop
import io.modelcontextprotocol.common.McpTransportContext
import io.modelcontextprotocol.json.McpJsonDefaults
import io.modelcontextprotocol.server.McpAsyncServerExchange
import io.modelcontextprotocol.server.McpNotificationHandler
import io.modelcontextprotocol.server.McpRequestHandler
import io.modelcontextprotocol.server.transport.ServerTransportSecurityException
import io.modelcontextprotocol.server.transport.ServerTransportSecurityValidator
import io.modelcontextprotocol.spec.HttpHeaders
import io.modelcontextprotocol.spec.McpSchema
import io.modelcontextprotocol.spec.McpStreamableServerSession
import io.modelcontextprotocol.spec.McpStreamableServerTransport
import io.modelcontextprotocol.spec.ProtocolVersions
import munit.CatsEffectSuite
import org.http4s.Header
import org.http4s.Method
import org.http4s.Request
import org.http4s.Status
import org.http4s.Uri
import org.http4s.syntax.all._
import org.slf4j.LoggerFactory
import org.typelevel.ci.CIString
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks

import java.time.Duration
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

final class Http4sStreamableServerTransportProviderSuite extends CatsEffectSuite {
  private val mapper          = McpJsonDefaults.getMapper()
  private val MaxRequestBytes = 16 * 1024 * 1024

  LoggerFactory.getLogger(classOf[McpSchema]).asInstanceOf[Logger].setLevel(Level.INFO)

  test("initialize creates a session and returns Mcp-Session-Id") {
    for {
      provider <- testProvider()
      response <- provider.routes.orNotFound.run(post(initializeJson))
      body     <- response.as[String]
    } yield {
      assertEquals(response.status, Status.Ok)
      assert(response.headers.get(CIString(HttpHeaders.MCP_SESSION_ID)).nonEmpty)
      assert(body.contains("protocolVersion"))
      assert(body.contains("test-server"))
    }
  }

  test("session notification POST returns Accepted") {
    for {
      seen      <- Ref.of[IO, Int](0)
      provider  <- testProvider(notificationHandler = Some(_ => seen.update(_ + 1)))
      sessionId <- initialize(provider)
      response  <- provider.routes.orNotFound.run(post(notificationJson, Some(sessionId)))
      count     <- seen.get
    } yield {
      assertEquals(response.status, Status.Accepted)
      assertEquals(count, 1)
    }
  }

  test("session request POST returns an SSE message") {
    for {
      provider  <- testProvider()
      sessionId <- initialize(provider)
      response  <- provider.routes.orNotFound.run(post(requestJson, Some(sessionId)))
      body      <- response.bodyText.compile.string
    } yield {
      assertEquals(response.status, Status.Ok)
      assert(body.contains("event: message"))
      assert(body.contains("request-ok"))
    }
  }

  test("GET stream receives notifyClient message") {
    for {
      listening <- Deferred[IO, Unit]
      provider  <- testProvider(listeningStarted = Some(listening.complete(()).void))
      sessionId <- initialize(provider)
      response  <- provider.routes.orNotFound.run(get(Some(sessionId)))
      read      <- response.bodyText.interruptAfter(1.second).compile.string.start
      _         <- listening.get.timeout(1.second)
      _         <- ReactorInterop.monoCompletionToIO(
        provider.notifyClient(sessionId, "server/notice", java.util.Map.of("ok", "true"))
      )
      _    <- ReactorInterop.monoCompletionToIO(provider.closeGracefully())
      body <- read.joinWithNever
    } yield {
      assertEquals(response.status, Status.Ok)
      assert(body.contains("event: message"))
      assert(body.contains("server/notice"))
    }
  }

  test("notifyClients isolates failed delivery") {
    for {
      firstListening  <- Deferred[IO, Unit]
      secondListening <- Deferred[IO, Unit]
      first           <- testProvider(listeningStarted = Some(firstListening.complete(()).void))
      second          <- testProvider(listeningStarted = Some(secondListening.complete(()).void))
      firstSession    <- initialize(first)
      secondSession   <- initialize(second)
      firstResponse   <- first.routes.orNotFound.run(get(Some(firstSession)))
      secondResponse  <- second.routes.orNotFound.run(get(Some(secondSession)))
      firstRead       <- firstResponse.bodyText.interruptAfter(1.second).compile.string.start
      secondBodyRef   <- Ref.of[IO, String]("")
      broadcastSeen   <- Deferred[IO, Unit]
      secondRead      <- readBodyAndSignal(
        secondResponse,
        secondBodyRef,
        List("broadcast" -> broadcastSeen)
      )
      _ <- firstListening.get.timeout(1.second)
      _ <- secondListening.get.timeout(1.second)
      _ <- ReactorInterop.monoCompletionToIO(first.closeGracefully())
      _ <- ReactorInterop.monoCompletionToIO(
        second.notifyClients("broadcast", java.util.Map.of("ok", "true"))
      )
      _          <- broadcastSeen.get.timeout(1.second)
      _          <- ReactorInterop.monoCompletionToIO(second.closeGracefully())
      firstBody  <- firstRead.joinWithNever
      _          <- secondRead.cancel
      secondBody <- secondBodyRef.get
    } yield {
      assert(firstBody.isEmpty || firstBody.contains("event: message") == false)
      assert(secondBody.contains("broadcast"))
    }
  }

  test("DELETE closes and removes a session") {
    for {
      provider  <- testProvider()
      sessionId <- initialize(provider)
      deleted   <- provider.routes.orNotFound.run(delete(Some(sessionId)))
      unknown   <- provider.routes.orNotFound.run(post(notificationJson, Some(sessionId)))
    } yield {
      assertEquals(deleted.status, Status.Ok)
      assertEquals(unknown.status, Status.NotFound)
    }
  }

  test("invalid JSON, missing session, unknown session, and unsupported method are rejected") {
    for {
      provider    <- testProvider()
      badJson     <- provider.routes.orNotFound.run(post("not-json"))
      missing     <- provider.routes.orNotFound.run(post(notificationJson))
      unknown     <- provider.routes.orNotFound.run(post(notificationJson, Some("missing")))
      unsupported <- provider.routes.orNotFound.run(
        Request[IO](method = Method.PUT, uri = uri"/mcp")
      )
    } yield {
      assertEquals(badJson.status, Status.BadRequest)
      assertEquals(missing.status, Status.BadRequest)
      assertEquals(unknown.status, Status.NotFound)
      assertEquals(unsupported.status, Status.MethodNotAllowed)
    }
  }

  test("GET with Last-Event-ID asks the SDK session to replay before live stream") {
    for {
      replayed  <- Ref.of[IO, Option[String]](None)
      listening <- Deferred[IO, Unit]
      provider  <- testProvider(
        replayObserver = Some(id => replayed.set(Some(id.toString))),
        listeningStarted = Some(listening.complete(()).void)
      )
      sessionId <- initialize(provider)
      response  <- provider.routes.orNotFound.run(
        get(Some(sessionId)).putHeaders(Header.Raw(CIString(HttpHeaders.LAST_EVENT_ID), "last-1"))
      )
      bodyRef      <- Ref.of[IO, String]("")
      replayedBody <- Deferred[IO, Unit]
      afterBody    <- Deferred[IO, Unit]
      read         <- readBodyAndSignal(
        response,
        bodyRef,
        List("replayed" -> replayedBody, "after-replay" -> afterBody)
      )
      _ <- replayedBody.get.timeout(2.seconds)
      _ <- listening.get.timeout(2.seconds)
      _ <- ReactorInterop.monoCompletionToIO(
        provider.notifyClient(sessionId, "after-replay", java.util.Map.of("ok", "true"))
      )
      _    <- afterBody.get.timeout(2.seconds)
      _    <- ReactorInterop.monoCompletionToIO(provider.closeGracefully())
      _    <- read.cancel
      body <- bodyRef.get
      seen <- replayed.get
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(seen, Some("last-1"))
      assert(body.contains("replayed"))
      assert(body.contains("after-replay"))
      assert(body.indexOf("replayed") < body.indexOf("after-replay"))
    }
  }

  test("GET attaches live stream only after replay completes") {
    for {
      replayStarted <- Semaphore[IO](0)
      replayRelease <- IO(Sinks.empty[Void]())
      listening     <- Deferred[IO, Unit]
      provider      <- testProvider(
        replayObserver = Some(_ => replayStarted.release),
        replayGate = Some(replayRelease.asMono()),
        listeningStarted = Some(listening.complete(()).void)
      )
      sessionId <- initialize(provider)
      response  <- provider.routes.orNotFound.run(
        get(Some(sessionId)).putHeaders(
          Header.Raw(CIString(HttpHeaders.LAST_EVENT_ID), "last-delayed")
        )
      )
      bodyRef      <- Ref.of[IO, String]("")
      replayedBody <- Deferred[IO, Unit]
      afterBody    <- Deferred[IO, Unit]
      read         <- readBodyAndSignal(
        response,
        bodyRef,
        List("replayed" -> replayedBody, "after-replay" -> afterBody)
      )
      _ <- replayStarted.acquire.timeout(1.second)
      _ <- ReactorInterop
        .monoCompletionToIO(
          provider.notifyClient(sessionId, "during-replay", java.util.Map.of("ok", "true"))
        )
        .attempt
      _ <- IO(replayRelease.tryEmitEmpty()).void
      _ <- replayedBody.get.timeout(2.seconds)
      _ <- listening.get.timeout(2.seconds)
      _ <- ReactorInterop.monoCompletionToIO(
        provider.notifyClient(sessionId, "after-replay", java.util.Map.of("ok", "true"))
      )
      _    <- afterBody.get.timeout(2.seconds)
      _    <- ReactorInterop.monoCompletionToIO(provider.closeGracefully())
      _    <- read.cancel
      body <- bodyRef.get
    } yield {
      assertEquals(response.status, Status.Ok)
      assert(body.contains("replayed"))
      assert(!body.contains("during-replay"))
      assert(body.contains("after-replay"))
      assert(body.indexOf("replayed") < body.indexOf("after-replay"))
    }
  }

  test("client disconnect leaves the session available until DELETE") {
    for {
      provider            <- testProvider()
      sessionId           <- initialize(provider)
      response            <- provider.routes.orNotFound.run(get(Some(sessionId)))
      fiber               <- response.bodyText.interruptAfter(100.millis).compile.string.start
      _                   <- fiber.joinWithNever
      postAfterDisconnect <- provider.routes.orNotFound.run(post(notificationJson, Some(sessionId)))
      deleted             <- provider.routes.orNotFound.run(delete(Some(sessionId)))
    } yield {
      assertEquals(postAfterDisconnect.status, Status.Accepted)
      assertEquals(deleted.status, Status.Ok)
    }
  }

  test("POST JSON-RPC response is accepted for a known session") {
    val responseJson = """{"jsonrpc":"2.0","id":"client-response","result":{}}"""
    for {
      provider  <- testProvider()
      sessionId <- initialize(provider)
      response  <- provider.routes.orNotFound.run(post(responseJson, Some(sessionId)))
    } yield assertEquals(response.status, Status.Accepted)
  }

  test("notifyClients continues when one session stream has disconnected") {
    for {
      listening      <- Semaphore[IO](0)
      provider       <- testProvider(listeningStarted = Some(listening.release))
      firstSession   <- initialize(provider)
      secondSession  <- initialize(provider)
      firstResponse  <- provider.routes.orNotFound.run(get(Some(firstSession)))
      firstRead      <- firstResponse.bodyText.interruptAfter(100.millis).compile.string.start
      _              <- listening.acquire.timeout(1.second)
      _              <- firstRead.joinWithNever
      secondResponse <- provider.routes.orNotFound.run(get(Some(secondSession)))
      secondRead     <- secondResponse.bodyText.interruptAfter(1.second).compile.string.start
      _              <- listening.acquire.timeout(1.second)
      _              <- ReactorInterop.monoCompletionToIO(
        provider.notifyClients("broadcast", java.util.Map.of("ok", "true"))
      )
      _          <- ReactorInterop.monoCompletionToIO(provider.closeGracefully())
      secondBody <- secondRead.joinWithNever
    } yield {
      assertEquals(secondResponse.status, Status.Ok)
      assert(secondBody.contains("broadcast"))
    }
  }

  test("custom endpoint and delete-disabled configuration are honored") {
    for {
      provider <- testProvider(
        config =
          Http4sStreamableServerTransportProviderConfig(endpoint = "/custom", disallowDelete = true)
      )
      notFound    <- provider.routes.orNotFound.run(post(initializeJson))
      initialized <- provider.routes.orNotFound.run(
        post(initializeJson).withUri(Uri.unsafeFromString("/custom"))
      )
      sessionId = initialized.headers
        .get(CIString(HttpHeaders.MCP_SESSION_ID))
        .map(_.head.value)
        .get
      deleteResponse <- provider.routes.orNotFound.run(
        delete(Some(sessionId)).withUri(Uri.unsafeFromString("/custom"))
      )
    } yield {
      assertEquals(notFound.status, Status.NotFound)
      assertEquals(initialized.status, Status.Ok)
      assertEquals(deleteResponse.status, Status.MethodNotAllowed)
    }
  }

  test("transport context extractor metadata reaches session handlers") {
    for {
      seen     <- Ref.of[IO, Option[String]](None)
      provider <- testProvider(
        notificationHandler =
          Some(context => seen.set(Option(context.get("request-id")).map(_.toString))),
        config = Http4sStreamableServerTransportProviderConfig(
          contextExtractor = request =>
            McpTransportContext.create(
              java.util.Map.of(
                "request-id",
                request.headers.get(CIString("X-Request-Id")).map(_.head.value).getOrElse("")
              )
            )
        )
      )
      sessionId <- initialize(provider)
      response  <- provider.routes.orNotFound.run(
        post(notificationJson, Some(sessionId)).putHeaders(
          Header.Raw(CIString("X-Request-Id"), "req-42")
        )
      )
      value <- seen.get
    } yield {
      assertEquals(response.status, Status.Accepted)
      assertEquals(value, Some("req-42"))
    }
  }

  test("closeGracefully closes sessions and rejects new requests") {
    for {
      provider <- testProvider()
      _        <- initialize(provider)
      _        <- ReactorInterop.monoCompletionToIO(provider.closeGracefully())
      response <- provider.routes.orNotFound.run(post(initializeJson))
    } yield assertEquals(response.status, Status.ServiceUnavailable)
  }

  test("concurrent closeGracefully callers wait for the same session cleanup") {
    val gate          = Sinks.empty[Void]()
    val closeAttempts = new AtomicInteger(0)

    for {
      closeStarted     <- Deferred[IO, Unit]
      secondSubscribed <- Deferred[IO, Unit]
      secondCompleted  <- Deferred[IO, Unit]
      closeMono = gate
        .asMono()
        .doOnSubscribe { _ =>
          closeAttempts.incrementAndGet()
          closeStarted.complete(()).void.unsafeRunAndForget()
        }
      provider <- testProvider(closeGate = Some(closeMono))
      _        <- initialize(provider)
      first    <- ReactorInterop.monoCompletionToIO(provider.closeGracefully()).start
      _        <- closeStarted.get.timeout(2.seconds)
      second   <- ReactorInterop
        .monoCompletionToIO(
          provider
            .closeGracefully()
            .doOnSubscribe(_ => secondSubscribed.complete(()).void.unsafeRunAndForget())
            .doOnSuccess(_ => secondCompleted.complete(()).void.unsafeRunAndForget())
        )
        .start
      _               <- secondSubscribed.get.timeout(2.seconds)
      completedBefore <- secondCompleted.tryGet
      _               <- IO(assertEquals(completedBefore, None))
      _               <- IO(gate.tryEmitEmpty()).void
      _               <- first.joinWithNever.timeout(2.seconds)
      _               <- second.joinWithNever.timeout(2.seconds)
      _               <- secondCompleted.get.timeout(2.seconds)
    } yield assertEquals(closeAttempts.get(), 1)
  }

  test("canceling the first closeGracefully subscriber does not cancel cleanup") {
    val gate          = Sinks.empty[Void]()
    val closeAttempts = new AtomicInteger(0)

    for {
      closeStarted    <- Deferred[IO, Unit]
      retrySubscribed <- Deferred[IO, Unit]
      retryCompleted  <- Deferred[IO, Unit]
      closeMono = gate
        .asMono()
        .doOnSubscribe { _ =>
          closeAttempts.incrementAndGet()
          closeStarted.complete(()).void.unsafeRunAndForget()
        }
      provider <- testProvider(closeGate = Some(closeMono))
      _        <- initialize(provider)
      first    <- ReactorInterop.monoCompletionToIO(provider.closeGracefully()).start
      _        <- closeStarted.get.timeout(2.seconds)
      _        <- first.cancel
      retry    <- ReactorInterop
        .monoCompletionToIO(
          provider
            .closeGracefully()
            .doOnSubscribe(_ => retrySubscribed.complete(()).void.unsafeRunAndForget())
            .doOnSuccess(_ => retryCompleted.complete(()).void.unsafeRunAndForget())
        )
        .start
      _               <- retrySubscribed.get.timeout(2.seconds)
      completedBefore <- retryCompleted.tryGet
      _               <- IO(assertEquals(completedBefore, None))
      _               <- IO(gate.tryEmitEmpty()).void
      _               <- retry.joinWithNever.timeout(2.seconds)
      _               <- retryCompleted.get.timeout(2.seconds)
    } yield assertEquals(closeAttempts.get(), 1)
  }

  test("POST accepts a body of exactly 16 MiB") {
    for {
      provider  <- testProvider()
      sessionId <- initialize(provider)
      body = notificationWithSize(MaxRequestBytes)
      _    = assertEquals(
        body.getBytes(java.nio.charset.StandardCharsets.UTF_8).length,
        MaxRequestBytes
      )
      response <- provider.routes.orNotFound.run(post(body, Some(sessionId)))
    } yield assertEquals(response.status, Status.Accepted)
  }

  test("POST rejects an oversized declared Content-Length") {
    for {
      provider <- testProvider()
      request = post(notificationJson).putHeaders(
        Header.Raw(CIString("Content-Length"), (MaxRequestBytes + 1).toString)
      )
      response <- provider.routes.orNotFound.run(request)
    } yield assertEquals(response.status, Status.PayloadTooLarge)
  }

  test("POST rejects an oversized streamed body without Content-Length") {
    for {
      provider <- testProvider()
      request = Request[IO](method = Method.POST, uri = uri"/mcp")
        .withBodyStream(Stream.chunk(Chunk.array(Array.fill[Byte](MaxRequestBytes + 1)('x'))))
        .putHeaders(commonHeaders(None))
      response <- provider.routes.orNotFound.run(request)
    } yield assertEquals(response.status, Status.PayloadTooLarge)
  }

  test("shutdown prevents a concurrently created session from being registered") {
    val started          = new CountDownLatch(1)
    val release          = new CountDownLatch(1)
    val closed           = new AtomicBoolean(false)
    val startSessionHook = (_: String) => {
      started.countDown()
      if (!release.await(2, TimeUnit.SECONDS))
        throw new IllegalStateException("timed out waiting to release session creation")
    }

    for {
      provider <- testProvider(
        startSessionHook = Some(startSessionHook),
        closeObserver = Some(() => closed.set(true))
      )
      initialization <- provider.routes.orNotFound.run(post(initializeJson)).start
      _              <- IO.blocking(assert(started.await(2, TimeUnit.SECONDS)))
      shutdown       <- ReactorInterop.monoCompletionToIO(provider.closeGracefully()).start
      _              <- IO(release.countDown())
      _              <- shutdown.joinWithNever.timeout(2.seconds)
      response       <- initialization.joinWithNever.timeout(2.seconds)
      rejected       <- provider.routes.orNotFound.run(post(initializeJson))
    } yield {
      assertEquals(response.status, Status.ServiceUnavailable)
      assert(closed.get())
      assertEquals(rejected.status, Status.ServiceUnavailable)
    }
  }

  test("canceling during session creation closes and does not register the created session") {
    val started          = new CountDownLatch(1)
    val release          = new CountDownLatch(1)
    val closed           = new AtomicBoolean(false)
    val sessionId        = new AtomicReference[String]()
    val startSessionHook = (id: String) => {
      sessionId.set(id)
      started.countDown()
      if (!release.await(2, TimeUnit.SECONDS))
        throw new IllegalStateException("timed out waiting to release session creation")
    }

    for {
      provider <- testProvider(
        startSessionHook = Some(startSessionHook),
        closeObserver = Some(() => closed.set(true))
      )
      initialization  <- provider.routes.orNotFound.run(post(initializeJson)).start
      _               <- IO.blocking(assert(started.await(2, TimeUnit.SECONDS)))
      cancelRequested <- Deferred[IO, Unit]
      cancellation    <- (cancelRequested.complete(()).void *> initialization.cancel).start
      _               <- cancelRequested.get.timeout(2.seconds)
      _               <- IO.cede
      _               <- IO(release.countDown())
      _               <- cancellation.joinWithNever.timeout(2.seconds)
      _               <- initialization.join.timeout(2.seconds)
      response <- provider.routes.orNotFound.run(post(notificationJson, Some(sessionId.get())))
    } yield {
      assert(closed.get())
      assertEquals(response.status, Status.NotFound)
    }
  }

  test("initialization result failure closes the started session") {
    val closed = new AtomicBoolean(false)
    for {
      provider <- testProvider(
        initResultFailure = Some(new IllegalStateException("initialization failed")),
        closeObserver = Some(() => closed.set(true))
      )
      result <- provider.routes.orNotFound.run(post(initializeJson)).attempt
    } yield {
      assert(result.isLeft)
      assert(closed.get())
    }
  }

  test("initialization failure releases reserved capacity") {
    val starts = new AtomicInteger(0)
    for {
      provider <- testProvider(
        startSessionHook = Some(_ => starts.incrementAndGet()),
        initResultFailure = Some(new IllegalStateException("initialization failed")),
        config = Http4sStreamableServerTransportProviderConfig(maxSessions = 1)
      )
      first  <- provider.routes.orNotFound.run(post(initializeJson)).attempt
      second <- provider.routes.orNotFound.run(post(initializeJson)).attempt
    } yield {
      assert(first.isLeft)
      assert(second.isLeft)
      assertEquals(starts.get(), 2)
    }
  }

  test("transport orders send before close and rejects send after close") {
    val notification = new McpSchema.JSONRPCNotification("test", java.util.Map.of())
    for {
      transport <- Http4sStreamableServerTransport.create("session", mapper)
      read      <- transport.events.compile.toList.start
      _         <- ReactorInterop.monoCompletionToIO(transport.sendMessage(notification))
      _         <- ReactorInterop.monoCompletionToIO(transport.closeGracefully())
      events    <- read.joinWithNever.timeout(2.seconds)
      rejected  <- ReactorInterop.monoCompletionToIO(transport.sendMessage(notification)).attempt
    } yield {
      assert(rejected.swap.toOption.exists(_.getMessage == "MCP_TRANSPORT_CLOSED"))
      assertEquals(events.size, 1)
      assertEquals(events.head.eventType, Some("message"))
    }
  }

  test("accept failure is logged with a fixed diagnostic code and still returns Accepted") {
    val appender = new ListAppender[ILoggingEvent]()
    val logger   = LoggerFactory
      .getLogger(classOf[Http4sStreamableServerTransportProvider])
      .asInstanceOf[Logger]
    appender.start()
    logger.addAppender(appender)

    (for {
      provider  <- testProvider(acceptFailure = Some(new IllegalStateException("accept failed")))
      sessionId <- initialize(provider)
      response  <- provider.routes.orNotFound.run(post(notificationJson, Some(sessionId)))
      events    <- IO(appender.list.asScala.toList)
    } yield {
      assertEquals(response.status, Status.Accepted)
      assert(
        events.exists(event =>
          event.getLevel == Level.ERROR &&
            event.getFormattedMessage == "MCP_TRANSPORT_NOTIFICATION_ACCEPT_FAILED" &&
            !event.getFormattedMessage.contains(sessionId) &&
            event.getThrowableProxy == null
        )
      )
    }).guarantee(IO(logger.detachAppender(appender)))
  }

  test("POST response-stream failure closes the SSE body") {
    for {
      provider <- testProvider(
        responseStreamFailure = Some(new IllegalStateException("response stream failed"))
      )
      sessionId <- initialize(provider)
      response  <- provider.routes.orNotFound.run(post(requestJson, Some(sessionId)))
      body      <- response.bodyText.compile.string.timeout(2.seconds)
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(body, "")
    }
  }

  test("Reactor Mono subscription is disposed when IO is canceled") {
    for {
      canceled   <- Deferred[IO, Unit]
      subscribed <- Semaphore[IO](0)
      mono = Mono
        .never[Void]()
        .doOnSubscribe(_ => subscribed.release.unsafeRunAndForget())
        .doOnCancel(() => canceled.complete(()).void.unsafeRunAndForget())
      fiber <- ReactorInterop.monoCompletionToIO(mono).start
      _     <- subscribed.acquire.timeout(2.seconds)
      _     <- fiber.cancel
      _     <- canceled.get.timeout(2.seconds)
    } yield ()
  }

  test("value-producing Mono subscription is disposed when IO is canceled") {
    for {
      canceled   <- Deferred[IO, Unit]
      subscribed <- Deferred[IO, Unit]
      mono = Mono
        .never[String]()
        .doOnSubscribe(_ => subscribed.complete(()).void.unsafeRunAndForget())
        .doOnCancel(() => canceled.complete(()).void.unsafeRunAndForget())
      fiber <- ReactorInterop.monoToIO(mono).start
      _     <- subscribed.get.timeout(2.seconds)
      _     <- fiber.cancel
      _     <- canceled.get.timeout(2.seconds)
    } yield ()
  }

  test("IO fiber is canceled when Reactor subscriber disposes") {
    for {
      started  <- Deferred[IO, Unit]
      canceled <- Deferred[IO, Unit]
      mono = ReactorInterop.ioUnitToMono(
        started.complete(()).void >> IO.never.onCancel(canceled.complete(()).void)
      )
      disposable <- IO(mono.subscribe())
      _          <- started.get.timeout(2.seconds)
      _          <- IO(disposable.dispose())
      _          <- canceled.get.timeout(2.seconds)
    } yield ()
  }

  test("configuration defaults and positive limits are enforced") {
    val defaults = Http4sStreamableServerTransportProviderConfig()
    assertEquals(defaults.requestMaxBytes, 16L * 1024 * 1024)
    assertEquals(defaults.outboundBufferCapacity, 256)
    assertEquals(defaults.maxSessions, 1024)
    assertEquals(defaults.sessionIdleTimeout, Duration.ofMinutes(30))
    assert(defaults.securityValidator eq ServerTransportSecurityValidator.NOOP)

    intercept[IllegalArgumentException](
      Http4sStreamableServerTransportProviderConfig(requestMaxBytes = 0)
    )
    intercept[IllegalArgumentException](
      Http4sStreamableServerTransportProviderConfig(outboundBufferCapacity = 0)
    )
    intercept[IllegalArgumentException](
      Http4sStreamableServerTransportProviderConfig(maxSessions = 0)
    )
    intercept[IllegalArgumentException](
      Http4sStreamableServerTransportProviderConfig(sessionIdleTimeout = Duration.ZERO)
    )
    intercept[IllegalArgumentException](
      Http4sStreamableServerTransportProviderConfig(requestMaxBytes = -1)
    )
    intercept[IllegalArgumentException](
      Http4sStreamableServerTransportProviderConfig(outboundBufferCapacity = -1)
    )
    intercept[IllegalArgumentException](
      Http4sStreamableServerTransportProviderConfig(maxSessions = -1)
    )
    intercept[IllegalArgumentException](
      Http4sStreamableServerTransportProviderConfig(sessionIdleTimeout = Duration.ofNanos(-1))
    )
  }

  test("custom request limit accepts the exact limit and reads one detection byte") {
    val limit = 1024
    for {
      provider <- testProvider(config =
        Http4sStreamableServerTransportProviderConfig(requestMaxBytes = limit)
      )
      sessionId <- initialize(provider)
      exact = notificationWithSize(limit)
      accepted <- provider.routes.orNotFound.run(post(exact, Some(sessionId)))
      read     <- Ref.of[IO, Int](0)
      oversized = Request[IO](method = Method.POST, uri = uri"/mcp")
        .withBodyStream(
          Stream
            .range(0, limit + 20)
            .covary[IO]
            .evalTap(_ => read.update(_ + 1))
            .map(_.toByte)
        )
        .putHeaders(commonHeaders(Some(sessionId)))
      rejected  <- provider.routes.orNotFound.run(oversized)
      bytesRead <- read.get
    } yield {
      assertEquals(accepted.status, Status.Accepted)
      assertEquals(rejected.status, Status.PayloadTooLarge)
      assertEquals(bytesRead, limit + 1)
    }
  }

  test("Long.MaxValue request limit does not overflow its detection read") {
    for {
      provider <- testProvider(config =
        Http4sStreamableServerTransportProviderConfig(requestMaxBytes = Long.MaxValue)
      )
      response <- provider.routes.orNotFound.run(post(initializeJson))
    } yield assertEquals(response.status, Status.Ok)
  }

  test("security validation receives repeated headers and rejects before reading the body") {
    val captured  = new AtomicReference[java.util.Map[String, java.util.List[String]]]()
    val validator = new ServerTransportSecurityValidator {
      override def validateHeaders(headers: java.util.Map[String, java.util.List[String]]): Unit = {
        captured.set(headers)
        throw new ServerTransportSecurityException(401, "rejected")
      }
    }
    for {
      read     <- Ref.of[IO, Boolean](false)
      provider <- testProvider(
        config = Http4sStreamableServerTransportProviderConfig(securityValidator = validator)
      )
      request = Request[IO](method = Method.POST, uri = uri"/mcp")
        .withBodyStream(Stream.eval(read.set(true)).drain ++ Stream.emits(initializeJson.getBytes))
        .putHeaders(
          commonHeaders(None) ++ List(
            Header.Raw(CIString("X-Probe"), "one"),
            Header.Raw(CIString("x-probe"), "two")
          )
        )
      response <- provider.routes.orNotFound.run(request)
      wasRead  <- read.get
    } yield {
      assertEquals(response.status.code, 401)
      assert(!wasRead)
      assertEquals(captured.get().get("X-Probe").asScala.toList, List("one", "two"))
    }
  }

  test("security validation runs before GET and DELETE session access") {
    val validations = new AtomicInteger(0)
    val validator   = new ServerTransportSecurityValidator {
      override def validateHeaders(headers: java.util.Map[String, java.util.List[String]]): Unit = {
        validations.incrementAndGet()
        throw new ServerTransportSecurityException(403, "rejected")
      }
    }
    for {
      provider <- testProvider(
        config = Http4sStreamableServerTransportProviderConfig(securityValidator = validator)
      )
      getResponse    <- provider.routes.orNotFound.run(get(Some("missing")))
      deleteResponse <- provider.routes.orNotFound.run(delete(Some("missing")))
    } yield {
      assertEquals(getResponse.status, Status.Forbidden)
      assertEquals(deleteResponse.status, Status.Forbidden)
      assertEquals(validations.get(), 2)
    }
  }

  test("unexpected security validator failure returns HTTP 500") {
    val validator = new ServerTransportSecurityValidator {
      override def validateHeaders(headers: java.util.Map[String, java.util.List[String]]): Unit =
        throw new IllegalStateException("validator failed")
    }
    for {
      provider <- testProvider(
        config = Http4sStreamableServerTransportProviderConfig(securityValidator = validator)
      )
      response <- provider.routes.orNotFound.run(post(initializeJson))
    } yield assertEquals(response.status, Status.InternalServerError)
  }

  test("Accept matching uses media ranges and quality values") {
    for {
      provider <- testProvider()
      wildcard <- provider.routes.orNotFound.run(
        post(initializeJson).putHeaders(Header.Raw(CIString(HttpHeaders.ACCEPT), "*/*"))
      )
      zero <- provider.routes.orNotFound.run(
        post(initializeJson).putHeaders(
          Header.Raw(
            CIString(HttpHeaders.ACCEPT),
            "application/json, text/event-stream;q=0"
          )
        )
      )
      repeated <- provider.routes.orNotFound.run(
        post(initializeJson).withHeaders(
          org.http4s.Headers(
            Header.Raw(CIString(HttpHeaders.ACCEPT), "application/json"),
            Header.Raw(CIString(HttpHeaders.ACCEPT), "text/event-stream")
          )
        )
      )
      substring <- provider.routes.orNotFound.run(
        post(initializeJson).putHeaders(
          Header.Raw(CIString(HttpHeaders.ACCEPT), "application/jsonish, text/event-stream")
        )
      )
      sessionId <- initialize(provider)
      getZero   <- provider.routes.orNotFound.run(
        get(Some(sessionId)).putHeaders(
          Header.Raw(CIString(HttpHeaders.ACCEPT), "text/event-stream;q=0")
        )
      )
    } yield {
      assertEquals(wildcard.status, Status.Ok)
      assertEquals(zero.status, Status.BadRequest)
      assertEquals(repeated.status, Status.Ok)
      assertEquals(substring.status, Status.BadRequest)
      assertEquals(getZero.status, Status.BadRequest)
    }
  }

  test("session capacity rejects without factory invocation and is released by DELETE") {
    val starts = new AtomicInteger(0)
    for {
      provider <- testProvider(
        startSessionHook = Some(_ => starts.incrementAndGet()),
        config = Http4sStreamableServerTransportProviderConfig(maxSessions = 1)
      )
      firstId <- initialize(provider)
      full    <- provider.routes.orNotFound.run(post(initializeJson))
      countAtCapacity = starts.get()
      _        <- provider.routes.orNotFound.run(delete(Some(firstId)))
      admitted <- provider.routes.orNotFound.run(post(initializeJson))
    } yield {
      assertEquals(full.status, Status.ServiceUnavailable)
      assertEquals(countAtCapacity, 1)
      assertEquals(admitted.status, Status.Ok)
      assertEquals(starts.get(), 2)
    }
  }

  test("a pending initialization reserves session capacity") {
    val started = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val starts  = new AtomicInteger(0)
    for {
      provider <- testProvider(
        startSessionHook = Some { _ =>
          starts.incrementAndGet()
          started.countDown()
          release.await()
        },
        config = Http4sStreamableServerTransportProviderConfig(maxSessions = 1)
      )
      pending <- provider.routes.orNotFound.run(post(initializeJson)).start
      full    <- (for {
        _    <- IO.blocking(assert(started.await(2, TimeUnit.SECONDS)))
        full <- provider.routes.orNotFound.run(post(initializeJson))
        _    <- IO(release.countDown())
        _    <- pending.joinWithNever.timeout(2.seconds)
      } yield full).guarantee(
        IO(release.countDown()) *> pending.cancel *>
          ReactorInterop.monoCompletionToIO(provider.closeGracefully())
      )
    } yield {
      assertEquals(full.status, Status.ServiceUnavailable)
      assertEquals(starts.get(), 1)
    }
  }

  test("an idle session expires at the controlled deadline and releases capacity") {
    val config = Http4sStreamableServerTransportProviderConfig(
      maxSessions = 1,
      sessionIdleTimeout = Duration.ofNanos(10)
    )
    for {
      clock      <- Ref.of[IO, Long](0L)
      terminated <- Deferred[IO, Unit]
      provider   <- testProvider(config = config)
      _          <- IO(
        Http4sStreamableServerTransportProvider.installTestHooks(
          provider,
          afterTermination = terminated.complete(()).void,
          monotonicNanos = Some(clock.get)
        )
      )
      sessionId <- initialize(provider)
      _         <- clock.set(9L)
      before    <- provider.routes.orNotFound.run(post(notificationJson, Some(sessionId)))
      _         <- clock.set(19L)
      _         <- Http4sStreamableServerTransportProvider.runExpirySweep(provider)
      expired   <- provider.routes.orNotFound.run(post(notificationJson, Some(sessionId)))
      _         <- terminated.get.timeout(2.seconds)
      admitted  <- provider.routes.orNotFound.run(post(initializeJson))
    } yield {
      assertEquals(before.status, Status.Accepted)
      assertEquals(expired.status, Status.NotFound)
      assertEquals(admitted.status, Status.Ok)
    }
  }

  test("transport backpressures at capacity and close releases blocked sends") {
    val notification = new McpSchema.JSONRPCNotification("test", java.util.Map.of())
    for {
      transport <- Http4sStreamableServerTransport.create("session", mapper, 1)
      _         <- ReactorInterop.monoCompletionToIO(transport.sendMessage(notification))
      blocked   <- ReactorInterop.monoCompletionToIO(transport.sendMessage(notification)).start
      before    <- blocked.join.timeoutTo(20.millis, IO.pure(Outcome.canceled[IO, Throwable, Unit]))
      _         <- ReactorInterop.monoCompletionToIO(transport.closeGracefully())
      rejected  <- blocked.joinWithNever.attempt.timeout(2.seconds)
      events    <- transport.events.compile.toList
      afterClose <- ReactorInterop.monoCompletionToIO(transport.sendMessage(notification)).attempt
    } yield {
      assert(before.isCanceled)
      assert(rejected.swap.toOption.exists(_.getMessage == "MCP_TRANSPORT_CLOSED"))
      assert(afterClose.swap.toOption.exists(_.getMessage == "MCP_TRANSPORT_CLOSED"))
      assertEquals(events.size, 1)
    }
  }

  test("Reactor Mono interop distinguishes values and completion") {
    val failure = new IllegalStateException("probe")
    for {
      value      <- ReactorInterop.monoToIO(Mono.just("value"))
      emptyValue <- ReactorInterop.monoToIO(Mono.empty[String]()).attempt
      completion <- ReactorInterop.monoCompletionToIO(Mono.empty[Void]()).attempt
      monoError  <- ReactorInterop.monoToIO(Mono.error[String](failure)).attempt
    } yield {
      assertEquals(value, "value")
      assert(emptyValue.isLeft)
      assert(completion.isRight)
      assertEquals(monoError, Left(failure))
    }
  }

  test("Reactor Flux interop bounds demand and propagates errors") {
    val failure = new IllegalStateException("probe")
    val demand  = new AtomicLong(0)
    for {
      fluxValues <- ReactorInterop
        .fluxToStream(Flux.range(1, 10).doOnRequest(requested => demand.addAndGet(requested)))
        .take(1)
        .compile
        .toList
      fluxError <- ReactorInterop.fluxToStream(Flux.error[Int](failure)).compile.drain.attempt
    } yield {
      assertEquals(fluxValues, List(Integer.valueOf(1)))
      assert(demand.get() <= 2)
      assertEquals(fluxError, Left(failure))
    }
  }

  test("Reactor Flux interop preserves errors after values and empty completion") {
    val failure = new IllegalStateException("original-upstream-error")
    for {
      observed <- ReactorInterop
        .fluxToStream(Flux.just(1, 2).concatWith(Flux.error[Int](failure)))
        .attempt
        .compile
        .toList
      empty      <- ReactorInterop.fluxToStream(Flux.empty[Int]()).compile.toList
      downstream <- ReactorInterop
        .fluxToStream(Flux.just(1))
        .evalMap(_ => IO.raiseError[Unit](failure))
        .compile
        .drain
        .attempt
    } yield {
      assertEquals(observed.take(2), List(Right(1), Right(2)))
      assert(observed.last.swap.toOption.exists(_ eq failure))
      assertEquals(empty, Nil)
      assert(downstream.swap.toOption.exists(_ eq failure))
    }
  }

  test("Reactor Flux interop cancels upstream after early downstream completion") {
    val canceled = new AtomicBoolean(false)
    val demand   = new AtomicLong(0)
    ReactorInterop
      .fluxToStream(
        Flux
          .range(1, 10)
          .doOnRequest(requested => demand.addAndGet(requested))
          .doOnCancel(() => canceled.set(true))
      )
      .take(1)
      .compile
      .toList
      .map { values =>
        assertEquals(values, List(Integer.valueOf(1)))
        assert(canceled.get())
        assert(demand.get() <= 2)
      }
  }

  test("Reactor Flux interop propagates downstream cancellation") {
    val requested = new CountDownLatch(1)
    val canceled  = new AtomicBoolean(false)
    for {
      stream <- ReactorInterop
        .fluxToStream(
          Flux
            .never[Int]()
            .doOnRequest(_ => requested.countDown())
            .doOnCancel(() => canceled.set(true))
        )
        .compile
        .drain
        .start
      _ <- IO.blocking(assert(requested.await(2, TimeUnit.SECONDS)))
      _ <- stream.cancel
    } yield assert(canceled.get())
  }

  test("an unconsumed POST response body does not start SDK processing") {
    val appender = new ListAppender[ILoggingEvent]()
    val logger   = LoggerFactory
      .getLogger(classOf[Http4sStreamableServerTransportProvider])
      .asInstanceOf[Logger]
    appender.start()
    logger.addAppender(appender)
    (for {
      provider <- testProvider(
        responseStreamFailure = Some(new IllegalStateException("must-not-run"))
      )
      sessionId <- initialize(provider)
      response  <- provider.routes.orNotFound.run(post(requestJson, Some(sessionId)))
      _         <- provider.routes.orNotFound.run(delete(Some(sessionId)))
      events    <- IO(appender.list.asScala.toList)
    } yield {
      assertEquals(response.status, Status.Ok)
      assert(!events.exists(_.getFormattedMessage == "MCP_TRANSPORT_RESPONSE_STREAM_FAILED"))
    }).guarantee(IO(logger.detachAppender(appender)))
  }

  test("POST ownership unwinds when activity admission expires before worker startup") {
    val config = Http4sStreamableServerTransportProviderConfig(
      maxSessions = 1,
      sessionIdleTimeout = Duration.ofNanos(10)
    )
    for {
      clock           <- Ref.of[IO, Long](0L)
      owned           <- Deferred[IO, Unit]
      release         <- Deferred[IO, Unit]
      finalized       <- Deferred[IO, Unit]
      terminationDone <- Deferred[IO, Unit]
      sdkStarted      <- Deferred[IO, Unit]
      provider        <- testProvider(
        responseStreamOverride = Some(_ =>
          Mono.defer(() => {
            sdkStarted.complete(()).void.unsafeRunAndForget()
            Mono.empty[Void]()
          })
        ),
        config = config
      )
      _ <- IO(
        Http4sStreamableServerTransportProvider.installTestHooks(
          provider,
          afterResponseOwnership = owned.complete(()).void *> release.get,
          afterTermination = terminationDone.complete(()).void,
          monotonicNanos = Some(clock.get)
        )
      )
      sessionId <- initialize(provider)
      response  <- provider.routes.orNotFound.run(post(requestJson, Some(sessionId)))
      body      <- response.body.onFinalize(finalized.complete(()).void).compile.drain.start
      _         <- owned.get.timeout(2.seconds)
      _         <- clock.set(10L)
      _         <- release.complete(())
      _         <- body.joinWithNever.timeout(2.seconds)
      _         <- finalized.get.timeout(2.seconds)
      _         <- terminationDone.get.timeout(2.seconds)
      startup   <- sdkStarted.tryGet
      admitted  <- provider.routes.orNotFound.run(post(initializeJson))
      _         <- ReactorInterop.monoCompletionToIO(provider.closeGracefully()).timeout(2.seconds)
    } yield {
      assertEquals(startup, None)
      assertEquals(admitted.status, Status.Ok)
    }
  }

  test("GET replay ownership unwinds when activity admission expires before SDK startup") {
    val config = Http4sStreamableServerTransportProviderConfig(
      maxSessions = 1,
      sessionIdleTimeout = Duration.ofNanos(10)
    )
    for {
      clock           <- Ref.of[IO, Long](0L)
      owned           <- Deferred[IO, Unit]
      release         <- Deferred[IO, Unit]
      finalized       <- Deferred[IO, Unit]
      terminationDone <- Deferred[IO, Unit]
      replayStarted   <- Deferred[IO, Unit]
      listenerStarted <- Deferred[IO, Unit]
      provider        <- testProvider(
        replayObserver = Some(_ => replayStarted.complete(()).void),
        listeningStarted = Some(listenerStarted.complete(()).void),
        config = config
      )
      _ <- IO(
        Http4sStreamableServerTransportProvider.installTestHooks(
          provider,
          afterResponseOwnership = owned.complete(()).void *> release.get,
          afterTermination = terminationDone.complete(()).void,
          monotonicNanos = Some(clock.get)
        )
      )
      sessionId <- initialize(provider)
      response  <- provider.routes.orNotFound.run(
        get(Some(sessionId)).putHeaders(Header.Raw(CIString(HttpHeaders.LAST_EVENT_ID), "last"))
      )
      body          <- response.body.onFinalize(finalized.complete(()).void).compile.drain.start
      _             <- owned.get.timeout(2.seconds)
      _             <- clock.set(10L)
      _             <- release.complete(())
      _             <- body.joinWithNever.timeout(2.seconds)
      _             <- finalized.get.timeout(2.seconds)
      _             <- terminationDone.get.timeout(2.seconds)
      replayState   <- replayStarted.tryGet
      listenerState <- listenerStarted.tryGet
      admitted      <- provider.routes.orNotFound.run(post(initializeJson))
      _ <- ReactorInterop.monoCompletionToIO(provider.closeGracefully()).timeout(2.seconds)
    } yield {
      assertEquals(replayState, None)
      assertEquals(listenerState, None)
      assertEquals(admitted.status, Status.Ok)
    }
  }

  test("DELETE cancels and awaits an admitted notification acceptance operation") {
    val started   = new CountDownLatch(1)
    val canceled  = new AtomicBoolean(false)
    val finalized = new AtomicBoolean(false)
    val held      = Mono
      .never[Void]()
      .doOnSubscribe(_ => started.countDown())
      .doOnCancel(() => canceled.set(true))
      .doFinally(_ => finalized.set(true))

    for {
      provider <- testProvider(
        acceptOverride = Some(held),
        config = Http4sStreamableServerTransportProviderConfig(maxSessions = 1)
      )
      sessionId  <- initialize(provider)
      processing <- provider.routes.orNotFound.run(post(notificationJson, Some(sessionId))).start
      _          <- IO.blocking(assert(started.await(2, TimeUnit.SECONDS)))
      deleted    <- provider.routes.orNotFound.run(delete(Some(sessionId))).timeout(2.seconds)
      response   <- processing.joinWithNever.timeout(2.seconds)
      admitted   <- provider.routes.orNotFound.run(post(initializeJson))
    } yield {
      assertEquals(response.status, Status.Accepted)
      assertEquals(deleted.status, Status.Ok)
      assert(canceled.get())
      assert(finalized.get())
      assertEquals(admitted.status, Status.Ok)
    }
  }

  test("shutdown cancels and awaits an admitted JSON-RPC response acceptance operation") {
    val started   = new CountDownLatch(1)
    val canceled  = new AtomicBoolean(false)
    val finalized = new AtomicBoolean(false)
    val held      = Mono
      .never[Void]()
      .doOnSubscribe(_ => started.countDown())
      .doOnCancel(() => canceled.set(true))
      .doFinally(_ => finalized.set(true))
    val responseJson = """{"jsonrpc":"2.0","id":"held-response","result":{}}"""

    for {
      provider   <- testProvider(acceptOverride = Some(held))
      sessionId  <- initialize(provider)
      processing <- provider.routes.orNotFound.run(post(responseJson, Some(sessionId))).start
      _          <- IO.blocking(assert(started.await(2, TimeUnit.SECONDS)))
      _          <- ReactorInterop.monoCompletionToIO(provider.closeGracefully()).timeout(2.seconds)
      response   <- processing.joinWithNever.timeout(2.seconds)
    } yield {
      assertEquals(response.status, Status.Accepted)
      assert(canceled.get())
      assert(finalized.get())
    }
  }

  test("session termination closes multiple provider-owned GET responses") {
    for {
      listening  <- Semaphore[IO](0)
      provider   <- testProvider(listeningStarted = Some(listening.release))
      sessionId  <- initialize(provider)
      first      <- provider.routes.orNotFound.run(get(Some(sessionId)))
      second     <- provider.routes.orNotFound.run(get(Some(sessionId)))
      firstBody  <- first.body.compile.drain.start
      secondBody <- second.body.compile.drain.start
      _          <- listening.acquireN(2).timeout(2.seconds)
      deleted    <- provider.routes.orNotFound.run(delete(Some(sessionId)))
      _          <- firstBody.joinWithNever.timeout(2.seconds)
      _          <- secondBody.joinWithNever.timeout(2.seconds)
    } yield assertEquals(deleted.status, Status.Ok)
  }

  test("canceling at the reservation boundary releases capacity before factory startup") {
    val starts = new AtomicInteger(0)
    for {
      reserved <- Deferred[IO, Unit]
      release  <- Deferred[IO, Unit]
      provider <- testProvider(
        startSessionHook = Some(_ => starts.incrementAndGet()),
        config = Http4sStreamableServerTransportProviderConfig(maxSessions = 1)
      )
      _ <- IO(
        Http4sStreamableServerTransportProvider.installTestHooks(
          provider,
          afterReservation = reserved.complete(()).void *> release.get
        )
      )
      pending  <- provider.routes.orNotFound.run(post(initializeJson)).start
      _        <- reserved.get.timeout(2.seconds)
      full     <- provider.routes.orNotFound.run(post(initializeJson))
      _        <- pending.cancel
      _        <- release.complete(())
      admitted <- provider.routes.orNotFound.run(post(initializeJson))
      _        <- ReactorInterop.monoCompletionToIO(provider.closeGracefully()).timeout(2.seconds)
    } yield {
      assertEquals(full.status, Status.ServiceUnavailable)
      assertEquals(admitted.status, Status.Ok)
      assertEquals(starts.get(), 1)
    }
  }

  test("retirement after body ownership prevents replay and listener startup") {
    for {
      owned     <- Deferred[IO, Unit]
      release   <- Deferred[IO, Unit]
      replayed  <- Deferred[IO, Unit]
      listening <- Deferred[IO, Unit]
      provider  <- testProvider(
        replayObserver = Some(_ => replayed.complete(()).void),
        listeningStarted = Some(listening.complete(()).void)
      )
      sessionId <- initialize(provider)
      _         <- IO(
        Http4sStreamableServerTransportProvider.installTestHooks(
          provider,
          afterResponseOwnership = owned.complete(()).void *> release.get
        )
      )
      response <- provider.routes.orNotFound.run(
        get(Some(sessionId)).putHeaders(Header.Raw(CIString(HttpHeaders.LAST_EVENT_ID), "last"))
      )
      body          <- response.body.compile.drain.start
      _             <- owned.get.timeout(2.seconds)
      deleted       <- provider.routes.orNotFound.run(delete(Some(sessionId))).timeout(2.seconds)
      _             <- body.joinWithNever.timeout(2.seconds)
      replayState   <- replayed.tryGet
      listenerState <- listening.tryGet
      _             <- release.complete(())
    } yield {
      assertEquals(deleted.status, Status.Ok)
      assertEquals(replayState, None)
      assertEquals(listenerState, None)
    }
  }

  test("expiry and shutdown win after GET ownership without starting replay or listener") {
    def runCase(expire: Boolean): IO[Unit] = {
      val config = Http4sStreamableServerTransportProviderConfig(
        sessionIdleTimeout = Duration.ofNanos(10)
      )
      for {
        clock     <- Ref.of[IO, Long](0L)
        owned     <- Deferred[IO, Unit]
        replayed  <- Deferred[IO, Unit]
        listening <- Deferred[IO, Unit]
        provider  <- testProvider(
          replayObserver = Some(_ => replayed.complete(()).void),
          listeningStarted = Some(listening.complete(()).void),
          config = config
        )
        _ <- IO(
          Http4sStreamableServerTransportProvider.installTestHooks(
            provider,
            afterResponseOwnership = owned.complete(()).void *> IO.never,
            monotonicNanos = Some(clock.get)
          )
        )
        sessionId <- initialize(provider)
        response  <- provider.routes.orNotFound.run(
          get(Some(sessionId)).putHeaders(Header.Raw(CIString(HttpHeaders.LAST_EVENT_ID), "last"))
        )
        body <- response.body.compile.drain.start
        _    <- owned.get.timeout(2.seconds)
        _    <-
          if (expire)
            clock.set(10L) *> Http4sStreamableServerTransportProvider.runExpirySweep(provider)
          else ReactorInterop.monoCompletionToIO(provider.closeGracefully())
        _             <- body.joinWithNever.timeout(2.seconds)
        replayState   <- replayed.tryGet
        listenerState <- listening.tryGet
      } yield {
        assertEquals(replayState, None)
        assertEquals(listenerState, None)
      }
    }

    runCase(expire = true) *> runCase(expire = false)
  }

  test("unconsumed GET bodies stay lazy across DELETE, expiry, and shutdown") {
    def responseWithSignals(
        config: Http4sStreamableServerTransportProviderConfig,
        clock: Option[Ref[IO, Long]] = None,
        afterTermination: IO[Unit] = IO.unit
    ): IO[
      (
          Http4sStreamableServerTransportProvider,
          String,
          org.http4s.Response[IO],
          Deferred[IO, Unit],
          Deferred[IO, Unit]
      )
    ] =
      for {
        replayed  <- Deferred[IO, Unit]
        listening <- Deferred[IO, Unit]
        provider  <- testProvider(
          replayObserver = Some(_ => replayed.complete(()).void),
          listeningStarted = Some(listening.complete(()).void),
          config = config
        )
        _ <- IO(
          Http4sStreamableServerTransportProvider.installTestHooks(
            provider,
            afterTermination = afterTermination,
            monotonicNanos = clock.map(_.get)
          )
        )
        sessionId <- initialize(provider)
        response  <- provider.routes.orNotFound.run(
          get(Some(sessionId)).putHeaders(Header.Raw(CIString(HttpHeaders.LAST_EVENT_ID), "last"))
        )
      } yield (provider, sessionId, response, replayed, listening)

    val expiryConfig = Http4sStreamableServerTransportProviderConfig(
      maxSessions = 1,
      sessionIdleTimeout = Duration.ofNanos(10)
    )
    for {
      deleteCase       <- responseWithSignals(Http4sStreamableServerTransportProviderConfig())
      _                <- deleteCase._1.routes.orNotFound.run(delete(Some(deleteCase._2)))
      _                <- deleteCase._3.body.compile.drain.timeout(2.seconds)
      deleteReplay     <- deleteCase._4.tryGet
      deleteListen     <- deleteCase._5.tryGet
      shutdownCase     <- responseWithSignals(Http4sStreamableServerTransportProviderConfig())
      _                <- ReactorInterop.monoCompletionToIO(shutdownCase._1.closeGracefully())
      _                <- shutdownCase._3.body.compile.drain.timeout(2.seconds)
      shutdownReplay   <- shutdownCase._4.tryGet
      shutdownListen   <- shutdownCase._5.tryGet
      expiryClock      <- Ref.of[IO, Long](0L)
      expiryTerminated <- Deferred[IO, Unit]
      expiryCase       <- responseWithSignals(
        expiryConfig,
        Some(expiryClock),
        expiryTerminated.complete(()).void
      )
      _            <- expiryClock.set(10L)
      _            <- Http4sStreamableServerTransportProvider.runExpirySweep(expiryCase._1)
      _            <- expiryTerminated.get.timeout(2.seconds)
      admitted     <- expiryCase._1.routes.orNotFound.run(post(initializeJson))
      _            <- expiryCase._3.body.compile.drain.timeout(2.seconds)
      expiryReplay <- expiryCase._4.tryGet
      expiryListen <- expiryCase._5.tryGet
    } yield {
      assertEquals(deleteReplay, None)
      assertEquals(deleteListen, None)
      assertEquals(shutdownReplay, None)
      assertEquals(shutdownListen, None)
      assertEquals(admitted.status, Status.Ok)
      assertEquals(expiryReplay, None)
      assertEquals(expiryListen, None)
    }
  }

  test("active POST processing delays expiry and starts a fresh idle interval") {
    val gate   = Sinks.empty[Void]()
    val config = Http4sStreamableServerTransportProviderConfig(
      maxSessions = 1,
      sessionIdleTimeout = Duration.ofNanos(10)
    )
    for {
      clock      <- Ref.of[IO, Long](0L)
      started    <- Deferred[IO, Unit]
      terminated <- Deferred[IO, Unit]
      provider   <- testProvider(
        responseStreamOverride = Some(_ =>
          gate.asMono().doOnSubscribe(_ => started.complete(()).void.unsafeRunAndForget())
        ),
        config = config
      )
      _ <- IO(
        Http4sStreamableServerTransportProvider.installTestHooks(
          provider,
          afterTermination = terminated.complete(()).void,
          monotonicNanos = Some(clock.get)
        )
      )
      sessionId <- initialize(provider)
      response  <- provider.routes.orNotFound.run(post(requestJson, Some(sessionId)))
      body      <- response.body.compile.drain.start
      _         <- started.get.timeout(2.seconds)
      _         <- clock.set(10L)
      _         <- Http4sStreamableServerTransportProvider.runExpirySweep(provider)
      active    <- provider.routes.orNotFound.run(post(notificationJson, Some(sessionId)))
      full      <- provider.routes.orNotFound.run(post(initializeJson))
      _         <- IO(gate.tryEmitEmpty()).void
      _         <- body.joinWithNever.timeout(2.seconds)
      _         <- Http4sStreamableServerTransportProvider.runExpirySweep(provider)
      stillFull <- provider.routes.orNotFound.run(post(initializeJson))
      _         <- clock.set(20L)
      _         <- Http4sStreamableServerTransportProvider.runExpirySweep(provider)
      _         <- terminated.get.timeout(2.seconds)
      admitted  <- provider.routes.orNotFound.run(post(initializeJson))
    } yield {
      assertEquals(active.status, Status.Accepted)
      assertEquals(full.status, Status.ServiceUnavailable)
      assertEquals(stillFull.status, Status.ServiceUnavailable)
      assertEquals(admitted.status, Status.Ok)
    }
  }

  test("background expiry continues while another session cleanup is blocked") {
    val config = Http4sStreamableServerTransportProviderConfig(
      maxSessions = 2,
      sessionIdleTimeout = Duration.ofNanos(10)
    )
    for {
      clock          <- Ref.of[IO, Long](0L)
      closes         <- Ref.of[IO, Int](0)
      firstClosing   <- Deferred[IO, Unit]
      releaseFirst   <- Deferred[IO, Unit]
      secondClosing  <- Deferred[IO, Unit]
      secondReleased <- Deferred[IO, Unit]
      activeStarted  <- Deferred[IO, Unit]
      finishActive   <- Deferred[IO, Unit]
      provider       <- testProvider(
        closeGate = Some(
          ReactorInterop.ioUnitToMono(
            closes.getAndUpdate(_ + 1).flatMap {
              case 0 => firstClosing.complete(()).void *> releaseFirst.get
              case 1 => secondClosing.complete(()).void
              case _ => IO.unit
            }
          )
        ),
        responseStreamOverride = Some(_ =>
          ReactorInterop.ioUnitToMono(
            activeStarted.complete(()).void *> finishActive.get
          )
        ),
        config = config
      )
      _ <- (for {
        _ <- IO(
          Http4sStreamableServerTransportProvider.installTestHooks(
            provider,
            monotonicNanos = Some(clock.get),
            afterTermination = secondReleased.complete(()).void
          )
        )
        _        <- initialize(provider)
        secondId <- initialize(provider)
        response <- provider.routes.orNotFound.run(post(requestJson, Some(secondId)))
        body     <- response.body.compile.drain.start
        _        <- activeStarted.get.timeout(2.seconds)
        _        <- clock.set(10L)
        _        <- firstClosing.get.timeout(2.seconds)
        _        <- finishActive.complete(()).void
        _        <- body.joinWithNever.timeout(2.seconds)
        _        <- clock.set(20L)
        _        <- secondClosing.get.timeout(2.seconds)
        _        <- secondReleased.get.timeout(2.seconds)
        held     <- releaseFirst.tryGet
        admitted <- provider.routes.orNotFound.run(post(initializeJson))
        full     <- provider.routes.orNotFound.run(post(initializeJson))
      } yield {
        assertEquals(held, None)
        assertEquals(admitted.status, Status.Ok)
        assertEquals(full.status, Status.ServiceUnavailable)
      }).guarantee(
        finishActive.complete(()).void *> releaseFirst.complete(()).void *>
          ReactorInterop.monoCompletionToIO(provider.closeGracefully())
      )
    } yield ()
  }

  test("active replay delays expiry while passive live GET and server sends do not") {
    val replayGate = Sinks.empty[Void]()
    val config     = Http4sStreamableServerTransportProviderConfig(
      maxSessions = 1,
      sessionIdleTimeout = Duration.ofNanos(10)
    )
    for {
      clock         <- Ref.of[IO, Long](0L)
      replayStarted <- Deferred[IO, Unit]
      listening     <- Deferred[IO, Unit]
      provider      <- testProvider(
        replayObserver = Some(_ => replayStarted.complete(()).void),
        replayGate = Some(replayGate.asMono()),
        listeningStarted = Some(listening.complete(()).void),
        config = config
      )
      _ <- IO(
        Http4sStreamableServerTransportProvider.installTestHooks(
          provider,
          monotonicNanos = Some(clock.get)
        )
      )
      sessionId <- initialize(provider)
      response  <- provider.routes.orNotFound.run(
        get(Some(sessionId)).putHeaders(Header.Raw(CIString(HttpHeaders.LAST_EVENT_ID), "last"))
      )
      body   <- response.body.compile.drain.start
      _      <- replayStarted.get.timeout(2.seconds)
      _      <- clock.set(10L)
      _      <- Http4sStreamableServerTransportProvider.runExpirySweep(provider)
      active <- provider.routes.orNotFound.run(post(notificationJson, Some(sessionId)))
      _      <- IO(replayGate.tryEmitEmpty()).void
      _      <- listening.get.timeout(2.seconds)
      _      <- ReactorInterop.monoCompletionToIO(
        provider.notifyClient(sessionId, "server-traffic", java.util.Map.of[String, Object]())
      )
      _       <- clock.set(20L)
      _       <- Http4sStreamableServerTransportProvider.runExpirySweep(provider)
      _       <- body.joinWithNever.timeout(2.seconds)
      expired <- provider.routes.orNotFound.run(post(notificationJson, Some(sessionId)))
    } yield {
      assertEquals(active.status, Status.Accepted)
      assertEquals(expired.status, Status.NotFound)
    }
  }

  test("malformed and security-rejected requests do not refresh idle time") {
    val validator = new ServerTransportSecurityValidator {
      override def validateHeaders(headers: java.util.Map[String, java.util.List[String]]): Unit =
        headers.asScala.collectFirst {
          case (name, values) if name.equalsIgnoreCase("X-Reject") && values.contains("yes") =>
            throw new ServerTransportSecurityException(403, "rejected")
        }
    }
    val config = Http4sStreamableServerTransportProviderConfig(
      sessionIdleTimeout = Duration.ofNanos(10),
      securityValidator = validator
    )
    for {
      clock    <- Ref.of[IO, Long](0L)
      provider <- testProvider(config = config)
      _        <- IO(
        Http4sStreamableServerTransportProvider.installTestHooks(
          provider,
          monotonicNanos = Some(clock.get)
        )
      )
      sessionId <- initialize(provider)
      _         <- clock.set(9L)
      malformed <- provider.routes.orNotFound.run(post("not-json", Some(sessionId)))
      rejected  <- provider.routes.orNotFound.run(
        post(notificationJson, Some(sessionId)).putHeaders(Header.Raw(CIString("X-Reject"), "yes"))
      )
      _       <- clock.set(10L)
      expired <- provider.routes.orNotFound.run(post(notificationJson, Some(sessionId)))
    } yield {
      assertEquals(malformed.status, Status.BadRequest)
      assertEquals(rejected.status, Status.Forbidden)
      assertEquals(expired.status, Status.NotFound)
    }
  }

  test("DELETE cancels and awaits active POST processing") {
    for {
      started  <- Deferred[IO, Unit]
      canceled <- Deferred[IO, Unit]
      provider <- testProvider(
        responseStreamOverride = Some(_ =>
          Mono
            .never[Void]()
            .doOnSubscribe(_ => started.complete(()).void.unsafeRunAndForget())
            .doOnCancel(() => canceled.complete(()).void.unsafeRunAndForget())
        )
      )
      sessionId <- initialize(provider)
      response  <- provider.routes.orNotFound.run(post(requestJson, Some(sessionId)))
      body      <- response.body.compile.drain.start
      _         <- started.get.timeout(2.seconds)
      deleted   <- provider.routes.orNotFound.run(delete(Some(sessionId))).timeout(2.seconds)
      _         <- canceled.get.timeout(2.seconds)
      _         <- body.joinWithNever.timeout(2.seconds)
    } yield assertEquals(deleted.status, Status.Ok)
  }

  test("DELETE cancels and awaits active replay") {
    for {
      subscribed <- Deferred[IO, Unit]
      canceled   <- Deferred[IO, Unit]
      replayGate = Mono
        .never[Void]()
        .doOnSubscribe(_ => subscribed.complete(()).void.unsafeRunAndForget())
        .doOnCancel(() => canceled.complete(()).void.unsafeRunAndForget())
      provider  <- testProvider(replayGate = Some(replayGate))
      sessionId <- initialize(provider)
      response  <- provider.routes.orNotFound.run(
        get(Some(sessionId)).putHeaders(Header.Raw(CIString(HttpHeaders.LAST_EVENT_ID), "last"))
      )
      body    <- response.body.compile.drain.start
      _       <- subscribed.get.timeout(2.seconds)
      deleted <- provider.routes.orNotFound.run(delete(Some(sessionId))).timeout(2.seconds)
      _       <- canceled.get.timeout(2.seconds)
      _       <- body.joinWithNever.timeout(2.seconds)
    } yield assertEquals(deleted.status, Status.Ok)
  }

  test("an emitted server keepalive does not prevent controlled idle expiry") {
    val config = Http4sStreamableServerTransportProviderConfig(
      keepAliveInterval = Some(Duration.ofMillis(1)),
      sessionIdleTimeout = Duration.ofNanos(10)
    )
    for {
      clock     <- Ref.of[IO, Long](0L)
      listening <- Deferred[IO, Unit]
      pingSeen  <- Deferred[IO, Unit]
      bodyRef   <- Ref.of[IO, String]("")
      provider  <- testProvider(
        listeningStarted = Some(listening.complete(()).void),
        config = config
      )
      _ <- IO(
        Http4sStreamableServerTransportProvider.installTestHooks(
          provider,
          monotonicNanos = Some(clock.get)
        )
      )
      sessionId <- initialize(provider)
      response  <- provider.routes.orNotFound.run(get(Some(sessionId)))
      body      <- readBodyAndSignal(response, bodyRef, List("ping" -> pingSeen))
      _         <- listening.get.timeout(2.seconds)
      _         <- pingSeen.get.timeout(2.seconds)
      _         <- clock.set(10L)
      _         <- Http4sStreamableServerTransportProvider.runExpirySweep(provider)
      _         <- body.joinWithNever.timeout(2.seconds)
      expired   <- provider.routes.orNotFound.run(post(notificationJson, Some(sessionId)))
    } yield assertEquals(expired.status, Status.NotFound)
  }

  test("cleanup failure releases capacity and does not skip owned GET cleanup") {
    val failure = new IllegalStateException("cleanup-secret")
    for {
      listening <- Deferred[IO, Unit]
      provider  <- testProvider(
        listeningStarted = Some(listening.complete(()).void),
        closeGate = Some(Mono.error[Void](failure)),
        config = Http4sStreamableServerTransportProviderConfig(maxSessions = 1)
      )
      sessionId <- initialize(provider)
      response  <- provider.routes.orNotFound.run(get(Some(sessionId)))
      body      <- response.body.compile.drain.start
      _         <- listening.get.timeout(2.seconds)
      deleted   <- provider.routes.orNotFound.run(delete(Some(sessionId)))
      _         <- body.joinWithNever.timeout(2.seconds)
      admitted  <- provider.routes.orNotFound.run(post(initializeJson))
    } yield {
      assertEquals(deleted.status, Status.Ok)
      assertEquals(admitted.status, Status.Ok)
    }
  }

  test("DELETE, expiry, and shutdown share cleanup after a waiter is canceled") {
    val gate     = Sinks.empty[Void]()
    val attempts = new AtomicInteger(0)
    val config   = Http4sStreamableServerTransportProviderConfig(
      sessionIdleTimeout = Duration.ofNanos(10)
    )
    for {
      clock        <- Ref.of[IO, Long](0L)
      closeStarted <- Deferred[IO, Unit]
      provider     <- testProvider(
        closeGate = Some(gate.asMono().doOnSubscribe { _ =>
          attempts.incrementAndGet()
          closeStarted.complete(()).void.unsafeRunAndForget()
        }),
        config = config
      )
      _ <- IO(
        Http4sStreamableServerTransportProvider.installTestHooks(
          provider,
          monotonicNanos = Some(clock.get)
        )
      )
      sessionId <- initialize(provider)
      _         <- clock.set(10L)
      deleted   <- provider.routes.orNotFound.run(delete(Some(sessionId))).start
      sweep     <- Http4sStreamableServerTransportProvider.runExpirySweep(provider).start
      _         <- closeStarted.get.timeout(2.seconds)
      shutdown  <- ReactorInterop.monoCompletionToIO(provider.closeGracefully()).start
      _         <- deleted.cancel
      waiting <- shutdown.join.timeoutTo(20.millis, IO.pure(Outcome.canceled[IO, Throwable, Unit]))
      _       <- IO(gate.tryEmitEmpty()).void
      _       <- sweep.joinWithNever.timeout(2.seconds)
      _       <- shutdown.joinWithNever.timeout(2.seconds)
    } yield {
      assert(waiting.isCanceled)
      assertEquals(attempts.get(), 1)
    }
  }

  test("transport-owned logs contain only fixed diagnostics") {
    val secret  = "secret-marker"
    val cause   = new IllegalStateException(secret + "-cause")
    val failure = new IllegalStateException(secret + "-message", cause)
    failure.addSuppressed(new IllegalArgumentException(secret + "-suppressed"))
    val appender = new ListAppender[ILoggingEvent]()
    val logger   = LoggerFactory
      .getLogger(classOf[Http4sStreamableServerTransportProvider])
      .asInstanceOf[Logger]
    val validator = new ServerTransportSecurityValidator {
      override def validateHeaders(headers: java.util.Map[String, java.util.List[String]]): Unit =
        throw failure
    }
    appender.start()
    logger.addAppender(appender)

    (for {
      securityProvider <- testProvider(
        config = Http4sStreamableServerTransportProviderConfig(securityValidator = validator)
      )
      _ <- securityProvider.routes.orNotFound.run(
        post(secret).putHeaders(Header.Raw(CIString("X-Secret"), secret))
      )
      initializeProvider <- testProvider(initResultFailure = Some(failure))
      _                  <- initializeProvider.routes.orNotFound.run(post(initializeJson)).attempt
      responseProvider   <- testProvider(responseStreamFailure = Some(failure))
      responseSession    <- initialize(responseProvider)
      response <- responseProvider.routes.orNotFound.run(post(requestJson, Some(responseSession)))
      _        <- response.body.compile.drain
      acceptProvider <- testProvider(
        acceptFailure = Some(failure),
        closeGate = Some(Mono.error[Void](failure)),
        sessionIdOverride = Some(secret + "-session")
      )
      sessionId <- initialize(acceptProvider)
      _         <- acceptProvider.routes.orNotFound.run(
        post(notificationJson.replace("{}", s"{\"value\":\"$secret\"}"), Some(sessionId))
      )
      _ <- acceptProvider.routes.orNotFound.run(
        post(s"{\"jsonrpc\":\"2.0\",\"id\":\"$secret\",\"result\":{}}", Some(sessionId))
      )
      _ <- ReactorInterop.monoCompletionToIO(
        acceptProvider.notifyClients(secret, secret)
      )
      _      <- acceptProvider.routes.orNotFound.run(delete(Some(sessionId)))
      events <- IO(appender.list.asScala.toList)
    } yield {
      val messages = events.map(_.getFormattedMessage).toSet
      assert(
        Set(
          "MCP_TRANSPORT_SECURITY_VALIDATION_FAILED",
          "MCP_TRANSPORT_INITIALIZE_FAILED",
          "MCP_TRANSPORT_RESPONSE_STREAM_FAILED",
          "MCP_TRANSPORT_NOTIFICATION_ACCEPT_FAILED",
          "MCP_TRANSPORT_RESPONSE_ACCEPT_FAILED",
          "MCP_TRANSPORT_NOTIFY_FAILED",
          "MCP_TRANSPORT_CLEANUP_FAILED"
        ).subsetOf(messages)
      )
      events.foreach { event =>
        assert(!event.getFormattedMessage.contains(secret))
        assert(!event.getMessage.contains(secret))
        assert(
          Option(event.getArgumentArray).toList.flatten.forall(arg =>
            !String.valueOf(arg).contains(secret)
          )
        )
        assertEquals(event.getThrowableProxy, null)
      }
    }).guarantee(IO(logger.detachAppender(appender)))
  }

  test("live-listener readiness is signaled only after registration and loses no later send") {
    for {
      listening <- Deferred[IO, Unit]
      received  <- Deferred[IO, Unit]
      bodyRef   <- Ref.of[IO, String]("")
      provider  <- testProvider(listeningStarted = Some(listening.complete(()).void))
      sessionId <- initialize(provider)
      response  <- provider.routes.orNotFound.run(get(Some(sessionId)))
      before    <- listening.tryGet
      body      <- readBodyAndSignal(response, bodyRef, List("ready-message" -> received))
      _         <- listening.get.timeout(2.seconds)
      _         <- ReactorInterop.monoCompletionToIO(
        provider.notifyClient(sessionId, "ready-message", java.util.Map.of[String, Object]())
      )
      _ <- received.get.timeout(2.seconds)
      _ <- provider.routes.orNotFound.run(delete(Some(sessionId)))
      _ <- body.joinWithNever.timeout(2.seconds)
    } yield assertEquals(before, None)
  }

  private def readBodyAndSignal(
      response: org.http4s.Response[IO],
      bodyRef: Ref[IO, String],
      signals: List[(String, Deferred[IO, Unit])]
  ): IO[cats.effect.FiberIO[Unit]] =
    response.bodyText
      .evalMap { chunk =>
        bodyRef.updateAndGet(_ + chunk).flatMap { body =>
          signals.traverse_ { case (text, signal) =>
            if (body.contains(text)) signal.complete(()).void else IO.unit
          }
        }
      }
      .compile
      .drain
      .start

  private def notificationWithSize(size: Int): String = {
    val prefix = """{"jsonrpc":"2.0","method":"notifications/initialized","params":{"padding":""""
    val suffix = """"}}"""
    prefix + ("x" * (size - prefix.length - suffix.length)) + suffix
  }

  private def testProvider(
      notificationHandler: Option[McpTransportContext => IO[Unit]] = None,
      replayObserver: Option[AnyRef => IO[Unit]] = None,
      replayGate: Option[Mono[Void]] = None,
      listeningStarted: Option[IO[Unit]] = None,
      startSessionHook: Option[String => Unit] = None,
      closeObserver: Option[() => Unit] = None,
      closeGate: Option[Mono[Void]] = None,
      initResultFailure: Option[Throwable] = None,
      acceptFailure: Option[Throwable] = None,
      acceptOverride: Option[Mono[Void]] = None,
      responseStreamFailure: Option[Throwable] = None,
      responseStreamOverride: Option[McpStreamableServerTransport => Mono[Void]] = None,
      sessionIdOverride: Option[String] = None,
      config: Http4sStreamableServerTransportProviderConfig =
        Http4sStreamableServerTransportProviderConfig()
  ): IO[Http4sStreamableServerTransportProvider] =
    IO {
      val provider = Http4sStreamableServerTransportProvider(jsonMapper = mapper, config = config)
      provider.setSessionFactory(
        new TestSessionFactory(
          notificationHandler,
          replayObserver,
          replayGate,
          listeningStarted,
          startSessionHook,
          closeObserver,
          closeGate,
          initResultFailure,
          acceptFailure,
          acceptOverride,
          responseStreamFailure,
          responseStreamOverride,
          sessionIdOverride
        )
      )
      provider
    }

  private def initialize(provider: Http4sStreamableServerTransportProvider): IO[String] =
    provider.routes.orNotFound.run(post(initializeJson)).map { response =>
      response.headers.get(CIString(HttpHeaders.MCP_SESSION_ID)).map(_.head.value).get
    }

  private def post(body: String, sessionId: Option[String] = None): Request[IO] =
    Request[IO](method = Method.POST, uri = uri"/mcp")
      .withEntity(body)
      .putHeaders(commonHeaders(sessionId))

  private def get(sessionId: Option[String]): Request[IO] =
    Request[IO](method = Method.GET, uri = uri"/mcp")
      .putHeaders(Header.Raw(CIString(HttpHeaders.ACCEPT), "text/event-stream"))
      .putHeaders(sessionId.map(id => Header.Raw(CIString(HttpHeaders.MCP_SESSION_ID), id)).toList)

  private def delete(sessionId: Option[String]): Request[IO] =
    Request[IO](method = Method.DELETE, uri = uri"/mcp")
      .putHeaders(sessionId.map(id => Header.Raw(CIString(HttpHeaders.MCP_SESSION_ID), id)).toList)

  private def commonHeaders(sessionId: Option[String]): List[Header.Raw] =
    List(Header.Raw(CIString(HttpHeaders.ACCEPT), "application/json, text/event-stream")) ++
      sessionId.map(id => Header.Raw(CIString(HttpHeaders.MCP_SESSION_ID), id)).toList

  private val initializeJson: String =
    s"""{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"${ProtocolVersions.MCP_2025_06_18}","capabilities":{},"clientInfo":{"name":"test-client","version":"1.0"}}}"""

  private val notificationJson: String =
    """{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}"""

  private val requestJson: String =
    """{"jsonrpc":"2.0","id":"req-1","method":"test/request","params":{"value":1}}"""

  private final class TestSessionFactory(
      notificationHandler: Option[McpTransportContext => IO[Unit]],
      replayObserver: Option[AnyRef => IO[Unit]],
      replayGate: Option[Mono[Void]],
      listeningStarted: Option[IO[Unit]],
      startSessionHook: Option[String => Unit],
      closeObserver: Option[() => Unit],
      closeGate: Option[Mono[Void]],
      initResultFailure: Option[Throwable],
      acceptFailure: Option[Throwable],
      acceptOverride: Option[Mono[Void]],
      responseStreamFailure: Option[Throwable],
      responseStreamOverride: Option[McpStreamableServerTransport => Mono[Void]],
      sessionIdOverride: Option[String]
  ) extends McpStreamableServerSession.Factory {
    override def startSession(
        initializeRequest: McpSchema.InitializeRequest
    ): McpStreamableServerSession.McpStreamableServerSessionInit = {
      val id              = sessionIdOverride.getOrElse(UUID.randomUUID().toString)
      val requestHandlers = Map[String, McpRequestHandler[_]](
        "test/request" -> new McpRequestHandler[Object] {
          override def handle(exchange: McpAsyncServerExchange, params: Object): Mono[Object] =
            Mono.just(java.util.Map.of("status", "request-ok"))
        }
      ).asJava
      val notificationHandlers = Map[String, McpNotificationHandler](
        "notifications/initialized" -> new McpNotificationHandler {
          override def handle(exchange: McpAsyncServerExchange, params: Object): Mono[Void] =
            ReactorInterop.ioUnitToMono(
              notificationHandler.fold(IO.unit)(_(exchange.transportContext()))
            )
        }
      ).asJava
      val session = new TestSession(
        id,
        initializeRequest.capabilities(),
        initializeRequest.clientInfo(),
        requestHandlers,
        notificationHandlers,
        replayObserver,
        replayGate,
        listeningStarted,
        closeObserver,
        closeGate,
        acceptFailure,
        acceptOverride,
        responseStreamFailure,
        responseStreamOverride
      )
      startSessionHook.foreach(_(id))
      val result = McpSchema.InitializeResult
        .builder(
          ProtocolVersions.MCP_2025_06_18,
          McpSchema.ServerCapabilities.builder().build(),
          McpSchema.Implementation.builder("test-server", "1.0").build()
        )
        .build()
      val initResult = initResultFailure.fold(Mono.just(result))(error =>
        Mono.error[McpSchema.InitializeResult](error)
      )
      new McpStreamableServerSession.McpStreamableServerSessionInit(session, initResult)
    }
  }

  private final class TestSession(
      id: String,
      capabilities: McpSchema.ClientCapabilities,
      clientInfo: McpSchema.Implementation,
      requestHandlers: java.util.Map[String, McpRequestHandler[_]],
      notificationHandlers: java.util.Map[String, McpNotificationHandler],
      replayObserver: Option[AnyRef => IO[Unit]],
      replayGate: Option[Mono[Void]],
      listeningStarted: Option[IO[Unit]],
      closeObserver: Option[() => Unit],
      closeGate: Option[Mono[Void]],
      acceptFailure: Option[Throwable],
      acceptOverride: Option[Mono[Void]],
      responseStreamFailure: Option[Throwable],
      responseStreamOverride: Option[McpStreamableServerTransport => Mono[Void]]
  ) extends McpStreamableServerSession(
        id,
        capabilities,
        clientInfo,
        Duration.ofSeconds(2),
        requestHandlers,
        notificationHandlers,
        () => Mono.empty[Void]()
      ) {
    override def listeningStream(transport: McpStreamableServerTransport) = {
      val stream = super.listeningStream(transport)
      listeningStarted.foreach(_.unsafeRunAndForget())
      stream
    }

    override def accept(notification: McpSchema.JSONRPCNotification): Mono[Void] =
      acceptOverride.getOrElse(
        acceptFailure.fold(super.accept(notification))(error => Mono.error[Void](error))
      )

    override def accept(response: McpSchema.JSONRPCResponse): Mono[Void] =
      acceptOverride.getOrElse(
        acceptFailure.fold(super.accept(response))(error => Mono.error[Void](error))
      )

    override def responseStream(
        request: McpSchema.JSONRPCRequest,
        transport: McpStreamableServerTransport
    ): Mono[Void] =
      responseStreamOverride.fold(
        responseStreamFailure.fold(super.responseStream(request, transport))(error =>
          Mono.error[Void](error)
        )
      )(_(transport))

    override def closeGracefully(): Mono[Void] =
      closeGate.getOrElse(super.closeGracefully())

    override def close(): Unit = {
      closeObserver.foreach(_())
      super.close()
    }

    override def replay(lastEventId: Object): Flux[McpSchema.JSONRPCMessage] = {
      replayObserver.foreach(observer => observer(lastEventId).unsafeRunAndForget())
      val messages: Flux[McpSchema.JSONRPCMessage] = Flux.just(
        new McpSchema.JSONRPCNotification(
          "replayed",
          java.util.Map.of("id", lastEventId)
        ): McpSchema.JSONRPCMessage
      )
      replayGate.fold(messages)(_.thenMany(messages))
    }
  }
}
