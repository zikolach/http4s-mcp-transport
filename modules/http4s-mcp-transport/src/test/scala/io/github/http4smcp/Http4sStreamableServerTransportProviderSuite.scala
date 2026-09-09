package io.github.http4smcp

import cats.effect.Deferred
import cats.effect.IO
import cats.effect.Ref
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
      _         <- ReactorInterop.monoToIO(
        provider.notifyClient(sessionId, "server/notice", java.util.Map.of("ok", "true"))
      )
      _    <- ReactorInterop.monoToIO(provider.closeGracefully())
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
      secondRead      <- secondResponse.bodyText.interruptAfter(1.second).compile.string.start
      _               <- firstListening.get.timeout(1.second)
      _               <- secondListening.get.timeout(1.second)
      _               <- ReactorInterop.monoToIO(first.closeGracefully())
      _               <- ReactorInterop.monoToIO(
        second.notifyClients("broadcast", java.util.Map.of("ok", "true"))
      )
      _          <- ReactorInterop.monoToIO(second.closeGracefully())
      firstBody  <- firstRead.joinWithNever
      secondBody <- secondRead.joinWithNever
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
      _ <- ReactorInterop.monoToIO(
        provider.notifyClient(sessionId, "after-replay", java.util.Map.of("ok", "true"))
      )
      _    <- afterBody.get.timeout(2.seconds)
      _    <- ReactorInterop.monoToIO(provider.closeGracefully())
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
        .monoToIO(provider.notifyClient(sessionId, "during-replay", java.util.Map.of("ok", "true")))
        .attempt
      _ <- IO(replayRelease.tryEmitEmpty()).void
      _ <- replayedBody.get.timeout(2.seconds)
      _ <- listening.get.timeout(2.seconds)
      _ <- ReactorInterop.monoToIO(
        provider.notifyClient(sessionId, "after-replay", java.util.Map.of("ok", "true"))
      )
      _    <- afterBody.get.timeout(2.seconds)
      _    <- ReactorInterop.monoToIO(provider.closeGracefully())
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
      _              <- ReactorInterop.monoToIO(
        provider.notifyClients("broadcast", java.util.Map.of("ok", "true"))
      )
      _          <- ReactorInterop.monoToIO(provider.closeGracefully())
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
      _        <- ReactorInterop.monoToIO(provider.closeGracefully())
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
      first    <- ReactorInterop.monoToIO(provider.closeGracefully()).start
      _        <- closeStarted.get.timeout(2.seconds)
      second   <- ReactorInterop
        .monoToIO(
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
      first    <- ReactorInterop.monoToIO(provider.closeGracefully()).start
      _        <- closeStarted.get.timeout(2.seconds)
      _        <- first.cancel
      retry    <- ReactorInterop
        .monoToIO(
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
      _              <- ReactorInterop.monoToIO(provider.closeGracefully())
      _              <- IO(release.countDown())
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

  test("transport orders send before close and ignores send after close") {
    val notification = new McpSchema.JSONRPCNotification("test", java.util.Map.of())
    for {
      transport <- Http4sStreamableServerTransport.create("session", mapper)
      read      <- transport.events.compile.toList.start
      _         <- ReactorInterop.monoToIO(transport.sendMessage(notification))
      _         <- ReactorInterop.monoToIO(transport.closeGracefully())
      events    <- read.joinWithNever.timeout(2.seconds)
      _         <- ReactorInterop.monoToIO(transport.sendMessage(notification))
    } yield {
      assertEquals(events.size, 1)
      assertEquals(events.head.eventType, Some("message"))
    }
  }

  test("accept failure is logged with session context and still returns Accepted") {
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
            event.getFormattedMessage.contains(sessionId) &&
            event.getFormattedMessage.contains("notification")
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
      fiber <- ReactorInterop.monoToIO(mono).start
      _     <- subscribed.acquire.timeout(2.seconds)
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
      responseStreamFailure: Option[Throwable] = None,
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
          responseStreamFailure
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
      responseStreamFailure: Option[Throwable]
  ) extends McpStreamableServerSession.Factory {
    override def startSession(
        initializeRequest: McpSchema.InitializeRequest
    ): McpStreamableServerSession.McpStreamableServerSessionInit = {
      val id              = UUID.randomUUID().toString
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
        responseStreamFailure
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
      responseStreamFailure: Option[Throwable]
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
      acceptFailure.fold(super.accept(notification))(error => Mono.error[Void](error))

    override def responseStream(
        request: McpSchema.JSONRPCRequest,
        transport: McpStreamableServerTransport
    ): Mono[Void] =
      responseStreamFailure.fold(super.responseStream(request, transport))(error =>
        Mono.error[Void](error)
      )

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
