package io.github.http4smcp

import cats.effect.IO
import cats.effect.Ref
import cats.effect.std.Semaphore
import cats.syntax.all._
import io.github.http4smcp.internal.ReactorInterop
import io.modelcontextprotocol.common.McpTransportContext
import io.modelcontextprotocol.json.McpJsonDefaults
import io.modelcontextprotocol.server.McpAsyncServerExchange
import io.modelcontextprotocol.server.McpNotificationHandler
import io.modelcontextprotocol.server.McpRequestHandler
import io.modelcontextprotocol.spec.HttpHeaders
import io.modelcontextprotocol.spec.McpSchema
import io.modelcontextprotocol.spec.McpStreamableServerSession
import io.modelcontextprotocol.spec.ProtocolVersions
import munit.CatsEffectSuite
import org.http4s.Header
import org.http4s.Method
import org.http4s.Request
import org.http4s.Status
import org.http4s.Uri
import org.http4s.syntax.all._
import org.typelevel.ci.CIString
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

final class Http4sStreamableServerTransportProviderSuite extends CatsEffectSuite {
  private val mapper = McpJsonDefaults.getMapper()

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
      provider  <- testProvider()
      sessionId <- initialize(provider)
      response  <- provider.routes.orNotFound.run(get(Some(sessionId)))
      read      <- response.bodyText.interruptAfter(1.second).compile.string.start
      _         <- IO.sleep(100.millis)
      _         <- ReactorInterop.monoToIO(
        provider.notifyClient(sessionId, "server/notice", java.util.Map.of("ok", "true"))
      )
      _    <- IO.sleep(100.millis)
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
      first          <- testProvider()
      second         <- testProvider()
      firstSession   <- initialize(first)
      secondSession  <- initialize(second)
      firstResponse  <- first.routes.orNotFound.run(get(Some(firstSession)))
      secondResponse <- second.routes.orNotFound.run(get(Some(secondSession)))
      firstRead      <- firstResponse.bodyText.interruptAfter(1.second).compile.string.start
      secondRead     <- secondResponse.bodyText.interruptAfter(1.second).compile.string.start
      _              <- IO.sleep(100.millis)
      _              <- ReactorInterop.monoToIO(first.closeGracefully())
      _              <- ReactorInterop.monoToIO(
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
      replayed <- Ref.of[IO, Option[String]](None)
      gate     <- Semaphore[IO](0)
      provider <- testProvider(replayObserver =
        Some(id => replayed.set(Some(id.toString)) >> gate.release)
      )
      sessionId <- initialize(provider)
      response  <- provider.routes.orNotFound.run(
        get(Some(sessionId)).putHeaders(Header.Raw(CIString(HttpHeaders.LAST_EVENT_ID), "last-1"))
      )
      read <- response.bodyText.interruptAfter(1.second).compile.string.start
      _    <- gate.acquire.timeout(1.second)
      _    <- IO.sleep(100.millis)
      _    <- ReactorInterop.monoToIO(
        provider.notifyClient(sessionId, "after-replay", java.util.Map.of("ok", "true"))
      )
      _    <- IO.sleep(100.millis)
      _    <- ReactorInterop.monoToIO(provider.closeGracefully())
      body <- read.joinWithNever
      seen <- replayed.get
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(seen, Some("last-1"))
      assert(body.contains("replayed"))
      assert(body.contains("after-replay"))
      assert(body.indexOf("replayed") < body.indexOf("after-replay"))
    }
  }

  test("GET attaches live stream only after delayed replay completes") {
    for {
      provider  <- testProvider(replayDelay = Some(java.time.Duration.ofMillis(200)))
      sessionId <- initialize(provider)
      response  <- provider.routes.orNotFound.run(
        get(Some(sessionId)).putHeaders(
          Header.Raw(CIString(HttpHeaders.LAST_EVENT_ID), "last-delayed")
        )
      )
      read <- response.bodyText.interruptAfter(1.second).compile.string.start
      _    <- IO.sleep(50.millis)
      _    <- ReactorInterop
        .monoToIO(provider.notifyClient(sessionId, "during-replay", java.util.Map.of("ok", "true")))
        .attempt
      _ <- IO.sleep(500.millis)
      _ <- ReactorInterop.monoToIO(
        provider.notifyClient(sessionId, "after-replay", java.util.Map.of("ok", "true"))
      )
      _    <- IO.sleep(100.millis)
      _    <- ReactorInterop.monoToIO(provider.closeGracefully())
      body <- read.joinWithNever
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
      provider       <- testProvider()
      firstSession   <- initialize(provider)
      secondSession  <- initialize(provider)
      firstResponse  <- provider.routes.orNotFound.run(get(Some(firstSession)))
      firstRead      <- firstResponse.bodyText.interruptAfter(100.millis).compile.string.start
      _              <- firstRead.joinWithNever
      secondResponse <- provider.routes.orNotFound.run(get(Some(secondSession)))
      secondRead     <- secondResponse.bodyText.interruptAfter(1.second).compile.string.start
      _              <- IO.sleep(100.millis)
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

  test("Reactor Mono subscription is disposed when IO is canceled") {
    for {
      canceled <- Ref.of[IO, Boolean](false)
      mono = Mono.never[Void]().doOnCancel(() => canceled.set(true).unsafeRunAndForget())
      fiber <- ReactorInterop.monoToIO(mono).start
      _     <- fiber.cancel
      _     <- IO.sleep(100.millis)
      value <- canceled.get
    } yield assert(value)
  }

  test("IO fiber is canceled when Reactor subscriber disposes") {
    for {
      canceled <- Ref.of[IO, Boolean](false)
      mono = ReactorInterop.ioUnitToMono(IO.never.onCancel(canceled.set(true)))
      disposable <- IO(mono.subscribe())
      _          <- IO(disposable.dispose())
      _          <- IO.sleep(100.millis)
      value      <- canceled.get
    } yield assert(value)
  }

  private def testProvider(
      notificationHandler: Option[McpTransportContext => IO[Unit]] = None,
      replayObserver: Option[AnyRef => IO[Unit]] = None,
      replayDelay: Option[java.time.Duration] = None,
      config: Http4sStreamableServerTransportProviderConfig =
        Http4sStreamableServerTransportProviderConfig()
  ): IO[Http4sStreamableServerTransportProvider] =
    IO {
      val provider = Http4sStreamableServerTransportProvider(jsonMapper = mapper, config = config)
      provider.setSessionFactory(
        new TestSessionFactory(notificationHandler, replayObserver, replayDelay)
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
      replayDelay: Option[java.time.Duration]
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
        replayDelay
      )
      val result = McpSchema.InitializeResult
        .builder(
          ProtocolVersions.MCP_2025_06_18,
          McpSchema.ServerCapabilities.builder().build(),
          McpSchema.Implementation.builder("test-server", "1.0").build()
        )
        .build()
      new McpStreamableServerSession.McpStreamableServerSessionInit(session, Mono.just(result))
    }
  }

  private final class TestSession(
      id: String,
      capabilities: McpSchema.ClientCapabilities,
      clientInfo: McpSchema.Implementation,
      requestHandlers: java.util.Map[String, McpRequestHandler[_]],
      notificationHandlers: java.util.Map[String, McpNotificationHandler],
      replayObserver: Option[AnyRef => IO[Unit]],
      replayDelay: Option[java.time.Duration]
  ) extends McpStreamableServerSession(
        id,
        capabilities,
        clientInfo,
        Duration.ofSeconds(2),
        requestHandlers,
        notificationHandlers,
        () => Mono.empty[Void]()
      ) {
    override def replay(lastEventId: Object): Flux[McpSchema.JSONRPCMessage] = {
      replayObserver.foreach(observer => observer(lastEventId).unsafeRunAndForget())
      val messages: Flux[McpSchema.JSONRPCMessage] = Flux.just(
        new McpSchema.JSONRPCNotification(
          "replayed",
          java.util.Map.of("id", lastEventId)
        ): McpSchema.JSONRPCMessage
      )
      replayDelay.fold(messages)(messages.delayElements)
    }
  }
}
