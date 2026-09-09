package io.github.http4smcp

import cats.effect.IO
import cats.effect.kernel.Poll
import cats.effect.unsafe.IORuntime
import cats.syntax.all._
import fs2.Stream
import io.github.http4smcp.internal.ReactorInterop
import io.modelcontextprotocol.common.McpTransportContext
import io.modelcontextprotocol.json.McpJsonDefaults
import io.modelcontextprotocol.json.McpJsonMapper
import io.modelcontextprotocol.json.TypeRef
import io.modelcontextprotocol.spec.HttpHeaders
import io.modelcontextprotocol.spec.McpError
import io.modelcontextprotocol.spec.McpSchema
import io.modelcontextprotocol.spec.McpStreamableServerSession
import io.modelcontextprotocol.spec.McpStreamableServerTransportProvider
import io.modelcontextprotocol.util.KeepAliveScheduler
import org.http4s.CacheDirective
import org.http4s.Header
import org.http4s.HttpRoutes
import org.http4s.MediaType
import org.http4s.Method
import org.http4s.Request
import org.http4s.Response
import org.http4s.ServerSentEvent
import org.http4s.ServerSentEvent.EventId
import org.http4s.Status
import org.http4s.dsl.io._
import org.http4s.headers.`Cache-Control`
import org.http4s.headers.`Content-Type`
import org.slf4j.LoggerFactory
import org.typelevel.ci.CIString
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

final class Http4sStreamableServerTransportProvider(
    jsonMapper: McpJsonMapper,
    config: Http4sStreamableServerTransportProviderConfig
)(implicit runtime: IORuntime)
    extends McpStreamableServerTransportProvider {

  private val logger        = LoggerFactory.getLogger(getClass)
  private val sessions      = new ConcurrentHashMap[String, McpStreamableServerSession]()
  private val lifecycleLock = new AnyRef
  @volatile private var sessionFactory: McpStreamableServerSession.Factory = _
  @volatile private var closing: Boolean                                   = false

  private val keepAliveScheduler: Option[KeepAliveScheduler] =
    config.keepAliveInterval.map { interval =>
      val scheduler = KeepAliveScheduler
        .builder(() => if (closing) Flux.empty() else Flux.fromIterable(sessions.values()))
        .initialDelay(interval)
        .interval(interval)
        .build()
      scheduler.start()
      scheduler
    }

  val routes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case request if isEndpoint(request) =>
      request.method match {
        case Method.POST   => handlePost(request)
        case Method.GET    => handleGet(request)
        case Method.DELETE => handleDelete(request)
        case _             => IO.pure(Response[IO](status = Status.MethodNotAllowed))
      }
  }

  override def setSessionFactory(sessionFactory: McpStreamableServerSession.Factory): Unit =
    this.sessionFactory = sessionFactory

  override def notifyClients(method: String, params: Object): Mono[Void] = {
    val sessionsSnapshot = sessions.values().asScala.toList
    Flux
      .fromIterable(sessionsSnapshot.asJava)
      .flatMap { session =>
        session.sendNotification(method, params).onErrorResume { error =>
          logger.warn(s"Failed to notify session ${session.getId}", error)
          Mono.empty()
        }
      }
      .`then`()
  }

  override def notifyClient(sessionId: String, method: String, params: Object): Mono[Void] =
    Option(sessions.get(sessionId)) match {
      case Some(session) => session.sendNotification(method, params)
      case None          => Mono.empty()
    }

  private val shutdownOperation: Mono[Void] = Mono
    .defer[Void](() => {
      val sessionsSnapshot = lifecycleLock.synchronized {
        closing = true
        sessions.values().asScala.toList
      }
      keepAliveScheduler.foreach(_.shutdown())
      Flux
        .fromIterable(sessionsSnapshot.asJava)
        .flatMap { session =>
          session.closeGracefully().onErrorResume { error =>
            logger.warn(s"Failed to close session ${session.getId}", error)
            Mono.empty[Void]()
          }
        }
        .`then`(Mono.fromRunnable(() => lifecycleLock.synchronized(sessions.clear())))
    })
    .cache()

  override def closeGracefully(): Mono[Void] = shutdownOperation

  private def handlePost(request: Request[IO]): IO[Response[IO]] =
    if (closing) ServiceUnavailable("Server is shutting down")
    else
      readPostBody(request).flatMap {
        case None       => IO.pure(Response[IO](status = Status.PayloadTooLarge))
        case Some(body) =>
          val transportContext = config.contextExtractor.extract(request)
          IO.blocking(McpSchema.deserializeJsonRpcMessage(jsonMapper, body)).attempt.flatMap {
            case Left(error) =>
              errorResponse(
                Status.BadRequest,
                McpSchema.ErrorCodes.INVALID_REQUEST,
                s"Invalid message format: ${message(error)}"
              )
            case Right(requestMessage: McpSchema.JSONRPCRequest)
                if requestMessage.method() == McpSchema.METHOD_INITIALIZE =>
              handleInitialize(request, requestMessage, transportContext)
            case Right(message) =>
              handleSessionPost(request, message, transportContext)
          }
      }

  private def handleInitialize(
      request: Request[IO],
      jsonrpcRequest: McpSchema.JSONRPCRequest,
      transportContext: McpTransportContext
  ): IO[Response[IO]] =
    if (!hasPostAccept(request)) badPostAcceptResponse
    else if (sessionFactory == null)
      errorResponse(
        Status.InternalServerError,
        McpSchema.ErrorCodes.INTERNAL_ERROR,
        "Session factory has not been set"
      )
    else {
      val initializeRequestType = new TypeRef[McpSchema.InitializeRequest]() {}
      for {
        initializeRequest <- IO.blocking(
          jsonMapper.convertValue(jsonrpcRequest.params(), initializeRequestType)
        )
        response <- IO.uncancelable { poll =>
          IO.blocking(sessionFactory.startSession(initializeRequest)).flatMap { init =>
            completeInitialization(init, jsonrpcRequest, transportContext, poll)
          }
        }
      } yield response
    }

  private def handleSessionPost(
      request: Request[IO],
      message: McpSchema.JSONRPCMessage,
      transportContext: McpTransportContext
  ): IO[Response[IO]] =
    sessionFromRequest(request, requireAccept = true).flatMap {
      case Left(response)              => IO.pure(response)
      case Right((sessionId, session)) =>
        message match {
          case response: McpSchema.JSONRPCResponse =>
            ReactorInterop
              .monoToIO(
                session
                  .accept(response)
                  .contextWrite(ctx => ctx.put(McpTransportContext.KEY, transportContext))
              )
              .attempt
              .flatMap {
                case Left(error) =>
                  IO(
                    logger
                      .error(s"Failed to accept JSON-RPC response for session $sessionId", error)
                  )
                case Right(_) => IO.unit
              } *> Accepted()
          case notification: McpSchema.JSONRPCNotification =>
            ReactorInterop
              .monoToIO(
                session
                  .accept(notification)
                  .contextWrite(ctx => ctx.put(McpTransportContext.KEY, transportContext))
              )
              .attempt
              .flatMap {
                case Left(error) =>
                  IO(
                    logger.error(
                      s"Failed to accept JSON-RPC notification for session $sessionId",
                      error
                    )
                  )
                case Right(_) => IO.unit
              } *> Accepted()
          case rpcRequest: McpSchema.JSONRPCRequest =>
            for {
              transport <- Http4sStreamableServerTransport.create(sessionId, jsonMapper)
              fiber     <- ReactorInterop
                .monoToIO(
                  session
                    .responseStream(rpcRequest, transport)
                    .contextWrite(ctx => ctx.put(McpTransportContext.KEY, transportContext))
                )
                .handleErrorWith { error =>
                  IO(
                    logger.error(s"Failed to process response stream for session $sessionId", error)
                  )
                }
                .guarantee(closeResponseTransport(sessionId, transport))
                .start
              response <- sseResponse(transport.events.onFinalize(fiber.cancel))
            } yield response
          case _ =>
            errorResponse(
              Status.InternalServerError,
              McpSchema.ErrorCodes.INVALID_REQUEST,
              "Unknown message type"
            )
        }
    }

  private def handleGet(request: Request[IO]): IO[Response[IO]] =
    if (closing) ServiceUnavailable("Server is shutting down")
    else {
      val transportContext = config.contextExtractor.extract(request)
      sessionFromRequest(request, requireAccept = false).flatMap {
        case Left(response)              => IO.pure(response)
        case Right((sessionId, session)) =>
          for {
            transport <- Http4sStreamableServerTransport.create(sessionId, jsonMapper)
            replay = request.headers
              .get(CIString(HttpHeaders.LAST_EVENT_ID))
              .fold[Stream[IO, ServerSentEvent]](Stream.empty.covary[IO]) { header =>
                ReactorInterop
                  .fluxToStream(
                    session
                      .replay(header.head.value)
                      .contextWrite(ctx => ctx.put(McpTransportContext.KEY, transportContext))
                  )
                  .evalMap(replayEvent(sessionId))
              }
            live = Stream
              .eval(IO(session.listeningStream(transport)))
              .flatMap { listeningStream =>
                transport.events.onFinalize(IO(listeningStream.close()))
              }
            response <- sseResponse(replay ++ live)
          } yield response
      }
    }

  private def handleDelete(request: Request[IO]): IO[Response[IO]] =
    if (closing) ServiceUnavailable("Server is shutting down")
    else if (config.disallowDelete) IO.pure(Response[IO](status = Status.MethodNotAllowed))
    else {
      val transportContext = config.contextExtractor.extract(request)
      headerValue(request, HttpHeaders.MCP_SESSION_ID) match {
        case None =>
          errorResponse(
            Status.BadRequest,
            McpSchema.ErrorCodes.METHOD_NOT_FOUND,
            "Session ID required in mcp-session-id header"
          )
        case Some(sessionId) =>
          Option(sessions.get(sessionId)) match {
            case None          => NotFound()
            case Some(session) =>
              ReactorInterop
                .monoToIO(
                  session
                    .delete()
                    .contextWrite(ctx => ctx.put(McpTransportContext.KEY, transportContext))
                )
                .attempt
                .flatMap {
                  case Right(_)    => IO(sessions.remove(sessionId)).void *> Ok()
                  case Left(error) =>
                    errorResponse(
                      Status.InternalServerError,
                      McpSchema.ErrorCodes.INTERNAL_ERROR,
                      message(error)
                    )
                }
          }
      }
    }

  private def sessionFromRequest(
      request: Request[IO],
      requireAccept: Boolean
  ): IO[Either[Response[IO], (String, McpStreamableServerSession)]] = {
    val errors = List(
      Option.when(requireAccept && !hasPostAccept(request))(
        "text/event-stream and application/json required in Accept header"
      ),
      Option.when(!requireAccept && !acceptsEventStream(request))(
        "text/event-stream required in Accept header"
      ),
      Option.when(headerValue(request, HttpHeaders.MCP_SESSION_ID).forall(_.trim.isEmpty))(
        "Session ID required in mcp-session-id header"
      )
    ).flatten

    if (errors.nonEmpty)
      errorResponse(Status.BadRequest, McpSchema.ErrorCodes.METHOD_NOT_FOUND, errors.mkString("; "))
        .map(Left(_))
    else {
      val sessionId = headerValue(request, HttpHeaders.MCP_SESSION_ID).get
      Option(sessions.get(sessionId)) match {
        case Some(session)         => IO.pure(Right(sessionId -> session))
        case None if requireAccept =>
          errorResponse(
            Status.NotFound,
            McpSchema.ErrorCodes.INTERNAL_ERROR,
            s"Session not found: $sessionId"
          ).map(Left(_))
        case None => NotFound().map(Left(_))
      }
    }
  }

  private def readPostBody(request: Request[IO]): IO[Option[String]] =
    if (request.contentLength.exists(_ > Http4sStreamableServerTransportProvider.RequestMaxBytes))
      IO.pure(None)
    else
      request.body
        .take(Http4sStreamableServerTransportProvider.RequestMaxBytes + 1)
        .compile
        .to(Array)
        .map { bytes =>
          Option.when(bytes.length <= Http4sStreamableServerTransportProvider.RequestMaxBytes)(
            new String(bytes, StandardCharsets.UTF_8)
          )
        }

  private def completeInitialization(
      init: McpStreamableServerSession.McpStreamableServerSessionInit,
      jsonrpcRequest: McpSchema.JSONRPCRequest,
      transportContext: McpTransportContext,
      poll: Poll[IO]
  ): IO[Response[IO]] = {
    val session  = init.session()
    val complete = for {
      initResult <- ReactorInterop.monoToIO(
        init.initResult().contextWrite(ctx => ctx.put(McpTransportContext.KEY, transportContext))
      )
      // McpJsonMapper is synchronous, so JSON encoding is isolated at the HTTP boundary.
      body <- IO.blocking(
        jsonMapper.writeValueAsString(
          McpSchema.JSONRPCResponse.result(jsonrpcRequest.id(), initResult)
        )
      )
      registered <- registerSession(session)
      response   <-
        if (registered)
          Ok(body, `Content-Type`(MediaType.application.json)).map(
            _.putHeaders(Header.Raw(CIString(HttpHeaders.MCP_SESSION_ID), session.getId))
          )
        else
          closeSession(session, "created after shutdown began") *>
            ServiceUnavailable("Server is shutting down")
    } yield response

    poll(complete)
      .onError { case error =>
        IO(logger.error(s"Failed to initialize session ${session.getId}", error)) *>
          discardSession(session, "initialization failed")
      }
      .onCancel(discardSession(session, "initialization was canceled"))
  }

  private def registerSession(session: McpStreamableServerSession): IO[Boolean] =
    IO {
      lifecycleLock.synchronized {
        if (closing) false
        else {
          sessions.put(session.getId, session)
          true
        }
      }
    }

  private def discardSession(
      session: McpStreamableServerSession,
      reason: String
  ): IO[Unit] =
    IO(lifecycleLock.synchronized(sessions.remove(session.getId, session))).void *>
      closeSession(session, reason)

  private def closeSession(session: McpStreamableServerSession, reason: String): IO[Unit] =
    IO(session.close()).handleErrorWith { error =>
      IO(logger.warn(s"Failed to close session ${session.getId} after $reason", error))
    }

  private def closeResponseTransport(
      sessionId: String,
      transport: Http4sStreamableServerTransport
  ): IO[Unit] =
    ReactorInterop.monoToIO(transport.closeGracefully()).void.handleErrorWith { error =>
      IO(logger.warn(s"Failed to close response transport for session $sessionId", error))
    }

  private def badPostAcceptResponse: IO[Response[IO]] =
    errorResponse(
      Status.BadRequest,
      McpSchema.ErrorCodes.METHOD_NOT_FOUND,
      "text/event-stream and application/json required in Accept header"
    )

  private def errorResponse(status: Status, code: Int, text: String): IO[Response[IO]] = {
    val error = McpError.builder(code).message(text).build()
    IO.blocking(jsonMapper.writeValueAsString(error)).flatMap { body =>
      Response[IO](status = status)
        .withEntity(body)
        .withContentType(`Content-Type`(MediaType.application.json))
        .pure[IO]
    }
  }

  private def replayEvent(
      sessionId: String
  )(message: McpSchema.JSONRPCMessage): IO[ServerSentEvent] =
    IO.blocking(jsonMapper.writeValueAsString(message)).map { json =>
      ServerSentEvent(Some(json), Some("message"), Some(EventId(sessionId)))
    }

  private def sseResponse(events: Stream[IO, ServerSentEvent]): IO[Response[IO]] =
    Ok(events).map(
      _.putHeaders(
        `Cache-Control`(CacheDirective.`no-cache`()),
        Header.Raw(CIString("Connection"), "keep-alive")
      )
    )

  private def isEndpoint(request: Request[IO]): Boolean =
    request.uri.path.renderString == config.endpoint || request.pathInfo.renderString == config.endpoint

  private def hasPostAccept(request: Request[IO]): Boolean =
    accepts(request, "text/event-stream") && accepts(request, "application/json")

  private def acceptsEventStream(request: Request[IO]): Boolean =
    accepts(request, "text/event-stream")

  private def accepts(request: Request[IO], mediaType: String): Boolean =
    request.headers.get(CIString(HttpHeaders.ACCEPT)).exists(_.head.value.contains(mediaType))

  private def headerValue(request: Request[IO], name: String): Option[String] =
    request.headers.get(CIString(name)).map(_.head.value)

  private def message(error: Throwable): String =
    Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
}

object Http4sStreamableServerTransportProvider {
  private val RequestMaxBytes = 16L * 1024 * 1024

  def apply(
      jsonMapper: McpJsonMapper = McpJsonDefaults.getMapper,
      config: Http4sStreamableServerTransportProviderConfig =
        Http4sStreamableServerTransportProviderConfig()
  )(implicit runtime: IORuntime): Http4sStreamableServerTransportProvider =
    new Http4sStreamableServerTransportProvider(jsonMapper, config)

  @deprecated(
    "Use Http4sStreamableServerTransportProvider(...).routes to retain provider lifecycle access",
    "0.1.1"
  )
  def routes(
      jsonMapper: McpJsonMapper = McpJsonDefaults.getMapper,
      config: Http4sStreamableServerTransportProviderConfig =
        Http4sStreamableServerTransportProviderConfig()
  )(implicit runtime: IORuntime): HttpRoutes[IO] =
    apply(jsonMapper, config).routes
}
