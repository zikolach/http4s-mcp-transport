package io.github.http4smcp

import cats.effect.Deferred
import cats.effect.IO
import cats.effect.Ref
import cats.effect.kernel.Fiber
import cats.effect.unsafe.IORuntime
import cats.syntax.all._
import fs2.Stream
import io.github.http4smcp.internal.ReactorInterop
import io.modelcontextprotocol.common.McpTransportContext
import io.modelcontextprotocol.json.McpJsonDefaults
import io.modelcontextprotocol.json.McpJsonMapper
import io.modelcontextprotocol.json.TypeRef
import io.modelcontextprotocol.server.transport.ServerTransportSecurityException
import io.modelcontextprotocol.spec.HttpHeaders
import io.modelcontextprotocol.spec.McpError
import io.modelcontextprotocol.spec.McpSchema
import io.modelcontextprotocol.spec.McpSession
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
import org.http4s.headers.Accept
import org.http4s.headers.`Cache-Control`
import org.http4s.headers.`Content-Type`
import org.slf4j.LoggerFactory
import org.typelevel.ci.CIString
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

final class Http4sStreamableServerTransportProvider(
    jsonMapper: McpJsonMapper,
    config: Http4sStreamableServerTransportProviderConfig
)(implicit runtime: IORuntime)
    extends McpStreamableServerTransportProvider {

  private sealed trait OwnedSessionResource {
    def terminate: IO[Unit]
  }

  private final class OwnedResponse extends OwnedSessionResource {
    val stop: Deferred[IO, Unit]        = Deferred.unsafe[IO, Unit]
    val cleanup: Deferred[IO, IO[Unit]] = Deferred.unsafe[IO, IO[Unit]]
    val bodyDone: Deferred[IO, Unit]    = Deferred.unsafe[IO, Unit]
    private val cleanupDone             = Deferred.unsafe[IO, Either[Throwable, Unit]]
    private val cleanupStarted          = new AtomicBoolean(false)

    private def awaitCleanup: IO[Unit] =
      IO.uncancelable { _ =>
        IO(cleanupStarted.compareAndSet(false, true)).flatMap {
          case true =>
            cleanup.get.flatten.attempt
              .flatMap { result =>
                result.leftTraverse(_ => safeCleanupLog) *> cleanupDone.complete(result).void
              }
              .start
              .void
          case false => IO.unit
        }
      } *> cleanupDone.get.flatMap(_.liftTo[IO])

    def finishBody: IO[Unit] =
      awaitCleanup.attempt *> bodyDone.complete(()).void

    def terminate: IO[Unit] =
      stop.complete(()).void *> awaitCleanup.attempt *> bodyDone.get
  }

  private final class OwnedAcceptance extends OwnedSessionResource {
    val stop: Deferred[IO, Unit] = Deferred.unsafe[IO, Unit]
    val done: Deferred[IO, Unit] = Deferred.unsafe[IO, Unit]

    override def terminate: IO[Unit] =
      stop.complete(()).void *> done.get
  }

  private sealed trait Reservation
  private final class Reserved extends Reservation {
    val stop: Deferred[IO, Unit]    = Deferred.unsafe[IO, Unit]
    val done: Deferred[IO, Unit]    = Deferred.unsafe[IO, Unit]
    val released                    = new AtomicBoolean(false)
    val cleanupStarted              = new AtomicBoolean(false)
    var entry: Option[SessionEntry] = None
  }
  private case object AtCapacity   extends Reservation
  private case object ShuttingDown extends Reservation

  private final class SessionEntry(
      val session: McpStreamableServerSession,
      val reservation: Reserved,
      var lastIdleNanos: Long
  ) {
    val resources: mutable.Set[OwnedSessionResource]      = mutable.Set.empty
    val terminated: Deferred[IO, Either[Throwable, Unit]] =
      Deferred.unsafe[IO, Either[Throwable, Unit]]
    var activeOperations: Int = 0
    var terminating: Boolean  = false
  }

  private val logger          = LoggerFactory.getLogger(getClass)
  private val sessions        = new ConcurrentHashMap[String, SessionEntry]()
  private val lifecycleLock   = new AnyRef
  private val timeoutNanos    = config.sessionIdleTimeout.toNanos
  private val sweepNanos      = math.max(1.millis.toNanos, math.min(timeoutNanos, 1.second.toNanos))
  private val shutdownStarted = new AtomicBoolean(false)
  private val shutdownComplete = Deferred.unsafe[IO, Either[Throwable, Unit]]
  @volatile private var sessionFactory: McpStreamableServerSession.Factory = _
  @volatile private var closing: Boolean                                   = false
  @volatile private var afterReservationHook: IO[Unit]                     = IO.unit
  @volatile private var afterResponseOwnershipHook: IO[Unit]               = IO.unit
  @volatile private var afterTerminationHook: IO[Unit]                     = IO.unit
  @volatile private var monotonicNanosHook: Option[IO[Long]]               = None
  private val reservations = mutable.Set.empty[Reserved]

  private val keepAliveScheduler: Option[KeepAliveScheduler] =
    config.keepAliveInterval.map { interval =>
      val scheduler = KeepAliveScheduler
        .builder(() =>
          if (closing) Flux.empty()
          else Flux.fromIterable(sessionValues.map(session => session: McpSession).asJava)
        )
        .initialDelay(interval)
        .interval(interval)
        .build()
      scheduler.start()
      scheduler
    }

  private val expiryFiber: Fiber[IO, Throwable, Unit] = expiryLoop.start.unsafeRunSync()

  val routes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case request if isEndpoint(request) =>
      request.method match {
        case Method.POST | Method.GET | Method.DELETE =>
          validateSecurity(request).flatMap {
            case Some(response) => IO.pure(response)
            case None           =>
              request.method match {
                case Method.POST   => handlePost(request)
                case Method.GET    => handleGet(request)
                case Method.DELETE => handleDelete(request)
                case _             => IO.pure(Response[IO](status = Status.MethodNotAllowed))
              }
          }
        case _ => IO.pure(Response[IO](status = Status.MethodNotAllowed))
      }
  }

  override def setSessionFactory(factory: McpStreamableServerSession.Factory): Unit =
    sessionFactory = factory

  override def notifyClients(method: String, params: Object): Mono[Void] =
    Flux
      .fromIterable(sessionValues.asJava)
      .flatMap { session =>
        session.sendNotification(method, params).onErrorResume { _ =>
          logger.warn("MCP_TRANSPORT_NOTIFY_FAILED")
          Mono.empty()
        }
      }
      .`then`()

  override def notifyClient(sessionId: String, method: String, params: Object): Mono[Void] =
    Option(sessions.get(sessionId)) match {
      case Some(entry) if !entry.terminating => entry.session.sendNotification(method, params)
      case _                                 => Mono.empty()
    }

  override def closeGracefully(): Mono[Void] =
    ReactorInterop.ioUnitToMono(shutdownIO)

  private def shutdownIO: IO[Unit] =
    IO.uncancelable { _ =>
      IO(shutdownStarted.compareAndSet(false, true)).flatMap {
        case true =>
          IO(lifecycleLock.synchronized {
            closing = true
          }) *> performShutdown.start.void
        case false => IO.unit
      }
    } *> shutdownComplete.get.flatMap(_.liftTo[IO])

  private def performShutdown: IO[Unit] = {
    val (entries, allReservations, pendingReservations) = lifecycleLock.synchronized {
      closing = true
      val all     = reservations.toList
      val entries = all.flatMap(_.entry).distinct
      (entries, all, all.filter(_.entry.isEmpty))
    }
    val stopPending = pendingReservations.traverse_(_.stop.complete(()).void)
    val cleanup     =
      expiryFiber.cancel *>
        cleanupAll(keepAliveScheduler.toList.map(scheduler => IO(scheduler.shutdown()))) *>
        stopPending *>
        entries.parTraverse_(entry => terminate(entry, session => IO(session.closeGracefully()))) *>
        allReservations.parTraverse_(_.done.get)
    cleanup.attempt.flatMap(result => shutdownComplete.complete(result).void)
  }

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
                s"Invalid message format: ${errorMessage(error)}"
              )
            case Right(requestMessage: McpSchema.JSONRPCRequest)
                if requestMessage.method() == McpSchema.METHOD_INITIALIZE =>
              handleInitialize(request, requestMessage, transportContext)
            case Right(message) => handleSessionPost(request, message, transportContext)
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
    else
      IO.uncancelable { poll =>
        reserveCapacity.flatMap {
          case AtCapacity            => ServiceUnavailable("Session capacity reached")
          case ShuttingDown          => ServiceUnavailable("Server is shutting down")
          case reservation: Reserved =>
            Ref.of[IO, Option[McpStreamableServerSession]](None).flatMap { startedSession =>
              val initializeRequestType = new TypeRef[McpSchema.InitializeRequest]() {}
              val initialize            =
                poll(afterReservationHook) *>
                  IO.blocking(
                    jsonMapper.convertValue(jsonrpcRequest.params(), initializeRequestType)
                  ).flatMap { initializeRequest =>
                    IO.uncancelable { initializationPoll =>
                      IO.blocking(sessionFactory.startSession(initializeRequest)).flatMap { init =>
                        val session = init.session()
                        startedSession.set(Some(session)) *>
                          completeInitialization(
                            init,
                            jsonrpcRequest,
                            transportContext,
                            reservation,
                            initializationPoll
                          )
                      }
                    }
                  }
              poll(IO.race(reservation.stop.get, initialize))
                .flatMap {
                  case Left(_) =>
                    cleanupInitialization(startedSession, reservation) *>
                      ServiceUnavailable("Server is shutting down")
                  case Right(response) => IO.pure(response)
                }
                .onError { case _ =>
                  IO(logger.error("MCP_TRANSPORT_INITIALIZE_FAILED")) *>
                    cleanupInitialization(startedSession, reservation)
                }
                .onCancel(cleanupInitialization(startedSession, reservation))
            }
        }
      }

  private def completeInitialization(
      init: McpStreamableServerSession.McpStreamableServerSessionInit,
      jsonrpcRequest: McpSchema.JSONRPCRequest,
      transportContext: McpTransportContext,
      reservation: Reserved,
      poll: cats.effect.kernel.Poll[IO]
  ): IO[Response[IO]] = {
    val session = init.session()
    for {
      initResult <- poll(
        IO.defer(
          ReactorInterop.monoToIO(
            init
              .initResult()
              .contextWrite(ctx => ctx.put(McpTransportContext.KEY, transportContext))
          )
        )
      )
      body <- IO.blocking(
        jsonMapper.writeValueAsString(
          McpSchema.JSONRPCResponse.result(jsonrpcRequest.id(), initResult)
        )
      )
      now        <- monotonicNanos
      registered <- registerSession(session, reservation, now)
      response   <-
        if (registered)
          Ok(body, `Content-Type`(MediaType.application.json)).map(
            _.putHeaders(Header.Raw(CIString(HttpHeaders.MCP_SESSION_ID), session.getId))
          )
        else
          closeStartedSession(session) *> releaseReservation(reservation) *>
            ServiceUnavailable("Server is shutting down")
    } yield response
  }

  private def cleanupInitialization(
      startedSession: Ref[IO, Option[McpStreamableServerSession]],
      reservation: Reserved
  ): IO[Unit] =
    IO(reservation.cleanupStarted.compareAndSet(false, true)).flatMap {
      case false => reservation.done.get
      case true  =>
        startedSession.get.flatMap {
          case Some(session) =>
            IO(lifecycleLock.synchronized(reservation.entry)).flatMap {
              case Some(entry) =>
                terminate(entry, started => IO(started.closeGracefully())).attempt.void
              case None => closeStartedSession(session) *> releaseReservation(reservation)
            }
          case None => releaseReservation(reservation)
        }
    }

  private def handleSessionPost(
      request: Request[IO],
      message: McpSchema.JSONRPCMessage,
      transportContext: McpTransportContext
  ): IO[Response[IO]] =
    sessionFromRequest(request, requirePostAccept = true).flatMap {
      case Left(response) => IO.pure(response)
      case Right(entry)   =>
        message match {
          case response: McpSchema.JSONRPCResponse =>
            processWithActivity(entry) {
              IO.defer(
                ReactorInterop.monoCompletionToIO(
                  entry.session
                    .accept(response)
                    .contextWrite(ctx => ctx.put(McpTransportContext.KEY, transportContext))
                )
              ).attempt
                .flatMap {
                  case Left(_)  => IO(logger.error("MCP_TRANSPORT_RESPONSE_ACCEPT_FAILED"))
                  case Right(_) => IO.unit
                }
            }.flatMap {
              case true  => Accepted()
              case false => sessionNotFound(request, requirePostAccept = true)
            }
          case notification: McpSchema.JSONRPCNotification =>
            processWithActivity(entry) {
              IO.defer(
                ReactorInterop.monoCompletionToIO(
                  entry.session
                    .accept(notification)
                    .contextWrite(ctx => ctx.put(McpTransportContext.KEY, transportContext))
                )
              ).attempt
                .flatMap {
                  case Left(_)  => IO(logger.error("MCP_TRANSPORT_NOTIFICATION_ACCEPT_FAILED"))
                  case Right(_) => IO.unit
                }
            }.flatMap {
              case true  => Accepted()
              case false => sessionNotFound(request, requirePostAccept = true)
            }
          case rpcRequest: McpSchema.JSONRPCRequest =>
            touch(entry).flatMap {
              case true  => sseResponse(postResponseStream(entry, rpcRequest, transportContext))
              case false => sessionNotFound(request, requirePostAccept = true)
            }
          case _ =>
            errorResponse(
              Status.InternalServerError,
              McpSchema.ErrorCodes.INVALID_REQUEST,
              "Unknown message type"
            )
        }
    }

  private def postResponseStream(
      entry: SessionEntry,
      request: McpSchema.JSONRPCRequest,
      context: McpTransportContext
  ): Stream[IO, ServerSentEvent] =
    ownedStream(entry) { owned =>
      Stream
        .bracket(acquireActivity(entry))(active => if (active) releaseActivity(entry) else IO.unit)
        .flatMap {
          case false => Stream.empty
          case true  =>
            Stream.eval {
              IO.uncancelable { _ =>
                for {
                  transport <- Http4sStreamableServerTransport.create(
                    entry.session.getId,
                    jsonMapper,
                    config.outboundBufferCapacity
                  )
                  worker <- IO
                    .defer(
                      ReactorInterop
                        .monoCompletionToIO(
                          entry.session
                            .responseStream(request, transport)
                            .contextWrite(ctx => ctx.put(McpTransportContext.KEY, context))
                        )
                    )
                    .handleErrorWith(_ => IO(logger.error("MCP_TRANSPORT_RESPONSE_STREAM_FAILED")))
                    .guarantee(transport.closeIO)
                    .start
                  _ <- owned.cleanup
                    .complete(cleanupAll(List(worker.cancel, transport.closeIO)))
                    .void
                } yield transport.events.interruptWhen(owned.stop.get.attempt)
              }
            }.flatten
        }
    }

  private def handleGet(request: Request[IO]): IO[Response[IO]] =
    if (closing) ServiceUnavailable("Server is shutting down")
    else
      sessionFromRequest(request, requirePostAccept = false).flatMap {
        case Left(response) => IO.pure(response)
        case Right(entry)   =>
          touch(entry).flatMap {
            case false => NotFound()
            case true  => sseResponse(getResponseStream(entry, request))
          }
      }

  private def getResponseStream(
      entry: SessionEntry,
      request: Request[IO]
  ): Stream[IO, ServerSentEvent] =
    ownedStream(entry) { owned =>
      Stream.eval(Ref.of[IO, Option[IO[Unit]]](None)).flatMap { listenerRef =>
        val transportContext = config.contextExtractor.extract(request)
        val replay           = request.headers
          .get(CIString(HttpHeaders.LAST_EVENT_ID))
          .fold[Stream[IO, ServerSentEvent]](Stream.empty.covary[IO]) { header =>
            Stream
              .bracket(acquireActivity(entry))(active =>
                if (active) releaseActivity(entry) else IO.unit
              )
              .flatMap {
                case false => Stream.empty
                case true  =>
                  ReactorInterop
                    .fluxToStream(
                      entry.session
                        .replay(header.head.value)
                        .contextWrite(ctx => ctx.put(McpTransportContext.KEY, transportContext))
                    )
                    .evalMap(replayEvent(entry.session.getId))
              }
          }

        Stream.eval {
          IO.uncancelable { _ =>
            for {
              transport <- Http4sStreamableServerTransport.create(
                entry.session.getId,
                jsonMapper,
                config.outboundBufferCapacity
              )
              cleanup = listenerRef.get
                .flatMap(listener => cleanupAll(listener.toList ++ List(transport.closeIO)))
              _ <- owned.cleanup.complete(cleanup).void
            } yield {
              val live = Stream
                .eval(acquireListener(entry, owned, listenerRef, transport))
                .flatMap {
                  case true  => transport.events
                  case false => Stream.empty
                }
              (replay ++ live).interruptWhen(owned.stop.get.attempt)
            }
          }
        }.flatten
      }
    }

  private def acquireListener(
      entry: SessionEntry,
      owned: OwnedResponse,
      listenerRef: Ref[IO, Option[IO[Unit]]],
      transport: Http4sStreamableServerTransport
  ): IO[Boolean] =
    IO.uncancelable { _ =>
      owned.stop.tryGet.flatMap {
        case Some(_) => IO.pure(false)
        case None    =>
          IO(entry.session.listeningStream(transport)).flatMap { listener =>
            listenerRef.set(Some(IO(listener.close()))) *> owned.stop.tryGet.flatMap {
              case Some(_) => IO(listener.close()).as(false)
              case None    => IO.pure(true)
            }
          }
      }
    }

  private def handleDelete(request: Request[IO]): IO[Response[IO]] =
    if (closing) ServiceUnavailable("Server is shutting down")
    else if (config.disallowDelete) IO.pure(Response[IO](status = Status.MethodNotAllowed))
    else
      headerValue(request, HttpHeaders.MCP_SESSION_ID) match {
        case None =>
          errorResponse(
            Status.BadRequest,
            McpSchema.ErrorCodes.METHOD_NOT_FOUND,
            "Session ID required in mcp-session-id header"
          )
        case Some(sessionId) =>
          Option(sessions.get(sessionId)) match {
            case None        => NotFound()
            case Some(entry) =>
              touch(entry).flatMap {
                case false => NotFound()
                case true  =>
                  terminate(
                    entry,
                    session =>
                      IO(session.delete()).map(
                        _.contextWrite(ctx =>
                          ctx.put(McpTransportContext.KEY, config.contextExtractor.extract(request))
                        )
                      )
                  ).attempt.flatMap {
                    case Right(_) => Ok()
                    case Left(_)  =>
                      errorResponse(
                        Status.InternalServerError,
                        McpSchema.ErrorCodes.INTERNAL_ERROR,
                        "Session cleanup failed"
                      )
                  }
              }
          }
      }

  private def ownedStream(
      entry: SessionEntry
  )(use: OwnedResponse => Stream[IO, ServerSentEvent]): Stream[IO, ServerSentEvent] =
    Stream.bracket(registerOwned(entry))(releaseOwned(entry, _)).flatMap {
      case Some(owned) =>
        Stream.eval(IO.race(afterResponseOwnershipHook, owned.stop.get)).flatMap {
          case Left(_) =>
            use(owned).handleErrorWith { error =>
              Stream.eval(owned.cleanup.complete(IO.unit)).drain ++ Stream.raiseError[IO](error)
            }
          case Right(_) => Stream.eval(owned.cleanup.complete(IO.unit)).drain
        }
      case None => Stream.empty
    }

  private def registerOwned(entry: SessionEntry): IO[Option[OwnedResponse]] = IO {
    lifecycleLock.synchronized {
      if (closing || entry.terminating || (sessions.get(entry.session.getId) ne entry)) None
      else {
        val owned = new OwnedResponse
        entry.resources += owned
        Some(owned)
      }
    }
  }

  private def releaseOwned(entry: SessionEntry, owned: Option[OwnedResponse]): IO[Unit] =
    owned.fold(IO.unit) { resource =>
      resource.cleanup.complete(IO.unit).void *> resource.finishBody.guarantee(
        IO(lifecycleLock.synchronized(entry.resources -= resource)).void
      )
    }

  private def acquireActivity(entry: SessionEntry): IO[Boolean] =
    monotonicNanos.flatMap { now =>
      IO.uncancelable { _ =>
        IO(lifecycleLock.synchronized {
          if (closing || entry.terminating || (sessions.get(entry.session.getId) ne entry))
            Left(false)
          else if (entry.activeOperations == 0 && now - entry.lastIdleNanos >= timeoutNanos) {
            retireLocked(entry)
            Left(true)
          } else {
            entry.activeOperations += 1
            Right(true)
          }
        }).flatTap {
          case Left(true) =>
            runTermination(entry, session => IO(session.closeGracefully())).start.void
          case _ => IO.unit
        }
      }.map {
        case Right(value) => value
        case Left(_)      => false
      }
    }

  private def releaseActivity(entry: SessionEntry): IO[Unit] =
    monotonicNanos.flatMap { now =>
      IO {
        lifecycleLock.synchronized {
          if (entry.activeOperations > 0) entry.activeOperations -= 1
          if (entry.activeOperations == 0 && !entry.terminating) entry.lastIdleNanos = now
        }
      }
    }

  private def processWithActivity(entry: SessionEntry)(operation: IO[Unit]): IO[Boolean] =
    IO.uncancelable { poll =>
      registerAcceptance(entry).flatMap {
        case Some(owned) =>
          poll(IO.race(owned.stop.get, operation).void)
            .guarantee(releaseAcceptance(entry, owned))
            .as(true)
        case None => IO.pure(false)
      }
    }

  private def registerAcceptance(entry: SessionEntry): IO[Option[OwnedAcceptance]] =
    monotonicNanos.flatMap { now =>
      IO.uncancelable { _ =>
        IO(lifecycleLock.synchronized {
          if (closing || entry.terminating || (sessions.get(entry.session.getId) ne entry))
            Left(false)
          else if (entry.activeOperations == 0 && now - entry.lastIdleNanos >= timeoutNanos) {
            retireLocked(entry)
            Left(true)
          } else {
            val owned = new OwnedAcceptance
            entry.activeOperations += 1
            entry.resources += owned
            Right(owned)
          }
        }).flatTap {
          case Left(true) =>
            runTermination(entry, session => IO(session.closeGracefully())).start.void
          case _ => IO.unit
        }
      }.map(_.toOption)
    }

  private def releaseAcceptance(entry: SessionEntry, owned: OwnedAcceptance): IO[Unit] =
    releaseActivity(entry).attempt *>
      IO(lifecycleLock.synchronized(entry.resources -= owned)).void *>
      owned.done.complete(()).void

  private def touch(entry: SessionEntry): IO[Boolean] =
    monotonicNanos.flatMap { now =>
      IO.uncancelable { _ =>
        IO(lifecycleLock.synchronized {
          if (closing || entry.terminating || (sessions.get(entry.session.getId) ne entry))
            Left(false)
          else if (entry.activeOperations == 0 && now - entry.lastIdleNanos >= timeoutNanos) {
            retireLocked(entry)
            Left(true)
          } else {
            if (entry.activeOperations == 0) entry.lastIdleNanos = now
            Right(true)
          }
        }).flatTap {
          case Left(true) =>
            runTermination(entry, session => IO(session.closeGracefully())).start.void
          case _ => IO.unit
        }
      }.flatMap {
        case Right(value) => IO.pure(value)
        case Left(true)   => entry.terminated.get.attempt.as(false)
        case Left(false)  => IO.pure(false)
      }
    }

  private def terminate(
      entry: SessionEntry,
      sdkCleanup: McpStreamableServerSession => IO[Mono[Void]]
  ): IO[Unit] =
    IO.uncancelable { _ =>
      IO(lifecycleLock.synchronized {
        if (entry.terminating) false
        else {
          retireLocked(entry)
          true
        }
      }).flatMap {
        case false => IO.unit
        case true  => runTermination(entry, sdkCleanup).start.void
      }
    } *> entry.terminated.get.flatMap(_.liftTo[IO])

  private def retireLocked(entry: SessionEntry): Unit = {
    entry.terminating = true
    sessions.remove(entry.session.getId, entry)
    ()
  }

  private def runTermination(
      entry: SessionEntry,
      sdkCleanup: McpStreamableServerSession => IO[Mono[Void]]
  ): IO[Unit] = {
    val resources = lifecycleLock.synchronized(entry.resources.toList)
    val cleanup   =
      resources.parTraverse_(_.terminate) *> sdkCleanup(entry.session)
        .flatMap(ReactorInterop.monoCompletionToIO)
        .attempt
        .flatMap {
          case Left(_)  => safeCleanupLog
          case Right(_) => IO.unit
        }
    cleanup.attempt.flatMap { result =>
      releaseReservation(entry.reservation) *>
        afterTerminationHook *>
        entry.terminated.complete(result).void
    }
  }

  private def expiryLoop: IO[Unit] =
    (IO.sleep(sweepNanos.nanos) *> expireIdleSessions).foreverM

  private def expireIdleSessions: IO[Unit] =
    monotonicNanos.flatMap { now =>
      val candidates = lifecycleLock.synchronized(sessions.values().asScala.toList)
      candidates.parTraverse_(entry => expireEntry(entry, now))
    }

  private def expireEntry(entry: SessionEntry, now: Long): IO[Unit] =
    IO.uncancelable { _ =>
      IO(lifecycleLock.synchronized {
        if (
          !closing && !entry.terminating && (sessions.get(entry.session.getId) eq entry) &&
          entry.activeOperations == 0 && now - entry.lastIdleNanos >= timeoutNanos
        ) {
          retireLocked(entry)
          true
        } else false
      }).flatTap {
        case true =>
          runTermination(entry, session => IO(session.closeGracefully())).start.void
        case false => IO.unit
      }
    }.void // Shutdown awaits tracked termination; the sweep must not wait for slow cleanup.

  private def reserveCapacity: IO[Reservation] = IO {
    lifecycleLock.synchronized {
      if (closing) ShuttingDown
      else if (reservations.size >= config.maxSessions) AtCapacity
      else {
        val reservation = new Reserved
        reservations += reservation
        reservation
      }
    }
  }

  private def releaseReservation(reservation: Reserved): IO[Unit] =
    IO(reservation.released.compareAndSet(false, true)).flatMap {
      case true =>
        IO(lifecycleLock.synchronized(reservations -= reservation)).void *>
          reservation.done.complete(()).void
      case false => IO.unit
    }

  private def registerSession(
      session: McpStreamableServerSession,
      reservation: Reserved,
      now: Long
  ): IO[Boolean] = IO {
    lifecycleLock.synchronized {
      if (closing || reservation.released.get()) false
      else {
        val entry      = new SessionEntry(session, reservation, now)
        val registered = sessions.putIfAbsent(session.getId, entry) == null
        if (registered) reservation.entry = Some(entry)
        registered
      }
    }
  }

  private def closeStartedSession(session: McpStreamableServerSession): IO[Unit] =
    IO(session.close()).handleErrorWith(_ => safeCleanupLog)

  private def validateSecurity(request: Request[IO]): IO[Option[Response[IO]]] =
    IO.blocking(config.securityValidator.validateHeaders(headerMap(request))).attempt.flatMap {
      case Right(_)                                      => IO.pure(None)
      case Left(error: ServerTransportSecurityException) =>
        Status.fromInt(error.getStatusCode) match {
          case Right(status) => IO.pure(Some(Response[IO](status).withEntity(errorMessage(error))))
          case Left(_)       =>
            IO.pure(Some(Response[IO](Status.Forbidden).withEntity(errorMessage(error))))
        }
      case Left(_) =>
        IO(logger.error("MCP_TRANSPORT_SECURITY_VALIDATION_FAILED")) *>
          IO.pure(Some(Response[IO](Status.InternalServerError)))
    }

  private def headerMap(request: Request[IO]): java.util.Map[String, java.util.List[String]] = {
    val grouped = mutable.LinkedHashMap.empty[String, (String, mutable.ListBuffer[String])]
    request.headers.headers.foreach { header =>
      val normalized = header.name.toString.toLowerCase(java.util.Locale.ROOT)
      grouped.get(normalized) match {
        case Some((_, values)) => values += header.value
        case None              =>
          grouped += normalized -> (header.name.toString, mutable.ListBuffer(header.value))
      }
    }
    grouped.values.map { case (name, values) => name -> values.toList.asJava }.toMap.asJava
  }

  private def sessionFromRequest(
      request: Request[IO],
      requirePostAccept: Boolean
  ): IO[Either[Response[IO], SessionEntry]] = {
    val errors = List(
      Option.when(requirePostAccept && !hasPostAccept(request))(
        "text/event-stream and application/json required in Accept header"
      ),
      Option.when(!requirePostAccept && !accepts(request, eventStreamMediaType))(
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
        case Some(entry) => IO.pure(Right(entry))
        case None        => sessionNotFound(request, requirePostAccept).map(Left(_))
      }
    }
  }

  private def sessionNotFound(
      request: Request[IO],
      requirePostAccept: Boolean
  ): IO[Response[IO]] =
    if (requirePostAccept)
      errorResponse(
        Status.NotFound,
        McpSchema.ErrorCodes.INTERNAL_ERROR,
        s"Session not found: ${headerValue(request, HttpHeaders.MCP_SESSION_ID).getOrElse("")}"
      )
    else NotFound()

  private def readPostBody(request: Request[IO]): IO[Option[String]] =
    if (request.contentLength.exists(_ > config.requestMaxBytes)) IO.pure(None)
    else
      request.body
        .take(
          if (config.requestMaxBytes == Long.MaxValue) Long.MaxValue else config.requestMaxBytes + 1
        )
        .compile
        .to(Array)
        .map(bytes =>
          Option.when(bytes.length <= config.requestMaxBytes)(
            new String(bytes, StandardCharsets.UTF_8)
          )
        )

  private def badPostAcceptResponse: IO[Response[IO]] =
    errorResponse(
      Status.BadRequest,
      McpSchema.ErrorCodes.METHOD_NOT_FOUND,
      "text/event-stream and application/json required in Accept header"
    )

  private def errorResponse(status: Status, code: Int, text: String): IO[Response[IO]] = {
    val error = McpError.builder(code).message(text).build()
    IO.blocking(jsonMapper.writeValueAsString(error)).map { body =>
      Response[IO](status = status)
        .withEntity(body)
        .withContentType(`Content-Type`(MediaType.application.json))
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

  private val eventStreamMediaType = MediaType.unsafeParse("text/event-stream")

  private def hasPostAccept(request: Request[IO]): Boolean =
    accepts(request, eventStreamMediaType) && accepts(request, MediaType.application.json)

  private def accepts(request: Request[IO], mediaType: MediaType): Boolean =
    request.headers
      .get[Accept]
      .exists(
        _.values
          .exists(value => value.qValue.isAcceptable && value.mediaRange.satisfiedBy(mediaType))
      )

  private def headerValue(request: Request[IO], name: String): Option[String] =
    request.headers.get(CIString(name)).map(_.head.value)

  private def isEndpoint(request: Request[IO]): Boolean =
    request.uri.path.renderString == config.endpoint || request.pathInfo.renderString == config.endpoint

  private def sessionValues: List[McpStreamableServerSession] =
    sessions.values().asScala.filterNot(_.terminating).map(_.session).toList

  private def monotonicNanos: IO[Long] =
    IO.defer(monotonicNanosHook.getOrElse(IO.monotonic.map(_.toNanos)))

  private def cleanupAll(actions: List[IO[Unit]]): IO[Unit] =
    actions.traverse_(_.attempt.flatMap {
      case Left(_)  => safeCleanupLog
      case Right(_) => IO.unit
    })

  private def safeCleanupLog: IO[Unit] = IO(logger.warn("MCP_TRANSPORT_CLEANUP_FAILED"))

  private def errorMessage(error: Throwable): String =
    Option(error.getMessage).getOrElse(error.getClass.getSimpleName)

  private def setTestHooks(
      afterReservation: IO[Unit],
      afterResponseOwnership: IO[Unit],
      afterTermination: IO[Unit],
      monotonicNanos: Option[IO[Long]]
  ): Unit = {
    afterReservationHook = afterReservation
    afterResponseOwnershipHook = afterResponseOwnership
    afterTerminationHook = afterTermination
    monotonicNanosHook = monotonicNanos
  }

  private def runExpirySweepForTest: IO[Unit] = expireIdleSessions
}

object Http4sStreamableServerTransportProvider {
  def apply(
      jsonMapper: McpJsonMapper = McJsonMapperDefault,
      config: Http4sStreamableServerTransportProviderConfig =
        Http4sStreamableServerTransportProviderConfig()
  )(implicit runtime: IORuntime): Http4sStreamableServerTransportProvider =
    new Http4sStreamableServerTransportProvider(jsonMapper, config)

  private val McJsonMapperDefault: McpJsonMapper = McpJsonDefaults.getMapper

  private[http4smcp] def installTestHooks(
      provider: Http4sStreamableServerTransportProvider,
      afterReservation: IO[Unit] = IO.unit,
      afterResponseOwnership: IO[Unit] = IO.unit,
      afterTermination: IO[Unit] = IO.unit,
      monotonicNanos: Option[IO[Long]] = None
  ): Unit =
    provider.setTestHooks(
      afterReservation,
      afterResponseOwnership,
      afterTermination,
      monotonicNanos
    )

  private[http4smcp] def runExpirySweep(
      provider: Http4sStreamableServerTransportProvider
  ): IO[Unit] = provider.runExpirySweepForTest
}
