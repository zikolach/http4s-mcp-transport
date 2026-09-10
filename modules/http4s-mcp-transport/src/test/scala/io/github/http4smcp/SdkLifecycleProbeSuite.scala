package io.github.http4smcp

import cats.effect.IO
import cats.effect.std.Semaphore
import io.github.http4smcp.internal.ReactorInterop
import io.modelcontextprotocol.json.McpJsonDefaults
import io.modelcontextprotocol.json.TypeRef
import io.modelcontextprotocol.server.McpAsyncServerExchange
import io.modelcontextprotocol.server.McpRequestHandler
import io.modelcontextprotocol.spec.McpSchema
import io.modelcontextprotocol.spec.McpSession
import io.modelcontextprotocol.spec.McpStreamableServerSession
import io.modelcontextprotocol.spec.McpStreamableServerTransport
import io.modelcontextprotocol.util.KeepAliveScheduler
import munit.CatsEffectSuite
import reactor.core.publisher.Mono

import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

final class SdkLifecycleProbeSuite extends CatsEffectSuite {
  private val mapper = McpJsonDefaults.getMapper()

  test("SDK 2.0.1 closeGracefully closes only the latest listening stream") {
    val first   = new ProbeTransport
    val second  = new ProbeTransport
    val session = testSession((_, _) => Mono.just(java.util.Map.of[String, Object]()))

    for {
      _ <- IO(session.listeningStream(first))
      _ <- IO(session.listeningStream(second))
      _ <- ReactorInterop.monoCompletionToIO(session.closeGracefully())
    } yield {
      assertEquals(first.closeCount.get(), 0)
      assertEquals(second.closeCount.get(), 1)
    }
  }

  test("SDK 2.0.1 responseStream cancellation leaves its pending client request until response") {
    val transport = new ProbeTransport
    val requestId = new AtomicReference[String]()
    val session   = testSession((exchange, _) => exchange.ping())
    val request   = new McpSchema.JSONRPCRequest("probe", "probe-request", java.util.Map.of())

    for {
      sent <- Semaphore[IO](0)
      _    <- IO {
        transport.onRequest = sdkRequest => {
          requestId.set(sdkRequest.id().toString)
          sent.release.unsafeRunAndForget()
        }
      }
      fiber <- ReactorInterop.monoCompletionToIO(session.responseStream(request, transport)).start
      _     <- sent.acquire.timeout(2.seconds)
      _     <- fiber.cancel
      accepted <- ReactorInterop
        .monoCompletionToIO(
          session.accept(
            McpSchema.JSONRPCResponse.result(requestId.get(), java.util.Map.of[String, Object]())
          )
        )
        .attempt
    } yield assert(accepted.isRight)
  }

  test("SDK 2.0.1 KeepAliveScheduler shutdown does not cancel an in-flight ping") {
    val subscribed = new java.util.concurrent.CountDownLatch(1)
    val canceled   = new AtomicBoolean(false)
    val session    = new McpSession {
      override def sendRequest[T](method: String, params: Object, typeRef: TypeRef[T]): Mono[T] =
        Mono
          .never[T]()
          .doOnSubscribe(_ => subscribed.countDown())
          .doOnCancel(() => canceled.set(true))
      override def sendNotification(method: String, params: Object): Mono[Void] = Mono.empty()
      override def closeGracefully(): Mono[Void]                                = Mono.empty()
      override def close(): Unit                                                = ()
    }
    val scheduler = KeepAliveScheduler
      .builder(() => reactor.core.publisher.Flux.just(session))
      .initialDelay(Duration.ofMillis(1))
      .interval(Duration.ofHours(1))
      .build()

    for {
      _ <- IO(scheduler.start()).void
      _ <- IO.blocking(assert(subscribed.await(2, java.util.concurrent.TimeUnit.SECONDS)))
      _ <- IO(scheduler.shutdown())
    } yield assert(!canceled.get())
  }

  private def testSession(
      handler: (McpAsyncServerExchange, Object) => Mono[Object]
  ): McpStreamableServerSession =
    new McpStreamableServerSession(
      "sdk-probe",
      McpSchema.ClientCapabilities.builder().build(),
      McpSchema.Implementation.builder("probe-client", "1").build(),
      Duration.ofSeconds(30),
      Map[String, McpRequestHandler[_]](
        "probe" -> new McpRequestHandler[Object] {
          override def handle(exchange: McpAsyncServerExchange, params: Object): Mono[Object] =
            handler(exchange, params)
        }
      ).asJava,
      Map.empty[String, io.modelcontextprotocol.server.McpNotificationHandler].asJava,
      () => Mono.empty[Void]()
    )

  private final class ProbeTransport extends McpStreamableServerTransport {
    val closeCount                                  = new AtomicInteger(0)
    var onRequest: McpSchema.JSONRPCRequest => Unit = _ => ()

    override def sendMessage(message: McpSchema.JSONRPCMessage): Mono[Void] =
      sendMessage(message, null)

    override def sendMessage(message: McpSchema.JSONRPCMessage, messageId: String): Mono[Void] = {
      message match {
        case request: McpSchema.JSONRPCRequest => onRequest(request)
        case _                                 => ()
      }
      Mono.empty()
    }

    override def unmarshalFrom[T](data: Object, typeRef: TypeRef[T]): T =
      mapper.convertValue(data, typeRef)

    override def closeGracefully(): Mono[Void] =
      Mono.fromRunnable[Void](() => closeCount.incrementAndGet()).`then`()

    override def close(): Unit = {
      closeCount.incrementAndGet()
      ()
    }
  }
}
