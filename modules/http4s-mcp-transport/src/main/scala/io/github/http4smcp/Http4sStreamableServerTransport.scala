package io.github.http4smcp

import cats.effect.IO
import cats.effect.Ref
import cats.effect.unsafe.IORuntime
import fs2.Stream
import cats.effect.std.Queue
import io.github.http4smcp.internal.ReactorInterop
import io.modelcontextprotocol.json.McpJsonMapper
import io.modelcontextprotocol.json.TypeRef
import io.modelcontextprotocol.spec.McpSchema
import io.modelcontextprotocol.spec.McpStreamableServerTransport
import org.http4s.ServerSentEvent
import org.http4s.ServerSentEvent.EventId
import reactor.core.publisher.Mono

private[http4smcp] final class Http4sStreamableServerTransport private (
    sessionId: String,
    jsonMapper: McpJsonMapper,
    queue: Queue[IO, Option[ServerSentEvent]],
    closed: Ref[IO, Boolean]
)(implicit runtime: IORuntime)
    extends McpStreamableServerTransport {

  val events: Stream[IO, ServerSentEvent] =
    Stream
      .repeatEval(queue.take)
      .unNoneTerminate
      .onFinalize(closeIO)

  override def sendMessage(message: McpSchema.JSONRPCMessage): Mono[Void] =
    sendMessage(message, null)

  override def sendMessage(message: McpSchema.JSONRPCMessage, messageId: String): Mono[Void] =
    ReactorInterop.ioUnitToMono {
      closed.get.flatMap {
        case true  => IO.unit
        case false =>
          IO(jsonMapper.writeValueAsString(message)).flatMap { json =>
            val eventId = Option(messageId).getOrElse(sessionId)
            queue.offer(Some(ServerSentEvent(Some(json), Some("message"), Some(EventId(eventId)))))
          }
      }
    }

  override def unmarshalFrom[T](data: Object, typeRef: TypeRef[T]): T =
    jsonMapper.convertValue(data, typeRef)

  override def closeGracefully(): Mono[Void] =
    ReactorInterop.ioUnitToMono(closeIO)

  override def close(): Unit =
    closeIO.unsafeRunAndForget()

  private def closeIO: IO[Unit] =
    closed.modify {
      case true  => true -> IO.unit
      case false => true -> queue.offer(None)
    }.flatten
}

private[http4smcp] object Http4sStreamableServerTransport {
  def create(sessionId: String, jsonMapper: McpJsonMapper)(implicit
      runtime: IORuntime
  ): IO[Http4sStreamableServerTransport] =
    for {
      queue  <- Queue.unbounded[IO, Option[ServerSentEvent]]
      closed <- Ref.of[IO, Boolean](false)
    } yield new Http4sStreamableServerTransport(sessionId, jsonMapper, queue, closed)
}
