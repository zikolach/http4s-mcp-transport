package io.github.http4smcp

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import cats.syntax.all._
import fs2.Stream
import fs2.concurrent.Channel
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
    channel: Channel[IO, ServerSentEvent]
)(implicit runtime: IORuntime)
    extends McpStreamableServerTransport {

  val events: Stream[IO, ServerSentEvent] = channel.stream.onFinalize(closeIO)

  override def sendMessage(message: McpSchema.JSONRPCMessage): Mono[Void] =
    sendMessage(message, null)

  override def sendMessage(message: McpSchema.JSONRPCMessage, messageId: String): Mono[Void] =
    ReactorInterop.ioUnitToMono {
      IO(jsonMapper.writeValueAsString(message)).flatMap { json =>
        val eventId = Option(messageId).getOrElse(sessionId)
        IO.race(
          channel.send(ServerSentEvent(Some(json), Some("message"), Some(EventId(eventId)))),
          channel.closed
        ).flatMap {
          case Left(Right(_)) => IO.unit
          case _              => IO.raiseError(new IllegalStateException("MCP_TRANSPORT_CLOSED"))
        }
      }
    }

  override def unmarshalFrom[T](data: Object, typeRef: TypeRef[T]): T =
    jsonMapper.convertValue(data, typeRef)

  override def closeGracefully(): Mono[Void] =
    ReactorInterop.ioUnitToMono(closeIO)

  override def close(): Unit =
    closeIO.unsafeRunAndForget()

  private[http4smcp] def closeIO: IO[Unit] = channel.close.void
}

private[http4smcp] object Http4sStreamableServerTransport {
  def create(sessionId: String, jsonMapper: McpJsonMapper, capacity: Int = 256)(implicit
      runtime: IORuntime
  ): IO[Http4sStreamableServerTransport] =
    Channel
      .bounded[IO, ServerSentEvent](capacity)
      .map(new Http4sStreamableServerTransport(sessionId, jsonMapper, _))
}
