package io.github.http4smcp.example

import cats.effect.IO
import cats.effect.IOApp
import cats.effect.unsafe.implicits.global
import com.comcast.ip4s._
import io.github.http4smcp.Http4sStreamableServerTransportProvider
import io.modelcontextprotocol.server.McpServer
import io.modelcontextprotocol.server.McpSyncServerExchange
import io.modelcontextprotocol.spec.McpSchema
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.syntax.all._

object SimpleServer extends IOApp.Simple {
  override val run: IO[Unit] = {
    val provider = Http4sStreamableServerTransportProvider()

    McpServer
      .sync(provider)
      .serverInfo("http4s-mcp-simple-server", "0.1.0")
      .toolCall(
        McpSchema.Tool
          .builder(
            "echo",
            java.util.Map.of[String, Object](
              "type",
              "object",
              "properties",
              java.util.Map.of[String, Object](
                "message",
                java.util.Map.of[String, Object]("type", "string")
              )
            )
          )
          .description("Echoes the provided message.")
          .build(),
        (_: McpSyncServerExchange, request: McpSchema.CallToolRequest) => {
          val message = Option(request.arguments())
            .flatMap(args => Option(args.get("message")))
            .map(_.toString)
            .getOrElse("")
          McpSchema.CallToolResult.builder().addTextContent(message).build()
        }
      )
      .build()

    EmberServerBuilder
      .default[IO]
      .withHost(host"0.0.0.0")
      .withPort(port"8080")
      .withHttpApp(provider.routes.orNotFound)
      .build
      .useForever
  }
}
