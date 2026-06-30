package io.github.http4smcp

import cats.effect.IO
import io.modelcontextprotocol.common.McpTransportContext
import io.modelcontextprotocol.server.McpTransportContextExtractor
import org.http4s.Request

import java.time.Duration

final case class Http4sStreamableServerTransportProviderConfig(
    endpoint: String = "/mcp",
    disallowDelete: Boolean = false,
    keepAliveInterval: Option[Duration] = None,
    contextExtractor: McpTransportContextExtractor[Request[IO]] = _ => McpTransportContext.EMPTY
) {
  require(endpoint.startsWith("/"), "endpoint must start with /")
  require(endpoint.length == 1 || !endpoint.endsWith("/"), "endpoint must not end with /")
}
