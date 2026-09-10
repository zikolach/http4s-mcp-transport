package io.github.http4smcp

import cats.effect.IO
import io.modelcontextprotocol.common.McpTransportContext
import io.modelcontextprotocol.server.McpTransportContextExtractor
import io.modelcontextprotocol.server.transport.ServerTransportSecurityValidator
import org.http4s.Request

import java.time.Duration

final case class Http4sStreamableServerTransportProviderConfig(
    endpoint: String = "/mcp",
    disallowDelete: Boolean = false,
    keepAliveInterval: Option[Duration] = None,
    contextExtractor: McpTransportContextExtractor[Request[IO]] = _ => McpTransportContext.EMPTY,
    requestMaxBytes: Long = 16L * 1024 * 1024,
    outboundBufferCapacity: Int = 256,
    securityValidator: ServerTransportSecurityValidator = ServerTransportSecurityValidator.NOOP,
    maxSessions: Int = 1024,
    sessionIdleTimeout: Duration = Duration.ofMinutes(30)
) {
  require(endpoint.startsWith("/"), "endpoint must start with /")
  require(endpoint.length == 1 || !endpoint.endsWith("/"), "endpoint must not end with /")
  require(requestMaxBytes > 0, "requestMaxBytes must be positive")
  require(outboundBufferCapacity > 0, "outboundBufferCapacity must be positive")
  require(securityValidator != null, "securityValidator must not be null")
  require(maxSessions > 0, "maxSessions must be positive")
  require(
    sessionIdleTimeout != null && !sessionIdleTimeout.isZero && !sessionIdleTimeout.isNegative,
    "sessionIdleTimeout must be positive"
  )
  require(
    scala.util.Try(sessionIdleTimeout.toNanos).isSuccess,
    "sessionIdleTimeout must be finite"
  )
}
