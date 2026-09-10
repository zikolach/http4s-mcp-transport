# http4s MCP Streamable HTTP transport

This library provides an `http4s` transport provider for the MCP Java SDK Streamable HTTP server API.
It implements `io.modelcontextprotocol.spec.McpStreamableServerTransportProvider` and exposes `HttpRoutes[IO]` for an existing http4s server.

## Dependency selection

The build depends on `mcp-core` and `mcp-json-jackson3` from MCP Java SDK 2.0.1.
It does not depend on the aggregate `mcp` artifact, and it does not pull Jackson2 and Jackson3 mapper modules together by default.

Applications may pass their own `McpJsonMapper` to the provider.

## Configuration

`Http4sStreamableServerTransportProviderConfig` supports:

- `endpoint`: MCP route path. The default is `/mcp`.
- `disallowDelete`: Disables `DELETE` session shutdown when set to `true`.
- `keepAliveInterval`: Optional SDK keepalive ping interval.
- `contextExtractor`: Extracts `McpTransportContext` metadata from an http4s request.
- `requestMaxBytes`: Maximum POST body size. The default is 16 MiB.
- `outboundBufferCapacity`: Maximum queued events for each SSE transport. The default is 256.
- `securityValidator`: Validates all POST, GET, and DELETE headers before body reads or session access. The default is `ServerTransportSecurityValidator.NOOP`.
- `maxSessions`: Maximum pending, active, and terminating sessions per provider. The default is 1,024.
- `sessionIdleTimeout`: Idle session timeout measured with a monotonic clock. The default is 30 minutes.

All limits must be positive. Session admission returns HTTP 503 when `maxSessions` is full. Requests for an expired session return HTTP 404, and clients must initialize a new session.

A validated client request refreshes session activity. Active POST processing and replay prevent expiry. The idle interval restarts when the last active operation ends. Passive GET SSE connections, outbound notifications, server keepalives, malformed requests, and rejected requests do not keep a session active. The expiry worker waits for the smaller of `sessionIdleTimeout` and one second between sweeps, with a minimum wait of one millisecond. Slow session cleanup does not block later sweeps.

Each SSE transport uses a bounded closeable channel. A producer waits when the channel is full. Successful send completion means the event was admitted, not that the client received it. If the channel closes before admission, the send fails with `MCP_TRANSPORT_CLOSED`. Closure releases waiting producers; normal stream completion drains admitted events in order.

## Security validation

The default validator performs no checks. Configure the SDK validator for network-facing servers according to the hosts and origins accepted by the application:

```scala
import io.github.http4smcp.Http4sStreamableServerTransportProviderConfig
import io.modelcontextprotocol.server.transport.DefaultServerTransportSecurityValidator

val validator = DefaultServerTransportSecurityValidator
  .builder()
  .allowedHost("mcp.example.com:443")
  .allowedOrigin("https://app.example.com")
  .build()

val config = Http4sStreamableServerTransportProviderConfig(
  securityValidator = validator
)
```

Authentication and authorization still belong in the consumer's http4s middleware before `provider.routes`.

## Basic usage

Retain the provider instance. Pass that instance to `McpServer.sync` or `McpServer.async`, then mount `provider.routes`:

```scala
import io.github.http4smcp.Http4sStreamableServerTransportProvider
import io.modelcontextprotocol.server.McpServer

val provider = Http4sStreamableServerTransportProvider()
val server = McpServer.sync(provider)
  .serverInfo("example", "1.0.0")
  .build()

val routes = provider.routes
```

The library does not start or own an HTTP server. A client initializes through POST, opens a live stream through GET, sends messages through POST, and ends the session through DELETE.

An HTTP 200 response from GET only returns a lazy response body. The live stream is ready after body consumption reaches SDK listener registration. If `Last-Event-ID` is present, replay finishes before live listener registration. A send made after listener registration is retained in order. The provider does not buffer sends made before registration.

## Migration from 0.1.x

Version 0.2.0 removes the deprecated companion helper and extends the configuration constructor. Replace this code:

```scala
val routes = Http4sStreamableServerTransportProvider.routes()
```

with this code:

```scala
val provider = Http4sStreamableServerTransportProvider()
val routes = provider.routes
```

Recompile source callers that construct `Http4sStreamableServerTransportProviderConfig`. Defaults now enforce finite request, buffering, session, and idle limits.

## Cleanup and logging boundaries

DELETE, idle expiry, and provider shutdown share one termination operation. The provider owns and awaits its POST workers, acceptance subscriptions, SSE transports, GET listener handles, replay subscriptions, and admission reservations. Cleanup still depends on cooperative application code and publishers. The transport cannot force cancellation of uninterruptible application work.

Tests against MCP Java SDK 2.0.1 found that SDK cleanup closes only the latest GET listener, canceled response streams can retain private pending-response state until a matching response arrives, and keepalive shutdown can leave an in-flight ping. The provider separately owns the resources it creates. It does not claim that all private SDK state is cleaned.

Transport-owned diagnostic logs use fixed operation and failure codes. They do not attach request data, session identifiers, exception messages, or exception objects. This guarantee does not cover SDK logs, handler logs, HTTP error text, or SDK-generated JSON-RPC errors. MCP Java SDK 2.0.1 can log raw JSON and other client-controlled values. Applications must configure SDK and application logging for their security requirements.

## Validation

Run all supported Scala versions:

```bash
sbt '+compile; +transport / Test / test; +simpleServer / compile'
```

Check formatting:

```bash
sbt scalafmtCheckAll
```

Run the example server:

```bash
sbt simpleServer/run
```

## Publishing

Publishing to Maven Central is configured through GitHub Actions and sbt-ci-release. See `docs/publishing.md`.

## License

MIT.
