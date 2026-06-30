# http4s MCP Streamable HTTP transport

This library provides an `http4s` transport provider for the MCP Java SDK Streamable HTTP server API.
It implements `io.modelcontextprotocol.spec.McpStreamableServerTransportProvider` and exposes `HttpRoutes[IO]` that can be mounted into an existing http4s server.

## Dependency selection

The build depends on `mcp-core` and `mcp-json-jackson3` from the MCP Java SDK.
It does not depend on the aggregate `mcp` artifact, and it does not pull Jackson2 and Jackson3 mapper modules together by default.

Applications may pass their own `McpJsonMapper` to the provider if they need a different mapper.

## Configuration

`Http4sStreamableServerTransportProviderConfig` supports:

- `endpoint`: MCP route path, default `/mcp`.
- `disallowDelete`: disables `DELETE` session shutdown when `true`.
- `keepAliveInterval`: optional SDK keep-alive ping interval.
- `contextExtractor`: extracts `McpTransportContext` metadata from an http4s request.

The Reactor-to-cats-effect bridge is isolated in `ReactorInterop`. It subscribes to SDK `Mono` and `Flux` values without calling `block()`. JSON mapping is synchronous in the SDK mapper and is wrapped in `IO.blocking` at the HTTP boundary.

## Basic usage

```scala
import io.github.http4smcp.Http4sStreamableServerTransportProvider
import io.modelcontextprotocol.json.McpJsonDefaults

val provider = Http4sStreamableServerTransportProvider(
  jsonMapper = McpJsonDefaults.getMapper()
)

val routes = provider.routes
```

Mount `routes` into an existing http4s server. The core transport does not start or own an HTTP server.

## Validation

Run all supported Scala versions:

```bash
sbt "+compile; +transport / Test / testOnly io.github.http4smcp.Http4sStreamableServerTransportProviderSuite; simpleServer/compile"
```

Check formatting:

```bash
sbt "scalafmtCheckAll"
```

## Publishing

Publishing to Maven Central is configured through GitHub Actions and sbt-ci-release. See `docs/publishing.md`.

## License

MIT.
