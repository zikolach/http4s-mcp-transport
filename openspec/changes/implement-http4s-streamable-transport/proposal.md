## Why

The MCP Java SDK provides a servlet Streamable HTTP server transport, but Scala services that use http4s need a Scala-native adapter to avoid embedding a servlet container only for MCP. A separate reusable transport project lets Scala applications integrate the Java SDK while keeping their existing http4s server stack.

## What Changes

- Add a reusable http4s implementation of `io.modelcontextprotocol.spec.McpStreamableServerTransportProvider` for the MCP Java SDK.
- Expose http4s `HttpRoutes` for the Streamable HTTP MCP endpoint.
- Support `POST`, `GET`, and `DELETE` request handling for stateful Streamable HTTP sessions.
- Bridge Java SDK Reactor APIs to cats-effect and fs2 without leaking blocking behavior into user code.
- Cross-compile the library for Scala 2.13 and Scala 3.
- Provide tests and examples that demonstrate SDK initialization, session handling, request/response streaming, server notifications, and graceful shutdown.
- Avoid Tapir in the core transport. Tapir support may be added later as a thin optional wrapper if it has clear value.

## Capabilities

### New Capabilities
- `http4s-streamable-transport`: Reusable Scala/http4s Streamable HTTP server transport for the MCP Java SDK.

### Modified Capabilities

None.

## Impact

- New standalone repository at `/Users/nikolay.kushin/Projects/http4s-mcp-transport`.
- New build definition for a Scala 2.13 and Scala 3 cross-built JVM library.
- New public API centered on an http4s transport provider that implements `McpStreamableServerTransportProvider`.
- Dependencies include MCP Java SDK `mcp-core`, one explicit MCP JSON mapper module, http4s, cats-effect, fs2, Reactor, and test libraries.
- No changes to existing Prewave application repositories are part of this change.
