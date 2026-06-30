## 1. Repository and build setup

- [ ] 1.1 Create an sbt build for a JVM library cross-built on Scala 2.13 and Scala 3.
- [ ] 1.2 Add explicit dependencies for `mcp-core`, one MCP JSON mapper module, http4s, cats-effect, fs2, Reactor, logging, and test libraries.
- [ ] 1.3 Add formatting, test, and CI commands that run for both Scala versions.
- [ ] 1.4 Add package metadata, license, README, and publishing placeholders.

## 2. Public API

- [ ] 2.1 Define the http4s transport provider class that implements `McpStreamableServerTransportProvider`.
- [ ] 2.2 Expose a route API that can be mounted into an existing http4s server without owning server lifecycle.
- [ ] 2.3 Support configurable endpoint path, delete policy, keep-alive configuration, and transport context extraction.
- [ ] 2.4 Document JSON mapper and dependency selection so Jackson2 and Jackson3 are not pulled together by default.

## 3. Streamable HTTP implementation

- [ ] 3.1 Implement session creation for JSON-RPC `initialize` requests and return `Mcp-Session-Id`.
- [ ] 3.2 Implement session-bound POST handling for JSON-RPC requests, responses, and notifications.
- [ ] 3.3 Implement GET SSE listening streams for server-initiated messages.
- [ ] 3.4 Implement DELETE session shutdown with configurable delete support.
- [ ] 3.5 Implement `notifyClient`, `notifyClients`, `close`, and `closeGracefully` behavior.
- [ ] 3.6 Implement `Last-Event-ID` replay using the SDK session replay API.

## 4. Effect and stream bridging

- [ ] 4.1 Implement a closeable per-stream transport that bridges SDK `sendMessage` calls to fs2 SSE output.
- [ ] 4.2 Add small Reactor-to-cats-effect boundary helpers and document any unavoidable blocking.
- [ ] 4.3 Handle client disconnects, stream completion, and queue cleanup without leaking resources.
- [ ] 4.4 Preserve transport context metadata from http4s requests into SDK calls.

## 5. Validation and examples

- [ ] 5.1 Add route-level tests for initialize, session POST, GET stream, DELETE, invalid JSON, missing session, unknown session, and unsupported methods.
- [ ] 5.2 Add notification tests for one session, all sessions, failed delivery isolation, and graceful shutdown.
- [ ] 5.3 Add replay and disconnect lifecycle tests.
- [ ] 5.4 Add a minimal executable http4s example with one MCP tool.
- [ ] 5.5 Run and document Scala 2.13 and Scala 3 compile and test commands.
