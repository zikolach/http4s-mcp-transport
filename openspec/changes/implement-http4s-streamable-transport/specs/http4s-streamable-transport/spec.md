## ADDED Requirements

### Requirement: Java SDK transport provider compatibility
The library SHALL provide a JVM implementation of `io.modelcontextprotocol.spec.McpStreamableServerTransportProvider` for the MCP Java SDK.

#### Scenario: MCP server accepts the provider
- **WHEN** application code passes the provider to `McpServer.sync(provider)` or `McpServer.async(provider)`
- **THEN** the provider accepts the SDK session factory through `setSessionFactory`
- **AND** the provider uses that factory to start Streamable HTTP sessions

#### Scenario: Provider exposes supported protocol versions
- **WHEN** the SDK calls `protocolVersions()`
- **THEN** the provider returns the SDK-supported Streamable HTTP protocol versions without hardcoded divergence from the selected SDK version

### Requirement: http4s route integration
The library SHALL expose http4s routes for a configurable MCP endpoint without starting or owning an HTTP server.

#### Scenario: Routes are mounted into an existing server
- **WHEN** a user creates the provider with endpoint `/mcp`
- **THEN** the provider exposes `HttpRoutes` that can be mounted in an existing http4s server
- **AND** the routes handle `POST /mcp`, `GET /mcp`, and `DELETE /mcp`

#### Scenario: Endpoint is configurable
- **WHEN** a user creates the provider with a non-default endpoint
- **THEN** the routes handle MCP requests only at that configured endpoint

### Requirement: Streamable HTTP request handling
The provider SHALL implement MCP Streamable HTTP behavior for initialization, client messages, server-to-client streams, and session deletion.

#### Scenario: Initialize opens a session
- **WHEN** a `POST` request contains a JSON-RPC `initialize` request without an MCP session header
- **THEN** the provider starts a new SDK session
- **AND** the response includes the `Mcp-Session-Id` header
- **AND** the response body contains the JSON-RPC initialize result

#### Scenario: Session message is accepted
- **WHEN** a `POST` request contains a JSON-RPC response or notification with a valid `Mcp-Session-Id` header
- **THEN** the provider forwards the message to the matching SDK session
- **AND** the HTTP response indicates accepted processing without opening an unnecessary response stream

#### Scenario: Request receives stream response
- **WHEN** a `POST` request contains a JSON-RPC request with a valid `Mcp-Session-Id` header
- **THEN** the provider opens an SSE response stream for that request
- **AND** the stream emits SDK response messages as `message` events

#### Scenario: GET opens listening stream
- **WHEN** a `GET` request contains a valid `Mcp-Session-Id` header and accepts `text/event-stream`
- **THEN** the provider opens an SSE stream for server-initiated messages for that session

#### Scenario: DELETE closes session
- **WHEN** a `DELETE` request contains a valid `Mcp-Session-Id` header and deletion is enabled
- **THEN** the provider deletes the SDK session
- **AND** the session is removed from the provider session store

### Requirement: HTTP validation and error responses
The provider SHALL validate method, content negotiation, session headers, and malformed JSON before invoking SDK session behavior.

#### Scenario: Missing session header is rejected
- **WHEN** a non-initialize request does not include `Mcp-Session-Id`
- **THEN** the provider returns an HTTP client error response
- **AND** the provider does not call a session handler

#### Scenario: Unknown session is rejected
- **WHEN** a request includes an unknown `Mcp-Session-Id`
- **THEN** the provider returns an HTTP not found response

#### Scenario: Unsupported method is rejected
- **WHEN** a request uses an HTTP method other than `POST`, `GET`, or `DELETE` at the MCP endpoint
- **THEN** the provider returns an HTTP method-not-allowed response

#### Scenario: Malformed JSON-RPC is rejected
- **WHEN** a `POST` request body cannot be decoded as an MCP JSON-RPC message
- **THEN** the provider returns an HTTP client error response
- **AND** the provider does not create or mutate a session

### Requirement: SSE lifecycle and replay
The provider SHALL bridge SDK stream transports to fs2-backed Server-Sent Events with correct close and replay behavior.

#### Scenario: Server notification reaches listening stream
- **WHEN** `notifyClient(sessionId, method, params)` is called for a session with an active GET stream
- **THEN** the provider sends a `message` SSE event to that stream

#### Scenario: Broadcast reaches all active sessions
- **WHEN** `notifyClients(method, params)` is called
- **THEN** the provider attempts delivery to every active session
- **AND** one failed session does not prevent delivery to other sessions

#### Scenario: Last event id is replayed
- **WHEN** a `GET` request includes `Last-Event-ID`
- **THEN** the provider asks the SDK session to replay missed messages before attaching the live listening stream

#### Scenario: Client disconnect closes stream transport
- **WHEN** an SSE client disconnects
- **THEN** the provider closes the corresponding stream transport
- **AND** the SDK session remains available unless the session itself is deleted

### Requirement: Effect and dependency integration
The provider SHALL integrate Reactor, cats-effect, fs2, and the MCP Java SDK through explicit boundaries.

#### Scenario: Reactor results are bridged safely
- **WHEN** SDK methods return `reactor.core.publisher.Mono`
- **THEN** the provider converts or evaluates those effects at the transport boundary
- **AND** the provider documents any unavoidable blocking boundary in code and tests

#### Scenario: JSON mapper is explicit
- **WHEN** a user creates the provider
- **THEN** the user can supply the SDK `McpJsonMapper`
- **AND** the library does not pull both Jackson2 and Jackson3 mapper modules transitively by default

### Requirement: Scala cross-build support
The library SHALL cross-compile for Scala 2.13 and Scala 3 on the JVM.

#### Scenario: Scala 2.13 build succeeds
- **WHEN** CI runs the Scala 2.13 build
- **THEN** the main library, tests, and example compile successfully

#### Scenario: Scala 3 build succeeds
- **WHEN** CI runs the Scala 3 build
- **THEN** the main library, tests, and example compile successfully

#### Scenario: Public API is source-compatible
- **WHEN** a user imports the provider from Scala 2.13 or Scala 3
- **THEN** the same documented constructor and route API are available in both versions

### Requirement: Examples and validation
The project SHALL include executable examples and automated tests for the transport behavior.

#### Scenario: Minimal example starts an MCP server
- **WHEN** a user runs the minimal example
- **THEN** it starts an http4s server with the MCP route mounted
- **AND** it exposes at least one simple MCP tool through the Java SDK

#### Scenario: Tests cover the protocol lifecycle
- **WHEN** the test suite runs
- **THEN** it verifies initialize, session request, notification, GET stream, DELETE, invalid input, shutdown, and cross-build compilation behavior
