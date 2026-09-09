## ADDED Requirements

### Requirement: Configurable request size limit
The provider SHALL expose a positive `requestMaxBytes` setting with a default value of 16 MiB and SHALL enforce the setting against actual POST body bytes.

#### Scenario: Body at the configured limit is accepted
- **WHEN** a valid MCP POST body contains exactly `requestMaxBytes` bytes
- **THEN** the provider processes the body normally

#### Scenario: Declared body exceeds the configured limit
- **WHEN** a POST request declares a `Content-Length` greater than `requestMaxBytes`
- **THEN** the provider returns HTTP 413 without parsing the body or invoking session behavior

#### Scenario: Streamed body exceeds the configured limit
- **WHEN** a POST body provides more than `requestMaxBytes` bytes regardless of its declared length
- **THEN** the provider consumes no more than the configured limit plus one detection byte and returns HTTP 413

#### Scenario: Request limit is invalid
- **WHEN** application configuration sets `requestMaxBytes` to zero or a negative value
- **THEN** provider configuration fails with a clear validation error

### Requirement: Bounded outbound event buffering
Each SSE transport SHALL use a closeable outbound channel whose queued event count is limited by a positive `outboundBufferCapacity` setting with a default value of 256.

#### Scenario: Capacity is available
- **WHEN** the SDK sends an event and the outbound channel has capacity
- **THEN** `sendMessage` completes after admitting the event
- **AND** the SSE stream emits admitted events in send order

#### Scenario: Capacity is exhausted
- **WHEN** the SDK sends an event while the outbound channel is full
- **THEN** `sendMessage` waits for consumer demand or channel closure
- **AND** the transport does not drop or reorder the event

#### Scenario: Stream closes with blocked producers
- **WHEN** an SSE stream closes while one or more sends are waiting for capacity
- **THEN** channel closure releases the waiting sends
- **AND** no event is admitted after closure
- **AND** no producer fiber remains blocked

#### Scenario: Buffer capacity is invalid
- **WHEN** application configuration sets `outboundBufferCapacity` to zero or a negative value
- **THEN** provider configuration fails with a clear validation error

### Requirement: Demand-aware Reactor streaming
The Reactor-to-FS2 bridge SHALL propagate downstream demand, terminal signals, errors, and cancellation through the Reactive Streams protocol without an unbounded intermediate queue.

#### Scenario: FS2 requests replay elements
- **WHEN** an FS2 consumer pulls elements from a Reactor replay publisher
- **THEN** the bridge requests a bounded amount of upstream demand
- **AND** emits elements in publisher order

#### Scenario: FS2 consumer cancels
- **WHEN** the FS2 stream terminates before its Reactor publisher
- **THEN** the bridge cancels the Reactor subscription
- **AND** releases bridge resources

#### Scenario: Reactor publisher fails
- **WHEN** the Reactor publisher terminates with an error
- **THEN** the FS2 stream fails with that error

### Requirement: SDK request security validation
The provider SHALL expose a `ServerTransportSecurityValidator` setting, default it to `ServerTransportSecurityValidator.NOOP`, and invoke it before processing every supported HTTP request.

#### Scenario: Validator accepts request headers
- **WHEN** the configured validator accepts the complete multi-value request header map
- **THEN** the provider continues normal POST, GET, or DELETE processing

#### Scenario: Validator rejects request headers
- **WHEN** the configured validator raises `ServerTransportSecurityException`
- **THEN** the provider returns the exception HTTP status and message
- **AND** does not read the request body, look up or mutate a session, or invoke SDK request behavior

#### Scenario: Header has multiple values
- **WHEN** a request contains repeated values for one header name
- **THEN** the provider passes every value to the security validator under that header

### Requirement: Structured Accept negotiation
The provider SHALL evaluate `Accept` through http4s media ranges and quality values instead of raw substring matching.

#### Scenario: POST accepts both response types
- **WHEN** a POST request accepts `application/json` and `text/event-stream` through media types or matching wildcards with nonzero quality
- **THEN** the provider continues normal POST processing

#### Scenario: Required POST response type has zero quality
- **WHEN** a POST request assigns quality zero to either `application/json` or `text/event-stream` without another acceptable matching range
- **THEN** the provider returns the existing HTTP 400 content-negotiation error

#### Scenario: GET does not accept event streams
- **WHEN** a GET request has no nonzero-quality media range matching `text/event-stream`
- **THEN** the provider returns the existing HTTP 400 content-negotiation error

#### Scenario: Header contains a media-type substring only
- **WHEN** an `Accept` value contains text such as `application/jsonish` that does not parse as a matching media type
- **THEN** the value does not satisfy the required media type

### Requirement: Typed Reactor completion interop
Reactor interop SHALL distinguish a `Mono` that must emit one value from a `Mono` used only for terminal completion, while preserving cancellation in both directions.

#### Scenario: Required value is emitted
- **WHEN** a value-producing `Mono` emits one value
- **THEN** `monoToIO` completes with that value

#### Scenario: Required value completes empty
- **WHEN** a value-producing `Mono` completes without a value
- **THEN** `monoToIO` fails instead of returning `null`

#### Scenario: Completion-only Mono completes empty
- **WHEN** a completion-only `Mono` completes without a value
- **THEN** its IO conversion completes successfully with `Unit`

#### Scenario: Either side cancels
- **WHEN** the IO fiber or Reactor subscriber cancels the conversion
- **THEN** the corresponding upstream computation is canceled once

### Requirement: Provider lifecycle remains accessible
Applications SHALL retain the provider instance used to create the MCP server and mount its `routes` value.

#### Scenario: Application creates routes
- **WHEN** an application constructs `Http4sStreamableServerTransportProvider`
- **THEN** it can pass the same provider to `McpServer.sync` or `McpServer.async`
- **AND** mount `provider.routes` in its http4s server

#### Scenario: Companion routes helper is unavailable
- **WHEN** application code migrates to version 0.2.0
- **THEN** `Http4sStreamableServerTransportProvider.routes` is no longer part of the public API
- **AND** migration documentation directs the application to retain the provider instance
