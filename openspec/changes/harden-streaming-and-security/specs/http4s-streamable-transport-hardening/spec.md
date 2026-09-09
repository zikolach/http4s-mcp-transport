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

### Requirement: Finite session admission
The provider SHALL expose a positive finite `maxSessions` setting with a default of 1,024 sessions per provider. Pending initializations, registered sessions, and sessions still releasing provider-owned resources SHALL count against the same limit.

#### Scenario: Concurrent initialization reaches capacity
- **WHEN** concurrent valid initialization requests would exceed `maxSessions`
- **THEN** the provider atomically reserves no more than `maxSessions` slots
- **AND** returns HTTP 503 for excess requests without calling the session factory or waiting for admission capacity
- **AND** does not evict existing sessions to admit new ones

#### Scenario: Initialization registers successfully
- **WHEN** a reserved initialization completes and registers its session
- **THEN** the same reservation becomes the registered session's capacity slot without a release-and-reacquire gap

#### Scenario: Initialization fails or is canceled
- **WHEN** initialization fails or is canceled after reserving capacity
- **THEN** the provider closes any acquired session and releases provider-owned resources
- **AND** releases the reservation exactly once

#### Scenario: Shutdown races initialization
- **WHEN** shutdown begins while an initialization is pending
- **THEN** the initialization cannot register a usable session after shutdown admission closes
- **AND** its acquired provider resources and reservation are released

#### Scenario: Session capacity is invalid
- **WHEN** configuration sets `maxSessions` to zero or a negative value
- **THEN** configuration fails rather than interpreting the value as unlimited

### Requirement: Idle session expiry
The provider SHALL expose a positive finite `sessionIdleTimeout` with a default of 30 minutes and enforce it using a monotonic clock. Validated requests admitted to a session SHALL count as client activity. Active request processing SHALL prevent idle expiry; completion of the last active operation SHALL start a fresh idle interval. Passive SSE connections and server-generated traffic SHALL NOT prevent idle expiry.

#### Scenario: Session becomes idle
- **WHEN** a session has no active processing and its idle interval reaches `sessionIdleTimeout`
- **THEN** new requests using that session receive HTTP 404
- **AND** the provider begins termination within one documented expiry sweep interval without requiring another client request

#### Scenario: Client activity refreshes idle time
- **WHEN** a validated request is admitted before the session expires
- **THEN** it refreshes client activity
- **AND** rejected requests, malformed bodies, outbound notifications, and server keepalive traffic do not refresh activity

#### Scenario: Long-running request or replay is active
- **WHEN** an admitted request or replay is still processing
- **THEN** idle expiry does not terminate that session
- **AND** the idle interval begins anew when the last active operation completes or is canceled

#### Scenario: Only a passive SSE connection remains
- **WHEN** live GET SSE remains connected without active processing or new client requests for `sessionIdleTimeout`
- **THEN** the provider expires the session and closes its provider-owned SSE resources

#### Scenario: Expiry races a request
- **WHEN** activity admission races the expiry decision
- **THEN** one atomic lifecycle decision either admits the operation or retires the session
- **AND** no operation acquires an already retired session

#### Scenario: Idle timeout is invalid
- **WHEN** configuration supplies a nonpositive or non-finite idle timeout
- **THEN** configuration fails rather than disabling expiry

### Requirement: Shared session termination
DELETE, expiry, and provider shutdown SHALL share idempotent per-session termination. Termination SHALL prevent new resource acquisition, release all provider-owned session resources, invoke SDK session cleanup, and release admission capacity exactly once. A caller canceling its wait SHALL NOT cancel the shared cleanup.

#### Scenario: Termination paths race
- **WHEN** DELETE, expiry, and shutdown target the same session concurrently
- **THEN** the provider runs one shared termination operation for that session
- **AND** does not double-release its capacity reservation

#### Scenario: Resource cleanup fails
- **WHEN** closing one owned resource fails
- **THEN** the provider still attempts cleanup of every other owned resource
- **AND** reports the failure through the transport-owned safe logging policy

#### Scenario: Provider shuts down
- **WHEN** provider shutdown completes
- **THEN** its expiry worker has stopped
- **AND** its provider-owned session resources and admission reservations have been released
- **AND** it admits no new sessions or stream resources

### Requirement: Provider-owned response resources
The provider SHALL own every transport, POST processing worker, GET listener handle, and replay subscription that it acquires. Ownership SHALL be registered atomically against session termination. This guarantee SHALL NOT claim cleanup of inaccessible SDK-internal state or forced termination of uninterruptible application code.

#### Scenario: Response body is never consumed
- **WHEN** the provider returns a response whose body is never consumed
- **THEN** no worker, transport, listener, replay subscription, or active-operation lease remains allocated solely for that body

#### Scenario: Response body starts after session termination
- **WHEN** a previously returned response body begins consumption after its session has terminated
- **THEN** it does not start SDK processing or acquire new live resources for that session

#### Scenario: Consumer disconnects or body processing fails
- **WHEN** an SSE consumer cancels or its response body fails during replay or live processing
- **THEN** the provider cancels processing and replay for that response and releases its acquired listener and transport
- **AND** it awaits provider finalizers and releases blocked sends without requiring further client consumption

#### Scenario: Session owns multiple responses
- **WHEN** a session terminates with multiple GET listeners, a replay subscription, and POST response workers
- **THEN** all provider-owned responses terminate, including older GET listeners no longer referenced by the SDK
- **AND** cleanup does not depend solely on `McpStreamableServerSession.closeGracefully`

#### Scenario: Acquisition races termination
- **WHEN** acquiring a transport, listener, replay subscription, or worker races session termination
- **THEN** the resource is either registered for termination or released by the acquiring operation
- **AND** no acquired resource becomes unowned

### Requirement: Payload-free transport diagnostic logs
Transport-owned diagnostic logs SHALL use fixed operation and failure codes without original throwable objects, exception messages, causes, suppressed exceptions, payloads, headers, parameters, raw session identifiers, or client-controlled strings. This requirement SHALL cover every enabled transport log level and SHALL NOT claim to sanitize SDK logs, application logs, HTTP error text, or SDK-generated JSON-RPC errors.

#### Scenario: Processing or cleanup exception contains a secret
- **WHEN** initialization, request processing, notification delivery, response acceptance, or cleanup fails with secret markers in exception messages, causes, or suppressed exceptions
- **THEN** a transport-owned event identifies the operation and failure using fixed codes
- **AND** neither its formatted output, argument data, nor throwable data contains those markers

#### Scenario: Request-controlled values contain a secret
- **WHEN** payloads, parameters, headers, or session identifiers contain secret markers
- **THEN** transport-owned diagnostic logs do not include those values

#### Scenario: Application enables SDK logging
- **WHEN** the application configures SDK or handler loggers
- **THEN** the transport does not modify global logger levels or install application-wide filters
- **AND** documentation identifies SDK-originated payload logging as outside the transport-owned guarantee

### Requirement: Explicit upstream capability coverage
Before accepting session and lifecycle hardening, verification SHALL identify the MCP SDK version, inspect its session admission, expiry, and cleanup behavior, and run focused lifecycle tests through the http4s provider. Source inspection alone SHALL NOT establish implementation acceptance.

#### Scenario: SDK lacks admission or expiry controls
- **WHEN** the selected SDK does not enforce session caps or idle expiry for this provider
- **THEN** the provider owns both policies
- **AND** tests demonstrate concurrent capacity enforcement, reservation release, and idle expiry through its routes

#### Scenario: SDK cleanup is incomplete
- **WHEN** supported SDK cleanup or cancellation leaves internal response-stream state or in-flight keepalive work
- **THEN** verification records the SDK version, source locations, and observed limitation
- **AND** separately verifies the provider-owned cleanup guarantee
- **AND** does not use a fork, reflection, or global logging changes to conceal the limitation

#### Scenario: SDK limitations are documented
- **WHEN** version 0.2.0 is prepared for release
- **THEN** documentation states the tested provider-owned guarantee and residual SDK cleanup and logging limitations
- **AND** it does not claim comprehensive SDK cleanup or end-to-end payload-free logging
