## Context

Version 0.1.1 bounds POST bodies with a private 16 MiB limit and fixes session shutdown ordering. Outbound SSE transports and the Reactor `Flux` bridge still use unbounded queues, request security validation cannot be configured, and content negotiation reads raw header strings. The public configuration is a case class, so adding fields changes its binary constructor and belongs in 0.2.0.

The provider also retains sessions in an uncapped `ConcurrentHashMap` without idle expiry. POST response processing starts before HTTP response-body consumption, while its cancellation finalizer belongs to that body. GET cleanup is attached to the live stream after replay. These paths need explicit ownership through cancellation, abandoned responses, and session termination.

The MCP Java SDK servlet provider is the reference for request limits and `ServerTransportSecurityValidator` integration, not evidence of complete resource protection. http4s and FS2 remain responsible for HTTP parsing and stream lifecycle.

Inspection of the declared MCP SDK `2.0.1` source artifact found these relevant boundaries:

- `HttpServletStreamableServerTransportProvider` keeps an uncapped session map with no idle-expiry policy. Its retention behavior must not substitute for provider admission and expiry tests.
- `McpStreamableServerSession.listeningStream` replaces the current listener reference without closing the previous listener. `closeGracefully` closes only the current listener and includes a TODO to close all streams.
- `McpStreamableServerSession.responseStream` creates an internal stream that the provider cannot directly close through a returned handle. Cancellation must be tested before claiming that pending SDK requests or stream mappings are released.
- `McpSchema.deserializeJsonRpcMessage` logs raw JSON at DEBUG. `McpStreamableServerSession.accept` can log an unknown notification at WARN. `KeepAliveScheduler` logs exception messages and starts ping subscriptions whose cancellation needs separate verification.

These are source findings, not runtime acceptance evidence. Recheck the SDK version used for implementation and record source locations and focused test results.

## Goals / Non-Goals

**Goals:**

- Bound queued outbound events without dropping or reordering messages.
- Propagate FS2 demand and cancellation to Reactor publishers.
- Make request size and queue capacity configurable with safe defaults.
- Enable finite session admission limits and idle expiry by default.
- Release every provider-owned stream resource when its response or session terminates.
- Prevent transport-owned diagnostic logs from including payloads or original exceptions.
- Allow applications to use the SDK request-header security validator.
- Apply HTTP media-range and quality semantics to `Accept` validation.
- Remove nullable generic completion from Reactor interop.
- Remove the deprecated companion `routes` helper with a documented migration.

**Non-Goals:**

- Change MCP JSON-RPC handling beyond session admission, expiry, and resource cleanup.
- Guarantee cleanup of inaccessible SDK-internal state or payload-free SDK and application logs.
- Fork the SDK, use reflection into SDK internals, or modify application-wide logging configuration.
- Add per-client quotas, stream-count limits, request execution deadlines, or a total process-memory budget.
- Sanitize SDK JSON-RPC error payloads or change HTTP exception-text responses as part of the logging requirement.
- Add a new HTTP server backend or own server startup.
- Add persistence for sessions or replay history.
- Drop Scala 2.13 support.
- Add message dropping as an overload strategy.

## Decisions

### Extend provider configuration in 0.2.0

Add these settings to `Http4sStreamableServerTransportProviderConfig`:

- `requestMaxBytes: Long = 16L * 1024 * 1024`
- `outboundBufferCapacity: Int = 256`
- `securityValidator: ServerTransportSecurityValidator = ServerTransportSecurityValidator.NOOP`
- `maxSessions: Int = 1024`
- `sessionIdleTimeout: java.time.Duration = java.time.Duration.ofMinutes(30)`

All limit settings must be positive. Session admission and expiry are enabled by default; zero, negative, or unlimited sentinel values are not supported. The request default matches the MCP Java SDK servlet provider. A capacity of 256 permits short bursts while placing a fixed bound on queued event count.

Q3 is resolved with O3-1: 1,024 sessions per provider and 30 minutes of idle time. These configurable defaults are approved policy values, not workload-validated sizing.

Adding fields to the case class intentionally breaks binary compatibility in 0.2.0. Source callers that use defaults remain valid after recompilation, but now receive finite session limits and expiry.

### Reserve session capacity before starting initialization

The provider owns `maxSessions` enforcement. Reserve capacity atomically before calling `sessionFactory.startSession`. Count pending initializations, registered sessions, and sessions still releasing provider-owned resources against the same limit. A full provider returns HTTP 503 without calling the session factory; it does not queue admission requests or evict an existing session.

Transfer the reservation to the registered session without a release-and-reacquire gap. Initialization failure or cancellation, DELETE, expiry, and shutdown release the reservation exactly once after provider cleanup. Registration, admission, and transition to termination must be ordered against shutdown. A cleanup error must not skip cleanup of other owned resources.

This limit bounds retained session count, not HTTP connection count, parsing concurrency, stream count, or memory used by one session.

### Expire idle sessions through the same termination operation

Use a monotonic clock and a provider-owned expiry worker. Start idle time at successful registration. A validated request admitted to an existing session records client activity. Rejected requests, malformed bodies, outbound notifications, and server keepalive traffic do not refresh activity.

Active admitted request processing prevents idle expiry. When the last active operation finishes, start a fresh idle interval. A passive live GET SSE connection is not active processing and does not keep a session alive indefinitely. Replay processing is active until it completes or is canceled. A valid client request, including a client ping, can refresh idle time.

After `sessionIdleTimeout`, reject new use of an idle session with HTTP 404, even before the background sweep reaches it. The expiry worker begins cleanup within one documented sweep interval. Recheck activity and transition to termination atomically so a concurrent request either acquires the session or receives HTTP 404; it must not use a session already selected for expiry.

DELETE, expiry, and provider shutdown share one idempotent per-session termination operation. It prevents new resource acquisition, releases provider-owned resources, invokes SDK session cleanup, and releases capacity. A cancellation of a caller waiting for termination must not cancel the shared cleanup. Shutdown also stops and joins the provider-owned expiry worker.

### Own provider-created resources independently of SDK tracking

Each session owns its acquired POST workers, SSE transports, GET listener handles, and replay subscriptions. Resource registration must be atomic against session termination. An acquisition that loses that race releases what it acquired instead of publishing an unowned resource.

Disconnect or body failure releases the resources belonging to that response. DELETE, expiry, and shutdown release all provider-owned resources belonging to the session, including older GET listeners no longer referenced by the SDK. Multiple GET responses remain supported; tracking them does not add a replacement or rejection policy.

Prefer acquiring POST processing and GET resources within the response body's resource scope. Do not start detached processing merely to return a response object. A response body that is never consumed must not leave a worker, transport, listener, replay subscription, or active-operation lease allocated solely for that body. Beginning body consumption must recheck whether the session is still available.

Terminal cleanup cancels owned processing and replay, closes listeners and transports, releases blocked sends, and awaits provider finalizers without depending on the client draining queued events. Normal response completion can still drain admitted events. A failure closing one resource must not skip the others. Cancellation remains subject to cooperative behavior of user code and publishers; no forced termination of uninterruptible application code is promised.

Before implementation, probe SDK response-stream cancellation, pending request cleanup, replaced listeners, and in-flight keepalive subscriptions. Use supported APIs only. Document residual SDK-internal state separately from the provider-owned guarantee. Missing SDK-internal cleanup is not a release blocker under the approved boundary, but failure to release a provider-owned resource is. If meeting that boundary requires a new SDK dependency or structural workaround, stop for approval.

### Keep transport-owned diagnostic logs payload-free

Transport-owned log events use fixed operation and failure codes with fixed messages. Do not attach the original throwable, its message, causes, suppressed exceptions, request or response payloads, headers, parameters, raw session identifiers, or other client-controlled strings. This includes initialization, request processing, notification delivery, response acceptance, and every cleanup path. Use fixed classifications rather than arbitrary exception text.

Capture log events in tests with secret markers in exception messages, causes, suppressed exceptions, identifiers, and payloads. Assert that neither formatted output, argument arrays, nor attached throwable data contains those markers at any enabled transport log level. Preserve operation-level failure visibility without asserting an end-to-end logging guarantee.

The library must not change global logger levels or install application-wide filters. Document SDK `2.0.1` payload logging and application responsibility for SDK and handler logs. HTTP error text and SDK-generated JSON-RPC errors remain outside this diagnostic-log guarantee.

### Use a closeable bounded channel for each SSE transport

Replace the unbounded event queue, close sentinel, closed `Ref`, and send-close semaphore with a closeable bounded channel. `sendMessage` waits until the event is admitted or the channel closes. Closing stops new sends and lets the stream drain admitted events in order.

Use `fs2.concurrent.Channel` if its close and blocked-producer behavior satisfies the tests. Race a blocked send with channel closure so disconnecting a consumer cannot leave a producer fiber blocked. Do not drop events or report successful admission when no event was queued.

Alternatives considered:

- Keep `Queue` and add overflow dropping. Rejected because MCP responses and notifications must not disappear silently.
- Keep the send-close semaphore around a bounded queue. Rejected because a blocked send could hold the semaphore and prevent close.
- Use an unbounded queue with monitoring. Rejected because monitoring does not enforce a resource bound.

### Use the FS2 Reactive Streams bridge for Reactor publishers

Add `fs2-reactive-streams` at the same version as `fs2-core` and convert Reactor `Flux` values through its demand-aware publisher bridge. Downstream FS2 cancellation must cancel the Reactor subscription. Remove callback subscriptions that call `unsafeRunAndForget` for every signal.

A custom Reactive Streams subscriber is rejected because the standard FS2 integration already owns demand, terminal signals, and cancellation.

### Validate security headers before request processing

Convert all http4s request headers into the multi-value map expected by `ServerTransportSecurityValidator`. Run validation for POST, GET, and DELETE before reading a body, looking up a session, or invoking SDK behavior. Map `ServerTransportSecurityException` to its HTTP status and message.

The default remains `NOOP`, matching the SDK provider. Applications can opt into host, origin, or other SDK validation without wrapping the routes.

### Apply structured Accept matching

Parse the typed http4s `Accept` header. A media range satisfies a required response type when it matches that type and has nonzero quality. This supports valid wildcards, rejects quality zero, and avoids substring matches such as `application/jsonish`.

POST requires acceptable `application/json` and `text/event-stream`. GET requires acceptable `text/event-stream`. Existing error statuses and messages remain unchanged.

### Split Reactor value and completion conversion

Keep `monoToIO[A]` for publishers that must emit one value. Empty completion becomes an error instead of producing `null`. Add a separate completion conversion for `Mono[Void]` and other call sites that only need terminal completion. Both conversions preserve bidirectional cancellation.

### Remove the deprecated routes helper

Remove `Http4sStreamableServerTransportProvider.routes`. Applications must retain the provider instance, pass it to `McpServer.sync` or `McpServer.async`, and mount `provider.routes`. This is required for session-factory installation and lifecycle methods.

## Risks / Trade-offs

- Bounded channels can make producers wait behind slow clients. Closing the channel and racing blocked sends with closure prevents permanent waits after disconnect.
- A count-based capacity does not bound the size of one serialized event. The transport bounds queue cardinality; outbound message-size policy remains application-owned.
- Structured `Accept` matching admits valid wildcard ranges that the previous substring check rejected. Protocol tests will make this behavior explicit.
- Security validation is opt-in by default. Documentation must show how internet-facing applications enable the SDK validator.
- Adding `fs2-reactive-streams` increases the published dependency set. Keeping its version aligned with FS2 avoids version skew.
- Finite defaults can reject initialization or expire clients that previously retained sessions indefinitely. Migration notes must describe client reinitialization after HTTP 404 and capacity rejection with HTTP 503.
- Active work delays idle expiry. Session caps and expiry do not bound stream count, stalled application work, or per-session memory.
- SDK shutdown is not proof that every provider response has stopped. Acceptance must observe owned workers, streams, and finalizers directly.
- Removing exception details reduces diagnostic detail. Fixed operation and failure codes preserve failure visibility without copying payloads into logs.

## Migration Plan

1. Complete the SDK feasibility checks, then implement and validate the approved configuration defaults and lifecycle behavior on both Scala versions.
2. Update README configuration and usage examples, including session capacity, idle activity rules, security validation, backpressure, and SDK cleanup and logging limitations.
3. Add a 0.2.0 migration note showing replacement of the removed companion helper.
4. Publish as 0.2.0 because the configuration constructor and helper API change binary compatibility.
5. If rollout reveals incompatibility, applications can remain on the maintained 0.1.x line while the 0.2.x fix is prepared.

## Open Questions

- No unresolved policy decisions. SDK feasibility checks remain required implementation work.
- The external feedback mentions task `7.8`, but no such task exists in the current repository. The upstream verification requirement is captured in the new SDK checks in `tasks.md`; no equivalence to the external task is claimed.
