## Context

Version 0.1.1 bounds POST bodies with a private 16 MiB limit and fixes session shutdown ordering. Outbound SSE transports and the Reactor `Flux` bridge still use unbounded queues, request security validation cannot be configured, and content negotiation reads raw header strings. The public configuration is a case class, so adding fields changes its binary constructor and belongs in 0.2.0.

The MCP Java SDK servlet provider is the behavioral reference. Its request limit and `ServerTransportSecurityValidator` integration define the expected security boundary. http4s and FS2 remain responsible for HTTP parsing and stream lifecycle.

## Goals / Non-Goals

**Goals:**

- Bound queued outbound events without dropping or reordering messages.
- Propagate FS2 demand and cancellation to Reactor publishers.
- Make request size and queue capacity configurable with safe defaults.
- Allow applications to use the SDK request-header security validator.
- Apply HTTP media-range and quality semantics to `Accept` validation.
- Remove nullable generic completion from Reactor interop.
- Remove the deprecated companion `routes` helper with a documented migration.

**Non-Goals:**

- Change MCP JSON-RPC message handling or session semantics.
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

Both numeric settings must be positive. The request default matches the MCP Java SDK servlet provider. A capacity of 256 permits short bursts while placing a fixed bound on queued event count.

Adding fields to the case class is intentionally a 0.2.0 binary compatibility. Source callers that use defaults remain valid after recompilation.

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

## Migration Plan

1. Implement and validate the new configuration defaults and stream behavior on both Scala versions.
2. Update README configuration and usage examples, including security validation and backpressure behavior.
3. Add a 0.2.0 migration note showing replacement of the removed companion helper.
4. Publish as 0.2.0 because the configuration constructor and helper API change binary compatibility.
5. If rollout reveals incompatibility, applications can remain on the maintained 0.1.x line while the 0.2.x fix is prepared.

## Open Questions

None.
