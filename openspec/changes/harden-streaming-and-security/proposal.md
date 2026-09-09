## Why

The transport currently has no configurable resource limits or SDK header validation, and its Reactor-to-FS2 bridge can buffer without propagating demand. Sessions have neither an admission cap nor idle expiry. Provider shutdown does not explicitly own every response worker and stream, and transport exception logs can expose payloads. Version 0.2.0 should define and test these limits, lifecycle guarantees, and logging boundaries.

## What Changes

- Add configurable POST request and outbound stream limits with safe defaults.
- Make Reactor-to-FS2 streaming demand-aware and prevent unbounded buffering.
- Enable configurable limits of 1,024 sessions per provider and 30 minutes of idle time by default, including capacity reservations for initialization.
- Own every provider-created response worker, SSE transport, GET listener handle, and replay subscription through disconnect and session termination.
- Make transport-owned diagnostic logs payload-free without changing application logging configuration.
- Verify SDK session retention and cleanup behavior explicitly; document SDK-internal cleanup and logging limitations rather than claiming end-to-end guarantees.
- Expose the MCP SDK `ServerTransportSecurityValidator` through the http4s provider configuration.
- Parse `Accept` as a structured HTTP header, including media ranges and quality values.
- Replace nullable empty-`Mono` conversion with separate value and completion APIs.
- **BREAKING**: Extend `Http4sStreamableServerTransportProviderConfig` with resource and security settings.
- **BREAKING**: Remove the deprecated companion `Http4sStreamableServerTransportProvider.routes` helper.

## Capabilities

### New Capabilities

- `http4s-streamable-transport-hardening`: Resource limits, session admission and expiry, provider-owned stream cleanup, transport-owned safe logging, demand-aware streaming, request security validation, HTTP content negotiation, typed Reactor interop, and the 0.2.0 API cleanup.

### Modified Capabilities

None.

## Impact

- Changes the public provider configuration and removes one deprecated public helper.
- Changes outbound stream buffering and producer backpressure behavior.
- Adds request-header validation before MCP request processing.
- Rejects new initialization when session capacity is exhausted and expires idle sessions without requiring client DELETE.
- Changes diagnostics by removing original exceptions and raw identifiers from transport-owned logs.
- Requires SDK feasibility checks before implementing the new session lifecycle work.
- Affects the provider, per-stream transport, Reactor interop, tests, README, migration notes, and published API compatibility.
- May add the FS2 Reactive Streams integration module if it provides the required demand propagation without a custom subscriber.
