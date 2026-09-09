## Why

The transport currently has no configurable resource limits or SDK header validation, and its Reactor-to-FS2 bridge can buffer without propagating demand. Version 0.2.0 should make these limits and security controls explicit before the public API gains more users.

## What Changes

- Add configurable POST request and outbound stream limits with safe defaults.
- Make Reactor-to-FS2 streaming demand-aware and prevent unbounded buffering.
- Expose the MCP SDK `ServerTransportSecurityValidator` through the http4s provider configuration.
- Parse `Accept` as a structured HTTP header, including media ranges and quality values.
- Replace nullable empty-`Mono` conversion with separate value and completion APIs.
- **BREAKING**: Extend `Http4sStreamableServerTransportProviderConfig` with resource and security settings.
- **BREAKING**: Remove the deprecated companion `Http4sStreamableServerTransportProvider.routes` helper.

## Capabilities

### New Capabilities

- `http4s-streamable-transport-hardening`: Resource limits, demand-aware streaming, request security validation, HTTP content negotiation, typed Reactor interop, and the 0.2.0 API cleanup.

### Modified Capabilities

None.

## Impact

- Changes the public provider configuration and removes one deprecated public helper.
- Changes outbound stream buffering and producer backpressure behavior.
- Adds request-header validation before MCP request processing.
- Affects the provider, per-stream transport, Reactor interop, tests, README, migration notes, and published API compatibility.
- May add the FS2 Reactive Streams integration module if it provides the required demand propagation without a custom subscriber.
