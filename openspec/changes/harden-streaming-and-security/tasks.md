## 1. Public configuration and dependencies

- [ ] 1.1 Add positive `requestMaxBytes` and `outboundBufferCapacity` settings with defaults of 16 MiB and 256 events.
- [ ] 1.2 Add the `ServerTransportSecurityValidator` setting with `NOOP` as its default.
- [ ] 1.3 Add `fs2-reactive-streams` at the existing FS2 version and confirm dependency resolution for both Scala versions.

## 2. Request validation

- [ ] 2.1 Replace the private request limit with `requestMaxBytes` and test custom limits, exact-limit bodies, declared oversize bodies, and streamed oversize bodies.
- [ ] 2.2 Convert all http4s request headers to the SDK multi-value representation and apply security validation before POST, GET, and DELETE behavior.
- [ ] 2.3 Map `ServerTransportSecurityException` to its HTTP status and verify rejected requests do not read bodies or access sessions.
- [ ] 2.4 Replace raw `Accept` substring checks with typed media-range and quality matching, including wildcard, quality-zero, repeated-header, and invalid-substring tests.

## 3. Bounded and demand-aware streaming

- [ ] 3.1 Replace each transport event queue and close sentinel with a bounded closeable channel configured by `outboundBufferCapacity`.
- [ ] 3.2 Make blocked sends race channel closure and test ordering, full-buffer backpressure, disconnect cleanup, and sends after close.
- [ ] 3.3 Replace callback-based `Flux` subscription with the FS2 Reactive Streams publisher bridge.
- [ ] 3.4 Test bounded upstream demand, Reactor error propagation, downstream cancellation, and resource release.

## 4. Typed Reactor interop

- [ ] 4.1 Split value-producing and completion-only `Mono` conversion APIs without returning nullable generic values.
- [ ] 4.2 Migrate every provider and transport call site to the correct conversion API.
- [ ] 4.3 Test emitted values, empty required values, empty completion, errors, and bidirectional cancellation.

## 5. API cleanup and migration

- [ ] 5.1 Remove the deprecated companion `Http4sStreamableServerTransportProvider.routes` helper and update all repository usage to retain the provider instance.
- [ ] 5.2 Advance the development version to `0.2.0-SNAPSHOT` and add concise `[Unreleased]` compatibility and behavior notes.
- [ ] 5.3 Update README configuration, security-validator, backpressure, and migration examples.

## 6. Verification

- [ ] 6.1 Run Scalafmt, cross-compilation, all transport tests, and example compilation on Scala 2.13 and Scala 3.
- [ ] 6.2 Run dependency eviction checks and local publication for both artifact variants.
- [ ] 6.3 Run focused security and maintainability reviews against the specification and resolve blocking findings.
