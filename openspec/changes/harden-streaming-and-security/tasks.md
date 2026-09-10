## Execution gates

Defaults of 1,024 sessions per provider and 30 minutes of idle time are approved under Q3, O3-1. Both settings remain configurable. Cleanup and safe logging cover provider-owned resources and transport-owned diagnostic logs.

Complete section 7 before implementing the new session lifecycle work. Implement admission before expiry, and shared resource ownership before validating termination. Section 6 is the final verification gate after all implementation sections. Existing task numbers remain unchanged; the external reference to task `7.8` has no known local equivalent.

## 1. Public configuration and dependencies

- [x] 1.1 Add positive `requestMaxBytes` and `outboundBufferCapacity` settings with defaults of 16 MiB and 256 events.
- [x] 1.2 Add the `ServerTransportSecurityValidator` setting with `NOOP` as its default.
- [x] 1.3 Use the existing FS2 Flow and Reactor JDK adapter APIs without adding `fs2-reactive-streams`; confirm compilation for both Scala versions under approved Q4, O4-1.
- [x] 1.4 Add positive finite `maxSessions` and `sessionIdleTimeout` settings with defaults of 1,024 sessions and 30 minutes; reject disabling sentinel values and test both defaults.

## 2. Request validation

- [x] 2.1 Replace the private request limit with `requestMaxBytes` and test custom limits, exact-limit bodies, declared oversize bodies, and streamed oversize bodies.
- [x] 2.2 Convert all http4s request headers to the SDK multi-value representation and apply security validation before POST, GET, and DELETE behavior.
- [x] 2.3 Map `ServerTransportSecurityException` to its HTTP status and verify rejected requests do not read bodies or access sessions.
- [x] 2.4 Replace raw `Accept` substring checks with typed media-range and quality matching, including wildcard, quality-zero, repeated-header, and invalid-substring tests.

## 3. Bounded and demand-aware streaming

- [x] 3.1 Replace each transport event queue and close sentinel with a bounded closeable channel configured by `outboundBufferCapacity`.
- [x] 3.2 Make blocked sends race channel closure and test ordering, full-buffer backpressure, disconnect cleanup, and sends after close. F8 repaired: closure without admission fails the send.
- [x] 3.3 Replace callback-based `Flux` subscription with the FS2 Flow publisher bridge and Reactor `JdkFlowAdapter`, preserving original publisher errors.
- [x] 3.4 Test bounded upstream demand, Reactor error propagation, downstream cancellation after established demand, and resource release on both Scala versions.

## 4. Typed Reactor interop

- [x] 4.1 Split value-producing and completion-only `Mono` conversion APIs without returning nullable generic values.
- [x] 4.2 Migrate every provider and transport call site to the correct conversion API.
- [x] 4.3 Test emitted values, empty required values, empty completion, errors, and bidirectional cancellation.

## 5. API cleanup and migration

- [x] 5.1 Remove the deprecated companion `Http4sStreamableServerTransportProvider.routes` helper and update all repository usage to retain the provider instance.
- [x] 5.2 Advance the development version to `0.2.0-SNAPSHOT` and add concise `[Unreleased]` compatibility and behavior notes.
- [x] 5.3 Update README configuration, security-validator, backpressure, and migration examples, including session admission HTTP 503, expiry HTTP 404, activity rules, and transport-owned logging and cleanup boundaries.

## 6. Verification

- [x] 6.1 Run Scalafmt, cross-compilation, all transport tests, and example compilation on Scala 2.13 and Scala 3.
- [x] 6.2 Run dependency eviction checks and local publication for both artifact variants.
- [x] 6.3 Run focused security and maintainability reviews against the specification and resolve blocking findings, with explicit evidence for session limits, expiry, provider-owned cleanup, logging, and residual SDK limitations.

## 7. Session defaults and upstream feasibility

- [x] 7.1 Record approved defaults of 1,024 sessions per provider and 30 minutes of idle time, with active-work semantics defined in the design.
- [x] 7.2 Verify admission and expiry capabilities in the selected SDK version. Record source evidence and assign enforcement to the provider where upstream coverage is absent; do not treat servlet-provider parity as acceptance.
- [x] 7.3 Probe SDK response-stream cancellation and pending-request cleanup, replaced GET listeners, and in-flight keepalive cancellation using supported APIs. Record runtime evidence and SDK-internal limitations separately from provider-owned cleanup. Escalate any needed dependency change or structural workaround for approval.

## 8. Session admission, expiry, and resource ownership

- [x] 8.1 Implement atomic capacity reservations before session creation and transfer them to registered sessions. Count pending initialization and terminating sessions; reject excess initialization with HTTP 503 without factory invocation or admission queuing.
- [x] 8.2 Test concurrent admission, successful registration, initialization failure and cancellation, cleanup failures, shutdown races, and exactly-once capacity release.
- [x] 8.3 Implement monotonic idle tracking and atomic activity admission. Active processing delays expiry; passive SSE and server traffic do not. Retired sessions return HTTP 404 even before the expiry sweep.
- [x] 8.4 Add a provider-owned expiry worker and shared per-session termination for DELETE, expiry, and shutdown. Document the sweep interval, stop and join the expiry worker on shutdown, and keep shared cleanup independent of waiting-caller cancellation. F9 repaired: sweeps start tracked cleanup without waiting for its completion.
- [x] 8.5 Scope response workers, transports, GET listener handles, and replay subscriptions to provider-owned resources. Avoid acquisition solely for unconsumed bodies and order acquisition against session termination.
- [x] 8.6 Test expiry with a controlled clock, including activity at the deadline, active POST and replay, passive GET, keepalive exclusion, malformed or rejected requests, and a fresh idle interval after active work completes. The F9 regression verifies background expiry of a later-idle session while an earlier cleanup remains blocked.
- [x] 8.7 Test termination during replay and POST processing, multiple GET listeners, never-consumed bodies, delayed body consumption, acquisition cancellation, full outbound channels, and failing cleanup. Observe provider finalizers and blocked-producer release directly rather than inferring them from SDK close completion.
- [x] 8.8 Test concurrent DELETE, expiry, and shutdown, plus cancellation of cleanup waiters, without double cleanup or leaked reservations.

## 9. Transport-owned safe diagnostics

- [x] 9.1 Replace transport-owned exception and raw-identifier logging with fixed operation and failure codes across initialization, processing, notification delivery, response acceptance, and cleanup.
- [x] 9.2 Capture transport log events at all enabled levels and test secret markers in payloads, headers, identifiers, exception messages, causes, and suppressed exceptions. Inspect formatted output, arguments, and throwable data.
- [x] 9.3 Document SDK and application logging exclusions and observed SDK cleanup limitations without changing global logging configuration or expanding the guarantee to HTTP and JSON-RPC error payloads.
