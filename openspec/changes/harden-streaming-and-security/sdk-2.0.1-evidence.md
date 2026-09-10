# MCP SDK 2.0.1 lifecycle evidence

Source artifact: `mcp-core-2.0.1-sources.jar`

SHA-256: `29ab2a6bdc1058327a233b0ae52203da1941ad911f40b1e7a2e2f318a2b1692f`

## Source findings

- `HttpServletStreamableServerTransportProvider.java:115,455-457` uses an uncapped `ConcurrentHashMap` and inserts initialized sessions directly. It has no idle-expiry state or worker. The http4s provider must enforce admission and expiry.
- `McpStreamableServerSession.java:187-190,308-313` replaces `listeningStreamRef` without closing the prior listener, and `closeGracefully` closes only the current listener. The http4s provider must own every GET listener.
- `McpStreamableServerSession.java:205-239,443-451` creates a response-local stream but exposes no close handle. Stream cleanup removes pending response state only through the stream handle or an error path.
- `KeepAliveScheduler.java:91-121` starts each ping through a nested `subscribe` and retains only the interval subscription. `shutdown` cannot cancel an already subscribed ping.
- `McpSchema.java:210-214` logs raw JSON at DEBUG. SDK and application logs remain outside the transport-owned payload-free logging guarantee.

## Runtime probes

Command:

```text
sbt '++2.13.18' 'transport/Test/testOnly io.github.http4smcp.SdkLifecycleProbeSuite'
```

Result on 2026-09-09: 3 tests passed with SDK 2.0.1.

Observed through supported APIs:

- Replacing a GET listener and calling session `closeGracefully` closed only the latest transport. The older transport remained open.
- Canceling `responseStream` while its handler awaited a client ping left the pending request accepted by a later matching response. Cancellation alone did not remove that pending SDK state.
- Calling `KeepAliveScheduler.shutdown` after an in-flight ping subscribed did not cancel the ping.

These SDK-internal limits are accepted under the approved boundary. Provider acceptance must separately observe provider-owned workers, transports, listener handles, replay subscriptions, and admission reservations.

## Streaming bridge verification

The `fs2-reactive-streams` 3.14.0 probe subscribed to `Flux.never`, waited for upstream demand, and then canceled the consuming fiber. The isolated test timed out after 31 seconds on Scala 2.13.18. Waiting only for subscription could falsely pass because cancellation could happen before the consumer began waiting for data.

Q4, O4-1 approves using the standard Flow bridge instead. `ReactorInterop.fluxToStream` now uses `Stream.fromPublisher` from FS2 3.14.0 with Reactor 3.8.7 `JdkFlowAdapter` and chunk size one. Upstream errors cross the bridge as `Either` values so FS2 does not replace the original throwable with its private wrapper. No custom subscriber, dependency addition, or version change is needed.

Commands:

```text
sbt '++2.13.18; transport/Test/testOnly io.github.http4smcp.Http4sStreamableServerTransportProviderSuite -- --tests *Flux*'
sbt '++3.3.8; transport/Test/testOnly io.github.http4smcp.Http4sStreamableServerTransportProviderSuite -- --tests *Flux*'
```

Both Scala versions passed all four focused tests. They cover bounded demand, original upstream errors before and after values, ordering, empty completion, downstream errors, early downstream completion with upstream cancellation, and cancellation of a stalled publisher after demand.

## Provider implementation verification

Command:

```text
sbt 'scalafmtCheckAll; ++2.13.18; compile; transport/Test/testOnly io.github.http4smcp.Http4sStreamableServerTransportProviderSuite io.github.http4smcp.SdkLifecycleProbeSuite; simpleServer/compile; ++3.3.8; compile; transport/Test/testOnly io.github.http4smcp.Http4sStreamableServerTransportProviderSuite io.github.http4smcp.SdkLifecycleProbeSuite; simpleServer/compile'
```

Result on 2026-09-10: both Scala versions compiled the library and example. Each version passed 69 tests, consisting of 66 provider and interop tests plus 3 SDK lifecycle probes.

The provider tests cover configuration defaults and invalid limits, exact and oversized request bodies, case-insensitive repeated headers, security rejection ordering, structured `Accept` values, bounded transport sends, typed Reactor conversion, capacity reservation and release, controlled-clock expiry, active POST and replay, passive GET, server notifications and keepalives, malformed and rejected requests, all termination paths, unconsumed and delayed bodies, response ownership races, POST and replay cancellation, multiple GET listeners, cleanup failures, canceled cleanup waiters, and fixed transport diagnostic data.

Additional gates:

```text
sbt 'scalafmtCheckAll; ++2.13.18; evicted; ++3.3.8; evicted'
sbt 'reload; ++2.13.18; transport/publishLocal; ++3.3.8; transport/publishLocal'
```

Scalafmt passed. Dependency eviction completed for both Scala versions with the existing selected-version warnings and no build failure. Local publication produced `http4s-mcp-transport_2.13` and `http4s-mcp-transport_3` at `0.2.0-SNAPSHOT`.

The provider guarantee covers its admission reservations, expiry worker, POST workers, transports, listener handles, replay subscriptions, acceptance subscriptions, and finalizers. It does not extend to the private SDK state described above or to uninterruptible application code.

## Independent review lifecycle repairs

Two blocking lifecycle findings were repaired as one batch:

- Activity admission after response ownership now starts retirement and returns without waiting for termination that owns the same response. POST and GET replay bodies unwind, complete their finalizers, avoid SDK startup, release capacity, and allow shutdown to complete.
- Notification and JSON-RPC response acceptance subscriptions are registered atomically with activity admission. DELETE and shutdown cancel and await those subscriptions and their cooperative finalizers before releasing session capacity.

Focused regression names:

- `POST ownership unwinds when activity admission expires before worker startup`
- `GET replay ownership unwinds when activity admission expires before SDK startup`
- `DELETE cancels and awaits an admitted notification acceptance operation`
- `shutdown cancels and awaits an admitted JSON-RPC response acceptance operation`

Commands:

```text
sbt 'scalafmtAll; ++2.13.18; transport/Test/testOnly io.github.http4smcp.Http4sStreamableServerTransportProviderSuite -- --tests *ownership*unwinds* --tests *acceptance*operation*'
sbt '++3.3.8; transport/Test/testOnly io.github.http4smcp.Http4sStreamableServerTransportProviderSuite -- --tests *ownership*unwinds* --tests *acceptance*operation*'
```

Result on 2026-09-10: all four focused regressions passed on Scala 2.13.18 and Scala 3.3.8. The complete cross-version command above then passed all 69 tests on each version.

## Final review gate

Security review is ready. Lifecycle re-review confirmed repairs for F6, activity admission waiting for its own cleanup, and F7, unowned notification and response acceptance subscriptions. The repair validation passed 69 tests on each Scala version.

The final complete-scope lifecycle re-review remains not ready:

- F8: Closing a full transport releases blocked sends with successful completion even when their events were not admitted. Existing tests accept that result and must distinguish admission from closure.
- F9: The expiry sweep awaits session termination, so one slow cleanup blocks later sweeps. A session that becomes idle during that wait can miss its documented expiry interval. Add a regression with one cleanup held while another session reaches its idle deadline.

These are source-reviewed failure schedules, not newly executed failing regressions. Tasks 3.2, 8.4, and 8.6 are reopened. Task 6.3 remains incomplete. Implementation stopped after the allowed re-review; the passing test count is not release acceptance. No implementation commit, push, or release has occurred.

## F8 and F9 repair verification

The user approved another repair pass. F8 now preserves the channel admission result: only successful admission completes the send successfully. Channel closure before admission raises `MCP_TRANSPORT_CLOSED`, including sends already waiting for capacity and sends started after closure.

F9 now starts shared session cleanup without making the expiry sweep await it. Termination remains tracked by the session reservation for shutdown to await. The new `background expiry continues while another session cleanup is blocked` test advances a controlled monotonic clock, holds the first session's SDK cleanup, finishes the second session's active POST, and observes the background worker retire the second session without releasing the first cleanup gate. It verifies the second slot becomes available while the first stays reserved.

Validation:

```text
sbt 'scalafmtCheckAll; ++2.13.18; compile; transport/Test/test; simpleServer/compile; ++3.3.8; compile; transport/Test/test; simpleServer/compile'
sbt '++2.13.18; transport/Test/testOnly io.github.http4smcp.SdkLifecycleProbeSuite; ++3.3.8; transport/Test/testOnly io.github.http4smcp.SdkLifecycleProbeSuite'
```

Each Scala version passed 67 provider and interop tests and 3 explicitly rerun SDK probes, for 70 tests per version. Library and example compilation passed. Independent lifecycle review of this repair remains the acceptance gate; no implementation commit or push has occurred yet.

## Accepted implementation

After the user-approved F8/F9 repair pass, the independent complete-scope lifecycle review is ready with no blocking or material non-blocking findings. It confirms F8/F9 are resolved and F6/F7 remain correct. Security review remains ready. All 35 implementation and verification tasks are complete.

Exact stable local publication also passed after stopping the persistent sbt server:

```text
sbt --client shutdown
HTTP4S_MCP_TRANSPORT_VERSION=0.2.0 sbt 'show version; +publishLocal'
```

Both artifact variants were published locally at `0.2.0`. `scalafmtCheckAll` passed after the repairs. No tag or remote publication is part of this acceptance evidence. The documented SDK and response-error disclosure limits remain unchanged.

Hosted CI run `34450726150` passed all 70 tests on each Scala version for implementation commit `2fdef8f`. The subsequent changelog-only run reused sbt's incremental test results. CI and release validation now use `+transport/Test/testOnly *` to execute every transport suite explicitly, including SDK probes, instead of relying on `test` and its incremental selection.

## Release validation synchronization repair

Release run `34451807835` failed before publication because the unconsumed-GET test attempted capacity reuse immediately after a nonblocking expiry sweep. It received HTTP 503 while cleanup still held the reservation. Under Q5, O5-1, the user approved repairing the test synchronization and moving the unpublished `v0.2.0` tag after validation.

The three expiry tests that assert capacity reuse now await an explicit `afterTermination` signal before initializing another session. Production code and assertions are unchanged. The full suites passed 70 tests per Scala version. The three affected tests also passed five consecutive focused runs on each Scala version. Formatting and library/example compilation passed.

Hosted repair CI run `34452622279` then exposed a two-second deadline inside the pending-initialization test's fake session factory. The gate now waits for the test driver and is always released during cleanup. The capacity assertion still runs while the factory is blocked. All 70 tests passed per Scala version with the test JVM restricted to two active processors. This follow-up changes tests only; production behavior is unchanged.
