## Context

The MCP Java SDK `2.0.0` includes a servlet-based Streamable HTTP server transport. That transport is not tied to Jetty, but it requires a Jakarta Servlet container. Scala services that already use http4s need a reusable adapter that implements the Java SDK transport provider while exposing normal http4s routes.

Existing references:
- Official Java SDK servlet provider: correctness reference for SDK lifecycle, headers, sessions, SSE, replay, and shutdown behavior.
- `TJC-LP/fast-mcp-scala`: Scala/ZIO HTTP implementation of `McpStreamableServerTransportProvider`; useful as a Scala transport reference, but it targets SDK `1.1.1`.
- `linkyard/scala-effect-mcp`: http4s MCP route design reference; useful for http4s route shape, but it does not implement the Java SDK provider interface.

## Goals / Non-Goals

**Goals:**
- Implement a reusable http4s transport provider for the MCP Java SDK Streamable HTTP server API.
- Keep server ownership outside the library. The library returns routes; applications choose Ember, Blaze, Netty, or another http4s server backend.
- Cross-compile and test on Scala 2.13 and Scala 3.
- Keep dependency selection explicit, especially the SDK JSON mapper module.
- Provide examples and protocol lifecycle tests.

**Non-Goals:**
- Do not implement a full MCP server independent of the Java SDK.
- Do not start or manage an HTTP server inside the core transport.
- Do not make Tapir part of the core transport.
- Do not add Akka HTTP support in the initial implementation.
- Do not support Scala.js or Scala Native.

## Decisions

### Use http4s routes as the public integration surface

Expose a provider class with a `routes: HttpRoutes[IO]` or equivalent constructor-selected effect API. The transport should be mounted by the user into an existing http4s server.

Alternatives considered:
- Embedded server API: rejected because it duplicates http4s server lifecycle decisions.
- Servlet wrapper: rejected because it does not remove the servlet dependency.
- Tapir endpoint as the core API: rejected because MCP Streamable HTTP is a fixed low-level protocol endpoint where direct control of SSE and headers matters.

### Start with `cats.effect.IO`

Use `IO` for the first implementation to keep SDK Reactor bridging and tests simple. Consider a generic `F[_]` API only after the IO implementation is correct.

Alternatives considered:
- Fully generic `Async[F]`: attractive for library design, but it increases initial complexity around Reactor interop and Java SDK callbacks.
- ZIO: already covered by the reference implementation and not aligned with http4s.

### Keep SDK JSON mapper dependency explicit

Depend on `mcp-core` and require one JSON mapper module in the build or constructor documentation. The project should not depend on the aggregate `mcp` artifact if that pulls incompatible Jackson2 and Jackson3 modules together.

Alternatives considered:
- Aggregate `mcp` dependency: rejected because mixed mapper modules can create runtime dependency conflicts.
- Hardwire Jackson3 only: rejected because users may need Jackson2 integration with existing JVM stacks.

### Mirror official servlet provider behavior

Use the official Java SDK servlet transport as the behavioral reference. The http4s implementation should match status codes, headers, session behavior, SSE event names, replay behavior, and graceful shutdown unless there is a documented reason to differ.

Alternatives considered:
- Copy the ZIO HTTP implementation behavior directly: rejected because it targets SDK `1.1.1` and may not match SDK `2.0.0` behavior.

### Add optional Tapir wrapper later

A future module may expose Tapir documentation or endpoint descriptions if it helps users, but it must delegate to the core http4s routes and must not duplicate protocol logic.

Alternatives considered:
- Implement Tapir first: rejected because Tapir does not reduce the hard parts of this transport: SDK session lifecycle, SSE, replay, and cancellation.

## Risks / Trade-offs

- Reactor-to-cats-effect bridging can block if implemented naively → isolate bridging in small helpers and test cancellation and shutdown behavior.
- SSE lifecycle bugs can leak fibers or queues → model each SSE connection as a closeable stream transport and test client disconnect behavior.
- SDK `2.0.0` behavior may differ from SDK `1.1.1` references → use the official SDK `2.0.0` servlet provider as the primary reference.
- Cross-building can constrain dependencies and syntax → keep core code in syntax accepted by Scala 2.13 and Scala 3, or isolate version-specific code behind shared APIs.
- Supporting both Jackson2 and Jackson3 in one artifact can cause dependency conflicts → publish separate integration guidance or modules if both mapper families are needed.

## Migration Plan

1. Create the build with Scala 2.13 and Scala 3 cross-compilation.
2. Add the core provider and route API behind tests.
3. Add minimal examples for both Scala versions through the same source API.
4. Run local lifecycle tests against http4s routes without a real TCP server.
5. Run at least one end-to-end example through an http4s server.
6. Only after tests pass, consider using the library in the Prewave MCP service as a separate change.

Rollback is simple before adoption: keep the Prewave service on the existing SDK servlet transport. After adoption, rollback means switching the service back to the servlet provider and removing the new dependency.

## Open Questions

- Should the first public API be fixed to `IO`, or should it expose both `IO` and generic `F[_]: Async` variants?
- Should Jackson2 and Jackson3 support be separate modules, or should the core module require users to provide the mapper and dependencies?
- Which CI provider should be used for the separate repository?
