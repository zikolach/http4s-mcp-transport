# Changelog

All notable changes to http4s-mcp-transport will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project uses semantic versioning while it remains pre-1.0.

## [Unreleased]

## [0.1.1] - 2026-09-09

### Changed

- Limited MCP POST request bodies to 16 MiB. Larger bodies now return HTTP 413.
- Hardened session shutdown, SSE response cleanup, and concurrent transport send and close behavior.
- Updated the MCP Java SDK to 2.0.1 for transport correctness fixes and bounded HTTP reads.
- Updated Scala 3 LTS, Cats Effect, FS2, Reactor Core, SLF4J, and test dependencies.
- Updated sbt, Scalafmt, publishing plugins, and GitHub Actions.

### Security

- Updated http4s to 0.23.37 to include upstream denial-of-service fixes for Ember HTTP/2 and WebSocket handling.

## [0.1.0] - 2026-07-01

### Added

- Added an http4s Streamable HTTP transport provider for the MCP Java SDK server API.
- Added routes for MCP initialization, notifications, request/response SSE streams, standalone SSE streams, session deletion, and Last-Event-ID replay.
- Added transport context extraction, client notification helpers, graceful shutdown, and Reactor cancellation propagation.
- Added Scala 2.13 and Scala 3 cross-build support with sbt 2.
- Added a simple http4s server example.
- Added Scalafmt, GitHub Actions CI, and Maven Central publishing configuration.
- Added release automation for GitHub Releases with Maven Central coordinates.

[Unreleased]: https://github.com/zikolach/http4s-mcp-transport/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/zikolach/http4s-mcp-transport/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/zikolach/http4s-mcp-transport/releases/tag/v0.1.0
