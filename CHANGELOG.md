# Changelog

All notable changes to http4s-mcp-transport will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project uses semantic versioning while it remains pre-1.0.

## [Unreleased]

## [0.1.0] - 2026-07-01

### Added

- Added an http4s Streamable HTTP transport provider for the MCP Java SDK server API.
- Added routes for MCP initialization, notifications, request/response SSE streams, standalone SSE streams, session deletion, and Last-Event-ID replay.
- Added transport context extraction, client notification helpers, graceful shutdown, and Reactor cancellation propagation.
- Added Scala 2.13 and Scala 3 cross-build support with sbt 2.
- Added a simple http4s server example.
- Added Scalafmt, GitHub Actions CI, and Maven Central publishing configuration.
- Added release automation for GitHub Releases with Maven Central coordinates.

[Unreleased]: https://github.com/zikolach/http4s-mcp-transport/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/zikolach/http4s-mcp-transport/releases/tag/v0.1.0
