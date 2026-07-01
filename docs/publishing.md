# Publishing

This project publishes the library module to Maven Central through GitHub Actions.

## Version source

The sbt build derives the package version from:

1. `HTTP4S_MCP_TRANSPORT_VERSION`, when set;
2. tag names like `v0.1.0`, via `GITHUB_REF_NAME`;
3. `0.1.0-SNAPSHOT` for local builds.

Published Scala coordinates use `%%` in sbt:

```scala
libraryDependencies += "io.github.zikolach" %% "http4s-mcp-transport" % "VERSION"
```

Concrete Maven artifact IDs are:

- `io.github.zikolach:http4s-mcp-transport_2.13`
- `io.github.zikolach:http4s-mcp-transport_3`

The root project and example project are not published.

## Maven Central

The `Publish Maven Central` workflow publishes stable releases on `v*` tags.
It can also be run manually for `-SNAPSHOT` versions.
Manual stable runs fail intentionally because `sbt-ci-release` requires a real tag ref for stable publishing.

After a successful stable tag publish, the workflow creates or updates the GitHub Release from `CHANGELOG.md` and includes Maven Central coordinates.

Required GitHub Actions secrets match the names used by `../sui`:

- `MILL_SONATYPE_USERNAME` — Sonatype Central user token username
- `MILL_SONATYPE_PASSWORD` — Sonatype Central user token password
- `MILL_PGP_SECRET_BASE64` — base64-encoded private signing key
- `MILL_PGP_PASSPHRASE` — passphrase for the signing key, if set

The workflow maps these secrets to sbt-ci-release environment variables.

Before the first Central release, ensure the `io.github.zikolach` namespace is verified in Central Portal and the signing public key has been published to a public keyserver.

## Local checks

Create local publish artifacts without Sonatype credentials:

```bash
sbt "+publishLocal"
```

Run the same validation used by CI:

```bash
sbt "scalafmtCheckAll; +compile; +transport / Test / testOnly io.github.http4smcp.Http4sStreamableServerTransportProviderSuite; simpleServer/compile"
```
