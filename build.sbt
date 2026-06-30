ThisBuild / scalaVersion := "2.13.18"
ThisBuild / crossScalaVersions := Seq("2.13.18", "3.3.7")
ThisBuild / organization := "io.github.zikolach"
ThisBuild / version := releaseVersion
ThisBuild / homepage := Some(url("https://github.com/zikolach/http4s-mcp-transport"))
ThisBuild / licenses := Seq("MIT" -> url("https://opensource.org/licenses/MIT"))
ThisBuild / developers := List(
  Developer(
    id = "zikolach",
    name = "zikolach",
    email = "",
    url = url("https://github.com/zikolach")
  )
)
ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")
ThisBuild / Test / fork := true
ThisBuild / publish / skip := true
ThisBuild / publishMavenStyle := true
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / versionScheme := Some("semver-spec")
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/zikolach/http4s-mcp-transport"),
    "scm:git:https://github.com/zikolach/http4s-mcp-transport.git",
    Some("scm:git:git@github.com:zikolach/http4s-mcp-transport.git")
  )
)


def releaseVersion: String =
  sys.env
    .get("HTTP4S_MCP_TRANSPORT_VERSION")
    .filter(_.nonEmpty)
    .orElse(sys.env.get("GITHUB_REF_NAME").filter(_.startsWith("v")).map(_.stripPrefix("v")))
    .getOrElse("0.1.0-SNAPSHOT")

val mcpVersion = "2.0.0"
val http4sVersion = "0.23.34"
val catsEffectVersion = "3.7.0"
val fs2Version = "3.13.0"
val reactorVersion = "3.8.6"
val logbackVersion = "1.5.37"
val munitVersion = "1.3.3"
val munitCatsEffectVersion = "2.2.0"

lazy val root = (project in file("."))
  .aggregate(transport, simpleServer)
  .settings(
    name := "http4s-mcp-transport-root",
    publish / skip := true
  )

lazy val transport = (project in file("modules/http4s-mcp-transport"))
  .settings(
    name := "http4s-mcp-transport",
    description := "http4s transport provider for the MCP Java SDK Streamable HTTP server API",
    publish / skip := false,
    libraryDependencies ++= Seq(
      "io.modelcontextprotocol.sdk" % "mcp-core" % mcpVersion,
      "io.modelcontextprotocol.sdk" % "mcp-json-jackson3" % mcpVersion,
      "org.http4s" %% "http4s-core" % http4sVersion,
      "org.http4s" %% "http4s-dsl" % http4sVersion,
      "org.typelevel" %% "cats-effect" % catsEffectVersion,
      "co.fs2" %% "fs2-core" % fs2Version,
      "io.projectreactor" % "reactor-core" % reactorVersion,
      "org.slf4j" % "slf4j-api" % "2.0.18",
      "ch.qos.logback" % "logback-classic" % logbackVersion % Test,
      "org.scalameta" %% "munit" % munitVersion % Test,
      "org.typelevel" %% "munit-cats-effect" % munitCatsEffectVersion % Test
    ),
    Test / testFrameworks += new TestFramework("munit.Framework")
  )

lazy val simpleServer = (project in file("examples/simple-server"))
  .dependsOn(transport)
  .settings(
    name := "http4s-mcp-simple-server",
    publish / skip := true,
    libraryDependencies ++= Seq(
      "org.http4s" %% "http4s-ember-server" % http4sVersion,
      "com.comcast" %% "ip4s-core" % "3.8.0"
    )
  )
