ThisBuild / scalaVersion := "2.13.18"
ThisBuild / crossScalaVersions := Seq("2.13.18", "3.3.7")
ThisBuild / organization := "io.github.http4smcp"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / homepage := Some(url("https://github.com/http4s-mcp/http4s-mcp-transport"))
ThisBuild / licenses := Seq("MIT" -> url("https://opensource.org/licenses/MIT"))
ThisBuild / developers := List(
  Developer(
    id = "http4s-mcp",
    name = "http4s MCP transport contributors",
    email = "",
    url = url("https://github.com/http4s-mcp")
  )
)
ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")
ThisBuild / Test / fork := true
ThisBuild / publish / skip := true

lazy val V = new {
  val mcp = "2.0.0"
  val http4s = "0.23.34"
  val catsEffect = "3.7.0"
  val fs2 = "3.13.0"
  val reactor = "3.7.0"
  val logback = "1.5.22"
  val munit = "1.2.4"
  val munitCatsEffect = "2.1.0"
}

lazy val root = (project in file("."))
  .aggregate(transport, simpleServer)
  .settings(
    name := "http4s-mcp-transport-root",
    publish / skip := true
  )

lazy val transport = (project in file("modules/http4s-mcp-transport"))
  .settings(
    name := "http4s-mcp-transport",
    libraryDependencies ++= Seq(
      "io.modelcontextprotocol.sdk" % "mcp-core" % V.mcp,
      "io.modelcontextprotocol.sdk" % "mcp-json-jackson3" % V.mcp,
      "org.http4s" %% "http4s-core" % V.http4s,
      "org.http4s" %% "http4s-dsl" % V.http4s,
      "org.typelevel" %% "cats-effect" % V.catsEffect,
      "co.fs2" %% "fs2-core" % V.fs2,
      "io.projectreactor" % "reactor-core" % V.reactor,
      "org.slf4j" % "slf4j-api" % "2.0.17",
      "ch.qos.logback" % "logback-classic" % V.logback % Test,
      "org.scalameta" %% "munit" % V.munit % Test,
      "org.typelevel" %% "munit-cats-effect" % V.munitCatsEffect % Test
    ),
    Test / testFrameworks += new TestFramework("munit.Framework")
  )

lazy val simpleServer = (project in file("examples/simple-server"))
  .dependsOn(transport)
  .settings(
    name := "http4s-mcp-simple-server",
    publish / skip := true,
    libraryDependencies ++= Seq(
      "org.http4s" %% "http4s-ember-server" % V.http4s,
      "com.comcast" %% "ip4s-core" % "3.7.0"
    )
  )
