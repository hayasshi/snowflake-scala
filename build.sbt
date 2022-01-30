import Dependencies._

val commonSettings = Seq(
  organization := "com.github.hayasshi",
  version      := "0.1.0-SNAPSHOT",
  scalaVersion := "2.13.8",
  scalacOptions ++= Seq(
    // https://docs.scala-lang.org/scala3/guides/migration/tooling-tour.html#the-scala-213-compiler
    "-Xsource:3",

    // https://docs.scala-lang.org/overviews/compiler-options/index.html
    "-deprecation",
    "-Xfatal-warnings",
    "-Wdead-code",
    "-Wnumeric-widen",
    "-Xlint:adapted-args",
    "-Xlint:inaccessible",
    "-Xlint:infer-any",
    "-Xlint:nullary-unit",
    "-Xlint:unused"
  ),
  Test / fork := true
)

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(
    name := "snowflake-scala"
  )
  .aggregate(
    akkaDynamoDb
  )

lazy val akkaDynamoDb = (project in file("akka-dynamodb"))
  .settings(commonSettings)
  .settings(
    name := "snowflake-scala-akka-dynamodb",
    libraryDependencies ++= Seq(
      akka,
      awsSdkDynamoDb,
      scalaTest           % Test,
      testcontainersScala % Test
    )
  )
