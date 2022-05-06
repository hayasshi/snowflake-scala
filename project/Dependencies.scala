import sbt._

object Dependencies {

  object Version {
    val scalaTest = "3.2.11"

    val akka = "2.6.18"

    val awsSdk = "2.17.121"

    val testcontainersScala = "0.40.0"
  }

  lazy val scalaTest = "org.scalatest" %% "scalatest-funsuite" % Version.scalaTest

  lazy val akka = "com.typesafe.akka" %% "akka-actor" % Version.akka

  lazy val akkaTestkit = "com.typesafe.akka" %% "akka-testkit" % Version.akka

  lazy val awsSdkDynamoDb = "software.amazon.awssdk" % "dynamodb" % Version.awsSdk

  lazy val testcontainersScala = "com.dimafeng" %% "testcontainers-scala" % Version.testcontainersScala

}
