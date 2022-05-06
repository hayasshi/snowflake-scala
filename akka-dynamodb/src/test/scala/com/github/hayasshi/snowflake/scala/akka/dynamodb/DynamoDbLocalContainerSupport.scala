package com.github.hayasshi.snowflake.scala.akka.dynamodb

import com.dimafeng.testcontainers.GenericContainer
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import java.net.URI

class DynamoDbLocalContainerSupport() {

  private val portOrigin = 8000

  val container: GenericContainer = GenericContainer(
    "amazon/dynamodb-local:latest",
    exposedPorts = Seq(portOrigin)
  )
  container.start()

  val testHost: String = container.containerIpAddress
  val testPort: Int    = container.mappedPort(portOrigin)

  val client: DynamoDbAsyncClient =
    DynamoDbAsyncClient
      .builder()
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
      .region(Region.AP_NORTHEAST_1)
      .endpointOverride(URI.create(s"http://$testHost:$testPort"))
      .build()

  def close(): Unit = {
    container.stop()
  }

}
