package com.github.hayasshi.snowflake.scala.akka.dynamodb

import com.dimafeng.testcontainers.GenericContainer
import com.github.hayasshi.snowflake.scala.core.IdFormat
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{ Millis, Seconds, Span }
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.*

import java.net.URI
import java.time.Instant
import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{ DurationInt, FiniteDuration }

class WorkerIdAssignDynamoDbOperatorTest extends AnyFunSuite with ScalaFutures with BeforeAndAfterAll {
  import com.github.hayasshi.snowflake.scala.akka.dynamodb.WorkerIdAssignItem.*

  implicit class FiniteDurationToScalatestTimeout(duration: FiniteDuration) {
    def timeout: Timeout = Timeout(Span(duration.toMillis, Millis))
  }

  val dynamoDbPortOrigin = 8000

  val dynamoDbContainer: GenericContainer = GenericContainer(
    "amazon/dynamodb-local:1.18.0",
    exposedPorts = Seq(dynamoDbPortOrigin)
  )
  dynamoDbContainer.start()

  val dynamoDbHost: String = dynamoDbContainer.containerIpAddress
  val dynamoDbPort: Int    = dynamoDbContainer.mappedPort(dynamoDbPortOrigin)

  override def afterAll(): Unit = {
    dynamoDbContainer.stop()
    super.afterAll()
  }

  val client: DynamoDbAsyncClient =
    DynamoDbAsyncClient.builder().endpointOverride(URI.create(s"http://$dynamoDbHost:$dynamoDbPort")).build()

  val tableName = "TestTable"

  {
    val request = CreateTableRequest
      .builder()
      .tableName(tableName)
      .billingMode(BillingMode.PAY_PER_REQUEST)
      .attributeDefinitions(
        AttributeDefinition.builder().attributeType(ScalarAttributeType.N).attributeName(PartitionKeyName).build(),
        AttributeDefinition.builder().attributeType(ScalarAttributeType.N).attributeName(SortKeyName).build()
      )
      .keySchema(
        KeySchemaElement.builder().keyType(KeyType.HASH).attributeName(PartitionKeyName).build(),
        KeySchemaElement.builder().keyType(KeyType.RANGE).attributeName(SortKeyName).build()
      )
      .build()
    println(client.createTable(request).get())
  }

  val operator = new WorkerIdAssignDynamoDbOperator(client, tableName)

  val testIdFormat: IdFormat = IdFormat.Default(Instant.EPOCH)

  test("`putForPreAssign` can put pre assign item when keys(PartitionKey and SortKey pair) is not exists") {
    val existsItem1 = createKeyMap(testIdFormat.datacenterId(1L), testIdFormat.workerId(2L))
    val existsItem2 = createKeyMap(testIdFormat.datacenterId(2L), testIdFormat.workerId(1L))
    client.putItem(PutItemRequest.builder().tableName(tableName).item(existsItem1).build()).get()
    client.putItem(PutItemRequest.builder().tableName(tableName).item(existsItem2).build()).get()

    val expectedItem = WorkerIdAssignItem(
      testIdFormat.datacenterId(1L),
      testIdFormat.workerId(1L),
      AssignState.PreAssign,
      "localhost",
      UUID.randomUUID(),
      Instant.now().plusSeconds(10L)
    )

    val actual = operator
      .putForPreAssign(
        expectedItem.datacenterId,
        expectedItem.workerId,
        expectedItem.host,
        expectedItem.sessionId,
        expectedItem.sessionExpiredAt
      )
      .futureValue(Timeout(Span(1, Seconds)))

    assert(actual == PreAssignResult.Succeeded)

    val actualItem = client
      .getItem(
        GetItemRequest
          .builder()
          .tableName(tableName)
          .key(createKeyMap(expectedItem.datacenterId, expectedItem.workerId))
          .build()
      )
      .thenApply(_.item())
      .thenApply(item => WorkerIdAssignItem.decode(testIdFormat)(item))
      .thenApply(_.getOrElse(fail("Illegal item")))
      .get()

    assert(actualItem == expectedItem)
  }

  test("`putForPreAssign` can not put pre assign item when keys(PartitionKey and SortKey pair) is exists") {
    val existsItem1 = createKeyMap(testIdFormat.datacenterId(1L), testIdFormat.workerId(1L))
    client.putItem(PutItemRequest.builder().tableName(tableName).item(existsItem1).build()).get()

    val actual = operator
      .putForPreAssign(
        testIdFormat.datacenterId(1L),
        testIdFormat.workerId(1L),
        "localhost",
        UUID.randomUUID(),
        Instant.now().plusSeconds(10L)
      )
      .futureValue(Timeout(Span(1, Seconds)))

    assert(actual.isInstanceOf[PreAssignResult.AlreadyAssigned])
  }

  test("`updateForAssign` can update item of `PreAssign` status when same SessionId and does not expired") {
    val datacenterId     = testIdFormat.datacenterId(1L)
    val workerId         = testIdFormat.workerId(1L)
    val host             = "localhost"
    val sessionId        = UUID.randomUUID()
    val sessionExpiredAt = Instant.now().plusSeconds(1.minute.toSeconds)
    val preAssignItem =
      WorkerIdAssignItem(datacenterId, workerId, AssignState.PreAssign, host, sessionId, sessionExpiredAt)
    client.putItem(PutItemRequest.builder().tableName(tableName).item(encode(preAssignItem)).build()).get()

    val actual =
      operator.updateForAssign(datacenterId, workerId, sessionId, sessionExpiredAt).futureValue(1.seconds.timeout)
    assert(actual == AssignResult.Succeeded)

    val expectedItem = preAssignItem.copy(state = AssignState.Assign)
    val actualItem = client
      .getItem(GetItemRequest.builder().tableName(tableName).key(createKeyMap(datacenterId, workerId)).build())
      .thenApply(_.item())
      .thenApply(item => WorkerIdAssignItem.decode(testIdFormat)(item))
      .thenApply(_.getOrElse(fail("Illegal item")))
      .get()

    assert(actualItem == expectedItem)
  }

  test("`updateForAssign` can not update item when does not same SessionId") {
    val datacenterId     = testIdFormat.datacenterId(1L)
    val workerId         = testIdFormat.workerId(1L)
    val host             = "localhost"
    val sessionId        = UUID.randomUUID()
    val sessionExpiredAt = Instant.now().plusSeconds(1.minute.toSeconds)
    val preAssignItem =
      WorkerIdAssignItem(datacenterId, workerId, AssignState.PreAssign, host, sessionId, sessionExpiredAt)
    client.putItem(PutItemRequest.builder().tableName(tableName).item(encode(preAssignItem)).build()).get()

    val differentSessionId = UUID.randomUUID()
    operator
      .updateForAssign(datacenterId, workerId, differentSessionId, sessionExpiredAt)
      .futureValue(1.seconds.timeout) match {
      case AssignResult.Failed(_: ConditionalCheckFailedException) => succeed
      case x                                                       => fail(s"Illegal result: $x")
    }

    val expectedItem = preAssignItem
    val actualItem = client
      .getItem(GetItemRequest.builder().tableName(tableName).key(createKeyMap(datacenterId, workerId)).build())
      .thenApply(_.item())
      .thenApply(item => WorkerIdAssignItem.decode(testIdFormat)(item))
      .thenApply(_.getOrElse(fail("Illegal item")))
      .get()

    assert(actualItem == expectedItem)
  }

  test("`updateForAssign` can not update item that already expired") {
    val datacenterId     = testIdFormat.datacenterId(1L)
    val workerId         = testIdFormat.workerId(1L)
    val host             = "localhost"
    val sessionId        = UUID.randomUUID()
    val sessionExpiredAt = Instant.now().minusSeconds(1.minute.toSeconds)
    val preAssignItem =
      WorkerIdAssignItem(datacenterId, workerId, AssignState.PreAssign, host, sessionId, sessionExpiredAt)
    client.putItem(PutItemRequest.builder().tableName(tableName).item(encode(preAssignItem)).build()).get()

    operator
      .updateForAssign(datacenterId, workerId, sessionId, Instant.now().plusSeconds(60L))
      .futureValue(1.seconds.timeout) match {
      case AssignResult.Failed(_: ConditionalCheckFailedException) => succeed
      case x                                                       => fail(s"Illegal result: $x")
    }

    val expectedItem = preAssignItem
    val actualItem = client
      .getItem(GetItemRequest.builder().tableName(tableName).key(createKeyMap(datacenterId, workerId)).build())
      .thenApply(_.item())
      .thenApply(item => WorkerIdAssignItem.decode(testIdFormat)(item))
      .thenApply(_.getOrElse(fail("Illegal item")))
      .get()

    assert(actualItem == expectedItem)
  }

  test("`keepAlive` can update item's expire time when same SessionId and does not expired") {
    val datacenterId     = testIdFormat.datacenterId(1L)
    val workerId         = testIdFormat.workerId(1L)
    val host             = "localhost"
    val sessionId        = UUID.randomUUID()
    val sessionExpiredAt = Instant.now().plusSeconds(1.minute.toSeconds)
    val assignItem =
      WorkerIdAssignItem(datacenterId, workerId, AssignState.Assign, host, sessionId, sessionExpiredAt)
    client.putItem(PutItemRequest.builder().tableName(tableName).item(encode(assignItem)).build()).get()

    val newSessionExpiredAt = sessionExpiredAt.plusSeconds(30.seconds.toSeconds)
    val actual =
      operator.keepAlive(datacenterId, workerId, sessionId, newSessionExpiredAt).futureValue(1.seconds.timeout)
    assert(actual == KeepAliveResult.Succeeded)

    val expectedItem = assignItem.copy(sessionExpiredAt = newSessionExpiredAt)
    val actualItem = client
      .getItem(GetItemRequest.builder().tableName(tableName).key(createKeyMap(datacenterId, workerId)).build())
      .thenApply(_.item())
      .thenApply(item => WorkerIdAssignItem.decode(testIdFormat)(item))
      .thenApply(_.getOrElse(fail("Illegal item")))
      .get()

    assert(actualItem == expectedItem)
  }

  test("`keepAlive` can not update item's expire time when assign state is not `Assign`") {
    val datacenterId     = testIdFormat.datacenterId(1L)
    val workerId         = testIdFormat.workerId(1L)
    val host             = "localhost"
    val sessionId        = UUID.randomUUID()
    val sessionExpiredAt = Instant.now().plusSeconds(1.minute.toSeconds)
    val item =
      WorkerIdAssignItem(datacenterId, workerId, AssignState.PreAssign, host, sessionId, sessionExpiredAt)
    client.putItem(PutItemRequest.builder().tableName(tableName).item(encode(item)).build()).get()

    val newSessionExpiredAt = sessionExpiredAt.plusSeconds(30.seconds.toSeconds)
    operator.keepAlive(datacenterId, workerId, sessionId, newSessionExpiredAt).futureValue(1.seconds.timeout) match {
      case KeepAliveResult.Failed(_: ConditionalCheckFailedException) => succeed
      case x                                                          => fail(s"Illegal result: $x")
    }

    val expectedItem = item
    val actualItem = client
      .getItem(GetItemRequest.builder().tableName(tableName).key(createKeyMap(datacenterId, workerId)).build())
      .thenApply(_.item())
      .thenApply(item => WorkerIdAssignItem.decode(testIdFormat)(item))
      .thenApply(_.getOrElse(fail("Illegal item")))
      .get()

    assert(actualItem == expectedItem)
  }

  test("`keepAlive` can not update item's expire time when does not same SessionId") {
    val datacenterId     = testIdFormat.datacenterId(1L)
    val workerId         = testIdFormat.workerId(1L)
    val host             = "localhost"
    val sessionId        = UUID.randomUUID()
    val sessionExpiredAt = Instant.now().plusSeconds(1.minute.toSeconds)
    val assignItem =
      WorkerIdAssignItem(datacenterId, workerId, AssignState.Assign, host, sessionId, sessionExpiredAt)
    client.putItem(PutItemRequest.builder().tableName(tableName).item(encode(assignItem)).build()).get()

    val newSessionExpiredAt = sessionExpiredAt.plusSeconds(30.seconds.toSeconds)
    val differentSessionId  = UUID.randomUUID()
    operator
      .keepAlive(datacenterId, workerId, differentSessionId, newSessionExpiredAt)
      .futureValue(1.seconds.timeout) match {
      case KeepAliveResult.Failed(_: ConditionalCheckFailedException) => succeed
      case x                                                          => fail(s"Illegal result: $x")
    }

    val expectedItem = assignItem
    val actualItem = client
      .getItem(GetItemRequest.builder().tableName(tableName).key(createKeyMap(datacenterId, workerId)).build())
      .thenApply(_.item())
      .thenApply(item => WorkerIdAssignItem.decode(testIdFormat)(item))
      .thenApply(_.getOrElse(fail("Illegal item")))
      .get()

    assert(actualItem == expectedItem)
  }

  test("`keepAlive` can not update item's expire time when already expired") {
    val datacenterId     = testIdFormat.datacenterId(1L)
    val workerId         = testIdFormat.workerId(1L)
    val host             = "localhost"
    val sessionId        = UUID.randomUUID()
    val sessionExpiredAt = Instant.now().minusSeconds(1.minute.toSeconds)
    val assignItem =
      WorkerIdAssignItem(datacenterId, workerId, AssignState.Assign, host, sessionId, sessionExpiredAt)
    client.putItem(PutItemRequest.builder().tableName(tableName).item(encode(assignItem)).build()).get()

    val newSessionExpiredAt = sessionExpiredAt.plusSeconds(30.seconds.toSeconds)
    operator.keepAlive(datacenterId, workerId, sessionId, newSessionExpiredAt).futureValue(1.seconds.timeout) match {
      case KeepAliveResult.Failed(_: ConditionalCheckFailedException) => succeed
      case x                                                          => fail(s"Illegal result: $x")
    }

    val expectedItem = assignItem
    val actualItem = client
      .getItem(GetItemRequest.builder().tableName(tableName).key(createKeyMap(datacenterId, workerId)).build())
      .thenApply(_.item())
      .thenApply(item => WorkerIdAssignItem.decode(testIdFormat)(item))
      .thenApply(_.getOrElse(fail("Illegal item")))
      .get()

    assert(actualItem == expectedItem)
  }

}
