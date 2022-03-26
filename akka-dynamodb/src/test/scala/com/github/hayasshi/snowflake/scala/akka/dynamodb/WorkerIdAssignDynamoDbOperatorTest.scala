package com.github.hayasshi.snowflake.scala.akka.dynamodb

import com.github.hayasshi.snowflake.scala.core.IdFormat
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{ Millis, Seconds, Span }
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.*

import java.time.Instant
import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{ DurationInt, FiniteDuration }

class WorkerIdAssignDynamoDbOperatorTest extends AnyFunSuite with ScalaFutures with BeforeAndAfterAll {
  import com.github.hayasshi.snowflake.scala.akka.dynamodb.WorkerIdAssignItem.*

  implicit class FiniteDurationToScalatestTimeout(duration: FiniteDuration) {
    def timeout: Timeout = Timeout(Span(duration.toMillis, Millis))
  }

  val dynamoDbSupport                     = new DynamoDbLocalContainerSupport()
  val dynamoDbClient: DynamoDbAsyncClient = dynamoDbSupport.client

  override def afterAll(): Unit = {
    dynamoDbSupport.close()
    super.afterAll()
  }

  val tableName = "TestTable"
  dynamoDbClient.createTable(createTableRequest(tableName, BillingMode.PAY_PER_REQUEST, None)).get()

  val operator = new WorkerIdAssignDynamoDbOperator(dynamoDbClient, tableName)

  val testIdFormat: IdFormat = IdFormat.Default(Instant.EPOCH)

  test("`putForPreAssign` can put pre assign item when keys(PartitionKey and SortKey pair) is not exists") {
    val existsItem1 = createKeyMap(testIdFormat.datacenterId(1L), testIdFormat.workerId(2L))
    val existsItem2 = createKeyMap(testIdFormat.datacenterId(2L), testIdFormat.workerId(1L))
    dynamoDbClient.putItem(PutItemRequest.builder().tableName(tableName).item(existsItem1).build()).get()
    dynamoDbClient.putItem(PutItemRequest.builder().tableName(tableName).item(existsItem2).build()).get()

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

    val actualItem = dynamoDbClient
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
    dynamoDbClient.putItem(PutItemRequest.builder().tableName(tableName).item(existsItem1).build()).get()

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
    dynamoDbClient.putItem(PutItemRequest.builder().tableName(tableName).item(encode(preAssignItem)).build()).get()

    val actual =
      operator.updateForAssign(datacenterId, workerId, sessionId, sessionExpiredAt).futureValue(1.seconds.timeout)
    assert(actual == AssignResult.Succeeded)

    val expectedItem = preAssignItem.copy(state = AssignState.Assign)
    val actualItem = dynamoDbClient
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
    dynamoDbClient.putItem(PutItemRequest.builder().tableName(tableName).item(encode(preAssignItem)).build()).get()

    val differentSessionId = UUID.randomUUID()
    operator
      .updateForAssign(datacenterId, workerId, differentSessionId, sessionExpiredAt)
      .futureValue(1.seconds.timeout) match {
      case AssignResult.Failed(_: ConditionalCheckFailedException) => succeed
      case x                                                       => fail(s"Illegal result: $x")
    }

    val expectedItem = preAssignItem
    val actualItem = dynamoDbClient
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
    dynamoDbClient.putItem(PutItemRequest.builder().tableName(tableName).item(encode(preAssignItem)).build()).get()

    operator
      .updateForAssign(datacenterId, workerId, sessionId, Instant.now().plusSeconds(60L))
      .futureValue(1.seconds.timeout) match {
      case AssignResult.Failed(_: ConditionalCheckFailedException) => succeed
      case x                                                       => fail(s"Illegal result: $x")
    }

    val expectedItem = preAssignItem
    val actualItem = dynamoDbClient
      .getItem(GetItemRequest.builder().tableName(tableName).key(createKeyMap(datacenterId, workerId)).build())
      .thenApply(_.item())
      .thenApply(item => WorkerIdAssignItem.decode(testIdFormat)(item))
      .thenApply(_.getOrElse(fail("Illegal item")))
      .get()

    assert(actualItem == expectedItem)
  }

  test("`heartbeat` can update item's expire time when same SessionId and does not expired") {
    val datacenterId     = testIdFormat.datacenterId(1L)
    val workerId         = testIdFormat.workerId(1L)
    val host             = "localhost"
    val sessionId        = UUID.randomUUID()
    val sessionExpiredAt = Instant.now().plusSeconds(1.minute.toSeconds)
    val assignItem =
      WorkerIdAssignItem(datacenterId, workerId, AssignState.Assign, host, sessionId, sessionExpiredAt)
    dynamoDbClient.putItem(PutItemRequest.builder().tableName(tableName).item(encode(assignItem)).build()).get()

    val newSessionExpiredAt = sessionExpiredAt.plusSeconds(30.seconds.toSeconds)
    val actual =
      operator.heartbeat(datacenterId, workerId, sessionId, newSessionExpiredAt).futureValue(1.seconds.timeout)
    assert(actual == HeartbeatResult.Succeeded)

    val expectedItem = assignItem.copy(sessionExpiredAt = newSessionExpiredAt)
    val actualItem = dynamoDbClient
      .getItem(GetItemRequest.builder().tableName(tableName).key(createKeyMap(datacenterId, workerId)).build())
      .thenApply(_.item())
      .thenApply(item => WorkerIdAssignItem.decode(testIdFormat)(item))
      .thenApply(_.getOrElse(fail("Illegal item")))
      .get()

    assert(actualItem == expectedItem)
  }

  test("`heartbeat` can not update item's expire time when assign state is not `Assign`") {
    val datacenterId     = testIdFormat.datacenterId(1L)
    val workerId         = testIdFormat.workerId(1L)
    val host             = "localhost"
    val sessionId        = UUID.randomUUID()
    val sessionExpiredAt = Instant.now().plusSeconds(1.minute.toSeconds)
    val item =
      WorkerIdAssignItem(datacenterId, workerId, AssignState.PreAssign, host, sessionId, sessionExpiredAt)
    dynamoDbClient.putItem(PutItemRequest.builder().tableName(tableName).item(encode(item)).build()).get()

    val newSessionExpiredAt = sessionExpiredAt.plusSeconds(30.seconds.toSeconds)
    operator.heartbeat(datacenterId, workerId, sessionId, newSessionExpiredAt).futureValue(1.seconds.timeout) match {
      case HeartbeatResult.Failed(_: ConditionalCheckFailedException) => succeed
      case x                                                          => fail(s"Illegal result: $x")
    }

    val expectedItem = item
    val actualItem = dynamoDbClient
      .getItem(GetItemRequest.builder().tableName(tableName).key(createKeyMap(datacenterId, workerId)).build())
      .thenApply(_.item())
      .thenApply(item => WorkerIdAssignItem.decode(testIdFormat)(item))
      .thenApply(_.getOrElse(fail("Illegal item")))
      .get()

    assert(actualItem == expectedItem)
  }

  test("`heartbeat` can not update item's expire time when does not same SessionId") {
    val datacenterId     = testIdFormat.datacenterId(1L)
    val workerId         = testIdFormat.workerId(1L)
    val host             = "localhost"
    val sessionId        = UUID.randomUUID()
    val sessionExpiredAt = Instant.now().plusSeconds(1.minute.toSeconds)
    val assignItem =
      WorkerIdAssignItem(datacenterId, workerId, AssignState.Assign, host, sessionId, sessionExpiredAt)
    dynamoDbClient.putItem(PutItemRequest.builder().tableName(tableName).item(encode(assignItem)).build()).get()

    val newSessionExpiredAt = sessionExpiredAt.plusSeconds(30.seconds.toSeconds)
    val differentSessionId  = UUID.randomUUID()
    operator
      .heartbeat(datacenterId, workerId, differentSessionId, newSessionExpiredAt)
      .futureValue(1.seconds.timeout) match {
      case HeartbeatResult.Failed(_: ConditionalCheckFailedException) => succeed
      case x                                                          => fail(s"Illegal result: $x")
    }

    val expectedItem = assignItem
    val actualItem = dynamoDbClient
      .getItem(GetItemRequest.builder().tableName(tableName).key(createKeyMap(datacenterId, workerId)).build())
      .thenApply(_.item())
      .thenApply(item => WorkerIdAssignItem.decode(testIdFormat)(item))
      .thenApply(_.getOrElse(fail("Illegal item")))
      .get()

    assert(actualItem == expectedItem)
  }

  test("`heartbeat` can not update item's expire time when already expired") {
    val datacenterId     = testIdFormat.datacenterId(1L)
    val workerId         = testIdFormat.workerId(1L)
    val host             = "localhost"
    val sessionId        = UUID.randomUUID()
    val sessionExpiredAt = Instant.now().minusSeconds(1.minute.toSeconds)
    val assignItem =
      WorkerIdAssignItem(datacenterId, workerId, AssignState.Assign, host, sessionId, sessionExpiredAt)
    dynamoDbClient.putItem(PutItemRequest.builder().tableName(tableName).item(encode(assignItem)).build()).get()

    val newSessionExpiredAt = sessionExpiredAt.plusSeconds(30.seconds.toSeconds)
    operator.heartbeat(datacenterId, workerId, sessionId, newSessionExpiredAt).futureValue(1.seconds.timeout) match {
      case HeartbeatResult.Failed(_: ConditionalCheckFailedException) => succeed
      case x                                                          => fail(s"Illegal result: $x")
    }

    val expectedItem = assignItem
    val actualItem = dynamoDbClient
      .getItem(GetItemRequest.builder().tableName(tableName).key(createKeyMap(datacenterId, workerId)).build())
      .thenApply(_.item())
      .thenApply(item => WorkerIdAssignItem.decode(testIdFormat)(item))
      .thenApply(_.getOrElse(fail("Illegal item")))
      .get()

    assert(actualItem == expectedItem)
  }

}
