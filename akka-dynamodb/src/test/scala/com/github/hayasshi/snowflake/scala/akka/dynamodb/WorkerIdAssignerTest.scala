package com.github.hayasshi.snowflake.scala.akka.dynamodb

import akka.actor.ActorSystem
import akka.testkit.{ ImplicitSender, TestKit }
import com.github.hayasshi.snowflake.scala.core.IdFormat
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.time.{ Millis, Span }
import software.amazon.awssdk.services.dynamodb.model.*

import java.time.Instant
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{ DurationInt, FiniteDuration }

class WorkerIdAssignerTest
    extends TestKit(ActorSystem("WorkerIdAssignerTest"))
    with ImplicitSender
    with AnyFunSuiteLike
    with ScalaFutures
    with BeforeAndAfterAll {
  import com.github.hayasshi.snowflake.scala.akka.dynamodb.WorkerIdAssignItem.*

  implicit class FiniteDurationToScalatestTimeout(duration: FiniteDuration) {
    def timeout: Timeout = Timeout(Span(duration.toMillis, Millis))
  }

  val dynamoDbSupport = new DynamoDbLocalContainerSupport()

  override def afterAll(): Unit = {
    dynamoDbSupport.close()
    super.afterAll()
  }

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
    println(dynamoDbSupport.client.createTable(request).get())
  }

  val operator = new WorkerIdAssignDynamoDbOperator(dynamoDbSupport.client, tableName)

  val testIdFormat: IdFormat = IdFormat(
    datacenterIdBits = 8,
    workerIdBits = 2,
    sequenceNumberBits = 12,
    baseEpoch = Instant.EPOCH
  )

  val datacenterIdValue                     = new AtomicLong(0)
  val testSessionDuration: FiniteDuration   = 1.minute
  val testSafeWaitDuration: FiniteDuration  = 1.second
  val testHeartbeatInterval: FiniteDuration = 300.millis

  import WorkerIdAssigner.Protocol.External.*

  test("WorkerIdAssigner become ready status, it is able to get assigned WorkerId") {
    val datacenterId = testIdFormat.datacenterId(datacenterIdValue.getAndIncrement())
    val settings = WorkerIdAssignerSettings(
      idFormat = testIdFormat,
      datacenterId = datacenterId,
      operator = operator,
      host = "localhost",
      sessionDuration = testSessionDuration,
      safeWaitDuration = testSafeWaitDuration,
      heartbeatInterval = testHeartbeatInterval
    )
    val assigner = system.actorOf(WorkerIdAssigner.props(settings), "test1")
    Thread.sleep(3000) // initial sleep

    awaitAssert(
      {
        assigner ! GetWorkerId
        expectMsgType[Ready]
      },
      5.seconds,
      1.second
    )

  }

}
