package com.github.hayasshi.snowflake.scala.akka.dynamodb

import akka.actor.ActorSystem
import akka.testkit.{ ImplicitSender, TestKit }
import com.github.hayasshi.snowflake.scala.core.IdFormat
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funsuite.AnyFunSuiteLike
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

  val dynamoDbSupport = new DynamoDbLocalContainerSupport()

  override def afterAll(): Unit = {
    dynamoDbSupport.close()
    super.afterAll()
  }

  val tableName = "TestTable"
  dynamoDbSupport.client.createTable(createTableRequest(tableName, BillingMode.PAY_PER_REQUEST, None)).get()

  val operator = new WorkerIdAssignDynamoDbOperator(dynamoDbSupport.client, tableName)

  val testIdFormat: IdFormat = IdFormat(
    datacenterIdBits = 7,
    workerIdBits = 3,
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
    val assigner = system.actorOf(WorkerIdAssigner.props(settings), s"DcId-${datacenterId.value}")
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

  test("WorkerIdAssigner can assign WorkerId for the number of WorkerIdBits without duplication") {
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

    val numOfAssigner = 1 << settings.idFormat.workerIdBits
    val assigners = (0 until numOfAssigner).map(i =>
      system.actorOf(WorkerIdAssigner.props(settings), s"DcId-${datacenterId.value}-$i")
    )
    Thread.sleep(3000) // initial sleep

    awaitAssert(
      {
        val actualWorkerIds = assigners.map { ref =>
          ref ! GetWorkerId
          expectMsgType[Ready].workerId
        }.toSet
        assert(actualWorkerIds.size == numOfAssigner)
      },
      10.seconds,
      1.second
    )

  }

}
