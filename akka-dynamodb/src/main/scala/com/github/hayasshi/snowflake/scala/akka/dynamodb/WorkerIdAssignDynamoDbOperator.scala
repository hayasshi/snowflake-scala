package com.github.hayasshi.snowflake.scala.akka.dynamodb

import com.github.hayasshi.snowflake.scala.akka.dynamodb.WorkerIdAssignDynamoDbOperator.unwrapIfCompletionException
import com.github.hayasshi.snowflake.scala.core.IdFormat.{ DatacenterId, WorkerId }
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.*

import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletionException
import scala.concurrent.{ ExecutionContext, Future }
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.control.NonFatal

sealed trait AssignState { def asString: String }
object AssignState {
  case object PreAssign extends AssignState { val asString: String = "PreAssign" }
  case object Assign    extends AssignState { val asString: String = "Assign"    }

  def from(s: String): AssignState = s match {
    case PreAssign.asString => PreAssign
    case Assign.asString    => Assign
    case _                  => throw new IllegalArgumentException(s"`$s` is not value of AssignState")
  }
}

sealed trait PreAssignResult
object PreAssignResult {
  case object Succeeded                                              extends PreAssignResult
  case class Failed(cause: Throwable)                                extends PreAssignResult
  case class AlreadyAssigned(cause: ConditionalCheckFailedException) extends PreAssignResult
}

sealed trait AssignResult
object AssignResult {
  case object Succeeded               extends AssignResult
  case class Failed(cause: Throwable) extends AssignResult
}

sealed trait HeartbeatResult
object HeartbeatResult {
  case object Succeeded               extends HeartbeatResult
  case class Failed(cause: Throwable) extends HeartbeatResult
}

object WorkerIdAssignDynamoDbOperator {
  def unwrapIfCompletionException(t: Throwable): Throwable = t match {
    case e: CompletionException => e.getCause
    case NonFatal(e)            => e
  }
}

class WorkerIdAssignDynamoDbOperator(val client: DynamoDbAsyncClient, val tableName: String)(implicit
    ec: ExecutionContext
) {
  import WorkerIdAssignItem.*

  def putForPreAssign(
      datacenterId: DatacenterId,
      workerId: WorkerId,
      host: String,
      sessionId: UUID,
      sessionExpiredAt: Instant
  ): Future[PreAssignResult] = {
    val item = WorkerIdAssignItem(
      datacenterId,
      workerId,
      AssignState.PreAssign,
      host,
      sessionId,
      sessionExpiredAt
    )
    val request = PutItemRequest
      .builder()
      .tableName(tableName)
      .item(encode(item))
      .conditionExpression(s"attribute_not_exists($PartitionKeyName) and attribute_not_exists($SortKeyName)")
      .build()
    client
      .putItem(request)
      .asScala
      .map(_ => PreAssignResult.Succeeded)
      .recover {
        unwrapIfCompletionException(_) match {
          case e: ConditionalCheckFailedException => PreAssignResult.AlreadyAssigned(e)
          case e                                  => PreAssignResult.Failed(e)
        }
      }
  }

  def updateForAssign(
      datacenterId: DatacenterId,
      workerId: WorkerId,
      sessionId: UUID,
      sessionExpiredAt: Instant
  ): Future[AssignResult] = {
    val request = UpdateItemRequest
      .builder()
      .tableName(tableName)
      .key(createKeyMap(datacenterId, workerId))
      .updateExpression(s"SET $AttrStateName = :newState, $AttrSessionExpiredAtName = :newSessionExpiredAt")
      .conditionExpression(
        s"$AttrStateName = :state AND $AttrSessionIdName = :sessionId AND $AttrSessionExpiredAtName > :now"
      )
      .expressionAttributeValues(
        Map(
          ":newState"            -> AttributeValue.builder().s(AssignState.Assign.asString).build(),
          ":newSessionExpiredAt" -> AttributeValue.builder().n(sessionExpiredAt.toEpochMilli.toString).build(),
          ":state"               -> AttributeValue.builder().s(AssignState.PreAssign.asString).build(),
          ":sessionId"           -> AttributeValue.builder().s(sessionId.toString).build(),
          ":now"                 -> AttributeValue.builder().n(Instant.now().toEpochMilli.toString).build()
        ).asJava
      )
      .build()
    client
      .updateItem(request)
      .asScala
      .map(_ => AssignResult.Succeeded)
      .recover(e => AssignResult.Failed(unwrapIfCompletionException(e)))
  }

  def heartbeat(
      datacenterId: DatacenterId,
      workerId: WorkerId,
      sessionId: UUID,
      sessionExpiredAt: Instant
  ): Future[HeartbeatResult] = {
    val request = UpdateItemRequest
      .builder()
      .tableName(tableName)
      .key(createKeyMap(datacenterId, workerId))
      .updateExpression(s"SET $AttrSessionExpiredAtName = :newSessionExpiredAt")
      .conditionExpression(
        s"$AttrStateName = :state AND $AttrSessionIdName = :sessionId AND $AttrSessionExpiredAtName > :now"
      )
      .expressionAttributeValues(
        Map(
          ":newSessionExpiredAt" -> AttributeValue.builder().n(sessionExpiredAt.toEpochMilli.toString).build(),
          ":state"               -> AttributeValue.builder().s(AssignState.Assign.asString).build(),
          ":sessionId"           -> AttributeValue.builder().s(sessionId.toString).build(),
          ":now"                 -> AttributeValue.builder().n(Instant.now().toEpochMilli.toString).build()
        ).asJava
      )
      .build()
    client
      .updateItem(request)
      .asScala
      .map(_ => HeartbeatResult.Succeeded)
      .recover(e => HeartbeatResult.Failed(unwrapIfCompletionException(e)))
  }

}
