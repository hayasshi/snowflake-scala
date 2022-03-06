package com.github.hayasshi.snowflake.scala.akka.dynamodb

import com.github.hayasshi.snowflake.scala.akka.dynamodb.WorkerIdAssignDynamoDbOperator.unwrapIfCompletionException
import com.github.hayasshi.snowflake.scala.core.IdFormat
import com.github.hayasshi.snowflake.scala.core.IdFormat.{ DatacenterId, WorkerId }
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.*

import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletionException
import scala.concurrent.{ ExecutionContext, Future }
import scala.jdk.CollectionConverters.{ MapHasAsJava, MapHasAsScala }
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

case class DecodingFailure(message: String, cause: Option[Throwable] = None) extends Exception(message, cause.orNull)

case class WorkerIdAssignItem(
    datacenterId: DatacenterId,
    workerId: WorkerId,
    state: AssignState,
    host: String,
    sessionId: UUID,
    sessionExpiredAt: Instant
)

object WorkerIdAssignItem {
  val PartitionKeyName         = "DatacenterId"
  val SortKeyName              = "WorkerId"
  val AttrStateName            = "AssignState"
  val AttrHostName             = "Host"
  val AttrSessionIdName        = "SessionId"
  val AttrSessionExpiredAtName = "SessionExpiredAt"

  def createKeyMap(datacenterId: DatacenterId, workerId: WorkerId): java.util.Map[String, AttributeValue] =
    Map(
      PartitionKeyName -> AttributeValue.builder().n(datacenterId.value.toString).build(),
      SortKeyName      -> AttributeValue.builder().n(workerId.value.toString).build()
    ).asJava

  def encode(item: WorkerIdAssignItem): java.util.Map[String, AttributeValue] = Map(
    PartitionKeyName         -> AttributeValue.builder().n(item.datacenterId.value.toString).build(),
    SortKeyName              -> AttributeValue.builder().n(item.workerId.value.toString).build(),
    AttrStateName            -> AttributeValue.builder().s(item.state.asString).build(),
    AttrHostName             -> AttributeValue.builder().s(item.host).build(),
    AttrSessionIdName        -> AttributeValue.builder().s(item.sessionId.toString).build(),
    AttrSessionExpiredAtName -> AttributeValue.builder().n(item.sessionExpiredAt.toEpochMilli.toString).build()
  ).asJava

  def decode(
      idFormat: IdFormat
  )(itemMap: java.util.Map[String, AttributeValue]): Either[DecodingFailure, WorkerIdAssignItem] = {
    val map = itemMap.asScala
    val result = for {
      datacenterId <- map.get(PartitionKeyName).toRight(DecodingFailure(s"`$PartitionKeyName` is not exists."))
      workerId     <- map.get(SortKeyName).toRight(DecodingFailure(s"`$SortKeyName` is not exists."))
      state        <- map.get(AttrStateName).toRight(DecodingFailure(s"`$AttrStateName` is not exists."))
      host         <- map.get(AttrHostName).toRight(DecodingFailure(s"`$AttrHostName` is not exists."))
      sessionId    <- map.get(AttrSessionIdName).toRight(DecodingFailure(s"`$AttrSessionIdName` is not exists."))
      sessionExpiredAt <- map
        .get(AttrSessionExpiredAtName)
        .toRight(DecodingFailure(s"`$AttrSessionExpiredAtName` is not exists."))
    } yield {
      try {
        Right(
          WorkerIdAssignItem(
            idFormat.datacenterId(datacenterId.n().toLong),
            idFormat.workerId(workerId.n().toLong),
            AssignState.from(state.s()),
            host.s(),
            UUID.fromString(sessionId.s()),
            Instant.ofEpochMilli(sessionExpiredAt.n().toLong)
          )
        )
      } catch {
        case NonFatal(e) => Left(DecodingFailure(e.getMessage, Some(e)))
      }
    }
    result.flatten
  }
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
