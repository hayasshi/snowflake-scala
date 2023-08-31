package com.github.hayasshi.snowflake.scala.akka.dynamodb

import com.github.hayasshi.snowflake.scala.core.IdFormat
import com.github.hayasshi.snowflake.scala.core.IdFormat.{ DatacenterId, WorkerId }
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeDefinition,
  AttributeValue,
  BillingMode,
  CreateTableRequest,
  KeySchemaElement,
  KeyType,
  ProvisionedThroughput,
  ScalarAttributeType
}

import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

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

  def createTableRequest(
      tableName: String,
      billingMode: BillingMode,
      provisionedThroughput: Option[ProvisionedThroughput]
  ): CreateTableRequest = {
    val builder = CreateTableRequest
      .builder()
      .tableName(tableName)
      .billingMode(billingMode)
      .attributeDefinitions(
        AttributeDefinition.builder().attributeType(ScalarAttributeType.N).attributeName(PartitionKeyName).build(),
        AttributeDefinition.builder().attributeType(ScalarAttributeType.N).attributeName(SortKeyName).build()
      )
      .keySchema(
        KeySchemaElement.builder().keyType(KeyType.HASH).attributeName(PartitionKeyName).build(),
        KeySchemaElement.builder().keyType(KeyType.RANGE).attributeName(SortKeyName).build()
      )

    (billingMode, provisionedThroughput) match {
      case (BillingMode.PAY_PER_REQUEST, _) =>
        builder.build()
      case (BillingMode.PROVISIONED, Some(pt)) =>
        builder.provisionedThroughput(pt).build()
      case _ =>
        throw new IllegalArgumentException(s"Illegal combination: $billingMode and $provisionedThroughput")
    }
  }

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
