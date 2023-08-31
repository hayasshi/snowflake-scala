package com.github.hayasshi.snowflake.scala.akka.dynamodb

import akka.actor.{ Actor, ActorLogging, Props }
import akka.pattern.pipe
import com.github.hayasshi.snowflake.scala.core.IdFormat
import com.github.hayasshi.snowflake.scala.core.IdFormat.{ DatacenterId, WorkerId }

import java.time.Instant
import java.util.UUID
import scala.concurrent.ExecutionContextExecutor
import scala.util.Random

object WorkerIdAssigner {

  object Protocol {

    private[dynamodb] object Internal {
      case class PreAssign(id: WorkerId)
      case object SucceededPreAssign
      case class FailedPreAssign(t: Throwable)

      case object Assign
      case object SucceededAssign
      case class FailedAssign(t: Throwable)

      case object Heartbeat
      case object SucceededHeartbeat
      case class FailedHeartbeat(t: Throwable)
    }

    object External {
      case object GetWorkerId
      trait GetWorkerIdResponse
      case class Ready(workerId: WorkerId) extends GetWorkerIdResponse
      case object NotInitialized           extends GetWorkerIdResponse
    }

  }

  def props(
      idFormat: IdFormat,
      datacenterId: DatacenterId,
      operator: WorkerIdAssignDynamoDbOperator,
      settings: WorkerIdAssignerSettings
  ): Props =
    Props(new WorkerIdAssigner(idFormat, datacenterId, operator, settings))

}

final class WorkerIdAssigner private (
    idFormat: IdFormat,
    datacenterId: DatacenterId,
    operator: WorkerIdAssignDynamoDbOperator,
    settings: WorkerIdAssignerSettings
) extends Actor
    with ActorLogging {

  import WorkerIdAssigner.Protocol.External.*
  import WorkerIdAssigner.Protocol.Internal.*

  val workerId: WorkerId = {
    // TODO: Check and set the id that is not assigned to DynamoDB
    val id = Random.nextLong(0x0001L << idFormat.workerIdBits)
    idFormat.workerId(id)
  }
  context.self ! PreAssign(workerId)

  val sessionId: UUID = UUID.randomUUID()

  implicit val executionContext: ExecutionContextExecutor = context.dispatcher

  def createSessionExpiredAt(): Instant = {
    Instant.now().plusMillis(settings.sessionDuration.toMillis)
  }

  override def postStop(): Unit = {
    // TODO: Delete assignment data on a best-effort basis
  }

  override def receive: Receive = preAssign

  def preAssign: Receive = {
    case PreAssign(id) =>
      log.info("Start to pre-assign at {}", id)
      operator
        .putForPreAssign(
          datacenterId,
          id,
          settings.host,
          sessionId,
          createSessionExpiredAt()
        )
        .map {
          case PreAssignResult.Succeeded          => SucceededPreAssign
          case PreAssignResult.AlreadyAssigned(e) => FailedPreAssign(e)
          case PreAssignResult.Failed(e)          => FailedPreAssign(e)
        }
        .recover(FailedPreAssign(_))
        .pipeTo(self)

    case FailedPreAssign(e) =>
      log.error(e, "Failed to create WorkerId pre-assignment data")
      throw e

    case SucceededPreAssign =>
      log.info("Succeeded to pre-assign at {}, and wait {} for safe assign.", workerId, settings.safeWaitDuration)
      context.system.scheduler.scheduleOnce(settings.safeWaitDuration, self, Assign)
      context.become(assign)

    case GetWorkerId =>
      sender() ! NotInitialized
  }

  def assign: Receive = {
    case Assign =>
      log.info("Start to assign at {}", workerId)
      operator
        .updateForAssign(datacenterId, workerId, sessionId, createSessionExpiredAt())
        .map {
          case AssignResult.Succeeded => SucceededAssign
          case AssignResult.Failed(e) => FailedAssign(e)
        }
        .recover(FailedAssign(_))
        .pipeTo(self)

    case FailedAssign(e) =>
      log.error(e, "Failed to update WorkerId assignment data from pre-assignment to assignment")
      throw e

    case SucceededAssign =>
      log.info("Succeeded to assign at {}, and heartbeat start.", workerId)
      context.system.scheduler.scheduleOnce(settings.heartbeatInterval, self, Heartbeat) // schedule initial heartbeat
      context.become(ready)

    case GetWorkerId =>
      sender() ! NotInitialized
  }

  def ready: Receive = {
    case Heartbeat =>
      operator
        .heartbeat(datacenterId, workerId, sessionId, createSessionExpiredAt())
        .map {
          case HeartbeatResult.Succeeded => SucceededHeartbeat
          case HeartbeatResult.Failed(e) => FailedHeartbeat(e)
        }
        .recover(FailedHeartbeat(_))
        .pipeTo(self)

    case FailedHeartbeat(e) =>
      log.error(e, "Failed to heartbeat for WorkerId assignment")
      throw e

    case SucceededHeartbeat =>
      context.system.scheduler.scheduleOnce(settings.heartbeatInterval, self, Heartbeat)

    case GetWorkerId =>
      sender() ! Ready(workerId)
  }

}
