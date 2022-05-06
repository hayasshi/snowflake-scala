package com.github.hayasshi.snowflake.scala.akka.dynamodb

import akka.actor.Actor
import com.github.hayasshi.snowflake.scala.core.{ IdFormat, IdWorkerModule }
import com.github.hayasshi.snowflake.scala.core.IdFormat.{ DatacenterId, WorkerId }
import com.github.hayasshi.snowflake.scala.core.IdWorkerModule.IdGenerationFailed

import java.time.Instant

object IdWorkerActor {

  case object Request

  sealed trait Result
  case class Generated(id: Long)                extends Result
  case class Failed(reason: IdGenerationFailed) extends Result

  private def timeGen(): Long = Instant.now().toEpochMilli
}

class IdWorkerActor(val idFormat: IdFormat, val datacenterId: DatacenterId, val workerId: WorkerId)
    extends Actor
    with IdWorkerModule {
  import IdWorkerActor.*

  var lastTimestamp: Long  = timeGen()
  var lastSequenceId: Long = 0L

  override def receive: Receive = { case Request =>
    nextId(timeGen(), lastTimestamp, lastSequenceId) match {
      case Left(failed) =>
        sender() ! Failed(failed)
      case Right(snowflake) =>
        lastTimestamp = snowflake.lastTimestamp
        lastSequenceId = snowflake.lastSequence
        sender() ! Generated(snowflake.id)
    }
  }

}
