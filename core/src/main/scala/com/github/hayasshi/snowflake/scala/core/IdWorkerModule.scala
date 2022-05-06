package com.github.hayasshi.snowflake.scala.core

import com.github.hayasshi.snowflake.scala.core.IdFormat.{ DatacenterId, WorkerId }

trait IdWorkerModule {
  import IdWorkerModule.*

  def idFormat: IdFormat
  def datacenterId: DatacenterId
  def workerId: WorkerId

  @inline def epochMillis: Long      = idFormat.baseEpoch.toEpochMilli
  @inline def maxSequenceId: Long    = (1L << idFormat.sequenceNumberBits) - 1
  @inline def workerIdShift: Int     = idFormat.sequenceNumberBits
  @inline def datacenterIdShift: Int = workerIdShift + idFormat.workerIdBits
  @inline def timestampShift: Int    = datacenterIdShift + idFormat.datacenterIdBits

  def nextId(
      timestamp: Long,
      lastTimestamp: Long,
      lastSequence: Long
  ): Either[IdGenerationFailed, Snowflake] = {
    val sequence = lastSequence + 1
    checkTimeAndSequence(timestamp, lastTimestamp, sequence).map { case (timestamp, sequence) =>
      val idValue =
        (timestamp - epochMillis) << timestampShift | (datacenterId.value << datacenterIdShift) | (workerId.value << workerIdShift) | sequence
      Snowflake(idValue, timestamp, sequence)
    }
  }

  @inline private def checkTimeAndSequence(
      timestamp: Long,
      lastTimestamp: Long,
      sequence: Long
  ): Either[IdGenerationFailed, (Long, Long)] = {
    if (timestamp < lastTimestamp)
      Left(InvalidSystemClock(lastTimestamp, timestamp))
    else if (timestamp > lastTimestamp)
      Right(timestamp -> 0L)
    else {
      if (sequence > maxSequenceId) Left(SequenceOverflow(sequence, maxSequenceId))
      else Right(timestamp -> sequence)
    }
  }

}

object IdWorkerModule {
  case class Snowflake(id: Long, lastTimestamp: Long, lastSequence: Long)

  sealed abstract class IdGenerationFailed(message: String) extends Exception(message)
  case class InvalidSystemClock(lastTimestamp: Long, timestamp: Long)
      extends IdGenerationFailed(
        s"Clock moved backwards. Refusing to generate id for ${lastTimestamp - timestamp} milliseconds"
      )
  case class SequenceOverflow(sequenceId: Long, maxSequenceId: Long)
      extends IdGenerationFailed(
        s"Sequence overflowed with a maximum value of $maxSequenceId and the specified $sequenceId"
      )
}
