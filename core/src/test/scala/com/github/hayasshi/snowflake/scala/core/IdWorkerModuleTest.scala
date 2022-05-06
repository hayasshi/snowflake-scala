package com.github.hayasshi.snowflake.scala.core

import com.github.hayasshi.snowflake.scala.core.IdFormat.{ DatacenterId, WorkerId }
import com.github.hayasshi.snowflake.scala.core.IdWorkerModule.{ InvalidSystemClock, SequenceOverflow }
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant

class IdWorkerModuleTest extends AnyFunSuite {
  self =>

  val idFormat: IdFormat = IdFormat.Default(Instant.EPOCH)
  val idWorker: IdWorkerModule = new IdWorkerModule {
    override def idFormat: IdFormat         = self.idFormat
    override def datacenterId: DatacenterId = idFormat.datacenterId(0L)
    override def workerId: WorkerId         = idFormat.workerId(0L)
  }

  test("`nextId` must return `InvalidSystemClock` if the clock moved backwards") {
    val timestamp     = 0L
    val lastTimestamp = 1L
    val lastSequence  = 0L
    idWorker.nextId(timestamp, lastTimestamp, lastSequence) match {
      case Left(_: InvalidSystemClock) => succeed
      case Left(e)                     => fail(s"Invalid id generation failure: $e")
      case Right(value)                => fail(s"Invalid id generation: $value")
    }
  }

  test("`nextId` must return `SequenceOverflow` if the timestamps are equal and sequence is the maximum value") {
    val timestamp     = 1L
    val lastTimestamp = 1L
    val lastSequence  = idWorker.maxSequenceId
    idWorker.nextId(timestamp, lastTimestamp, lastSequence) match {
      case Left(_: SequenceOverflow) => succeed
      case Left(e)                   => fail(s"Invalid id generation failure: $e")
      case Right(value)              => fail(s"Invalid id generation: $value")
    }
  }

  test("`nextId` must return `Snowflake` with the sequence is the next value if the timestamps are equal") {
    val timestamp     = 1L
    val lastTimestamp = 1L
    val lastSequence  = 0L
    idWorker.nextId(timestamp, lastTimestamp, lastSequence) match {
      case Left(e) => fail(s"Invalid id generation failure: $e")
      case Right(actual) =>
        val expectedSequence = lastSequence + 1
        val expectedId =
          (timestamp << idFormat.datacenterIdBits << idFormat.workerIdBits << idFormat.sequenceNumberBits) + expectedSequence
        assert(actual.id == expectedId)
        assert(actual.lastTimestamp == timestamp)
        assert(actual.lastSequence == expectedSequence)
    }
  }

  test(
    "`nextId` must return `Snowflake` with the sequence is the zero value if the timestamp is grater than tha last timestamp"
  ) {
    val timestamp     = 2L
    val lastTimestamp = 1L
    val lastSequence  = idWorker.maxSequenceId
    idWorker.nextId(timestamp, lastTimestamp, lastSequence) match {
      case Left(e) => fail(s"Invalid id generation failure: $e")
      case Right(actual) =>
        val expectedSequence = 0L
        val expectedId =
          (timestamp << idFormat.datacenterIdBits << idFormat.workerIdBits << idFormat.sequenceNumberBits) + expectedSequence
        assert(actual.id == expectedId)
        assert(actual.lastTimestamp == timestamp)
        assert(actual.lastSequence == expectedSequence)
    }
  }

}
