package com.github.hayasshi.snowflake.scala.core

import java.time.Instant

object IdFormat {
  case class DatacenterId private[core] (value: Long)
  case class WorkerId private[core] (value: Long)

  val TimeBits: Int = 41

  object Default {
    val DatacenterIdBits   = 5  // < 32
    val WorkerIdBits       = 5  // < 32
    val SequenceNumberBits = 12 // < 4096

    def apply(baseEpochTime: Instant): IdFormat =
      IdFormat(DatacenterIdBits, WorkerIdBits, SequenceNumberBits, baseEpochTime)
  }

  private def maxValueOfBits(bits: Int): Long = (0x0001L << bits) - 1
}

case class IdFormat(
    datacenterIdBits: Int,
    workerIdBits: Int,
    sequenceNumberBits: Int,
    baseEpoch: Instant
) {
  require(datacenterIdBits + workerIdBits + sequenceNumberBits == 22)
  require(datacenterIdBits >= 0)
  require(workerIdBits >= 0)
  require(sequenceNumberBits > 0)

  import IdFormat.*

  def datacenterId(value: Long): DatacenterId = {
    require(0 <= value && value <= maxValueOfBits(datacenterIdBits))
    DatacenterId(value)
  }

  def workerId(value: Long): WorkerId = {
    require(0 <= value && value <= maxValueOfBits(workerIdBits))
    WorkerId(value)
  }
}
