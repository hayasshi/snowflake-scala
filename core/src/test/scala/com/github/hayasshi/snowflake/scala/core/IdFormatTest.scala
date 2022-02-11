package com.github.hayasshi.snowflake.scala.core

import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant

class IdFormatTest extends AnyFunSuite {

  val testEpoch: Instant = Instant.EPOCH

  test("Constructor should check that total bits length is 22") {
    assertThrows[IllegalArgumentException](IdFormat(1, 2, 18, testEpoch))
    assertThrows[IllegalArgumentException](IdFormat(1, 2, 20, testEpoch))

    IdFormat.Default(testEpoch)
    IdFormat(1, 2, 19, testEpoch)
    succeed
  }

  test("Constructor should check that datacenter id bits is grater or equal than 0") {
    assertThrows[IllegalArgumentException](IdFormat(-1, 2, 21, testEpoch))

    IdFormat(0, 2, 20, testEpoch)
    IdFormat(1, 2, 19, testEpoch)
    succeed
  }

  test("Constructor should check that worker id bits is grater or equal than 0") {
    assertThrows[IllegalArgumentException](IdFormat(2, -1, 21, testEpoch))

    IdFormat(2, 0, 20, testEpoch)
    IdFormat(2, 1, 19, testEpoch)
    succeed
  }

  test("Constructor should check that sequence number bits is grater than 0") {
    assertThrows[IllegalArgumentException](IdFormat(2, 21, -1, testEpoch))
    assertThrows[IllegalArgumentException](IdFormat(2, 20, 0, testEpoch))
    IdFormat(2, 19, 1, testEpoch)
    succeed
  }

}
