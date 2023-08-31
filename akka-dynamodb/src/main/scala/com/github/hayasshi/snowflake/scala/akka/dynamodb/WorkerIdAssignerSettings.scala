package com.github.hayasshi.snowflake.scala.akka.dynamodb

import com.typesafe.config.{ Config, ConfigFactory }

import scala.concurrent.duration.{ DurationLong, FiniteDuration }

case class WorkerIdAssignerSettings(
    host: String,
    sessionDuration: FiniteDuration,
    safeWaitDuration: FiniteDuration,
    heartbeatInterval: FiniteDuration
)

object WorkerIdAssignerSettings {

  def apply(): WorkerIdAssignerSettings = apply(ConfigFactory.load())

  def apply(config: Config): WorkerIdAssignerSettings = {
    val c = config.getConfig("snowflake-scala.akka-dynamodb").ensuring(_ != null)

    val canonicalHostname = c.getString("canonical.hostname").ensuring(_ != null)
    val canonicalPort     = c.getInt("canonical.port")

    WorkerIdAssignerSettings(
      s"$canonicalHostname:$canonicalPort",
      c.getDuration("session-timeout").toMillis.millis,
      c.getDuration("initial-wait").toMillis.millis,
      c.getDuration("heartbeat-interval").toMillis.millis
    )
  }

}
