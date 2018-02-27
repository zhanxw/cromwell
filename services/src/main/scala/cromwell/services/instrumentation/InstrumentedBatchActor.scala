package cromwell.services.instrumentation

import cats.data.NonEmptyList
import cromwell.core.actor.BatchActor
import cromwell.services.instrumentation.InstrumentedBatchActor.{QueueSizeTimerAction, QueueSizeTimerKey}

import scala.concurrent.Future

object InstrumentedBatchActor {
  case object QueueSizeTimerKey
  case object QueueSizeTimerAction
}

/**
  * Layer over batch actor that instruments the throughput and queue size
  */
trait InstrumentedBatchActor[C] { this: BatchActor[C] with CromwellInstrumentation =>

  protected def instrumentationPath: NonEmptyList[String]
  protected def instrumentationPrefix: Option[String]

  private val processedPath = instrumentationPath.::("processed")
  private val queueSizePath = instrumentationPath.::("queue")

  timers.startPeriodicTimer(QueueSizeTimerKey, QueueSizeTimerAction, CromwellInstrumentation.InstrumentationRate)

  protected def instrumentationReceive: Receive = {
    case QueueSizeTimerAction => sendGauge(queueSizePath, stateData.weight.toLong, instrumentationPrefix)
  }
  
  protected def instrumentedProcess(f: => Future[Int]) = {
    val action = f
    action foreach { n => count(processedPath, n.toLong, instrumentationPrefix) }
    action
  }
}
