package cromwell.services.loadcontroller.impl

import akka.actor.{Actor, ActorLogging, ActorRef, Timers}
import akka.routing.Listeners
import cats.data.NonEmptyList
import com.typesafe.config.Config
import cromwell.core.actor.BatchActor.QueueWeight
import cromwell.core.instrumentation.InstrumentationPrefixes
import cromwell.services.ServiceRegistryActor.{ListenToMessage, ServiceRegistryMessage}
import cromwell.services.instrumentation.CromwellInstrumentation
import cromwell.services.loadcontroller.impl.LoadControllerServiceActor._
import cromwell.services.metadata.MetadataService.ListenToMetadataWriteActor
import cromwell.util.GracefulShutdownHelper.ShutdownCommand

import scala.concurrent.duration._

object LoadControllerServiceActor {
  val LoadControllerServiceName = "LoadController"
  val LoadLevelInstrumentation = NonEmptyList.one("loadLevel")
  case object LoadControlTimerKey
  case object LoadControlTimerAction

  sealed trait LoadLevel { def level: Int }
  case object NormalLoad extends LoadLevel { val level = 0 }
  case object HighLoad extends LoadLevel { val level = 1 }
  case object VeryHighLoad extends LoadLevel { val level = 2 }
  case object CriticalLoad extends LoadLevel { val level = 3 }

  implicit val loadLevelOrdering: Ordering[LoadLevel] = Ordering.by[LoadLevel, Int](_.level)
  case object MemoryAlertLifted

  sealed trait LoadControllerMessage extends ServiceRegistryMessage {
    def serviceName = LoadControllerServiceName
  }

  case object ListenToLoadController extends LoadControllerMessage with ListenToMessage
}

class LoadControllerServiceActor(serviceConfig: Config, globalConfig: Config, override val serviceRegistryActor: ActorRef) extends Actor
  with ActorLogging with Listeners with Timers with CromwellInstrumentation {
  private val controlFrequency = 5.seconds
  private val runtime = Runtime.getRuntime
  private val maxMemory = runtime.maxMemory().toDouble

  private var loadLevel: LoadLevel = NormalLoad
  private var loadMetrics: Map[LoadMetric, LoadLevel] = Map.empty

  override def receive = listenerManagement.orElse(controlReceive)

  override def preStart() = {
    serviceRegistryActor ! ListenToMetadataWriteActor
    timers.startPeriodicTimer(LoadControlTimerKey, LoadControlTimerAction, controlFrequency)
    super.preStart()
  }

  private def controlReceive: Receive = {
    case LoadControlTimerAction => checkLoad()
    case QueueWeight(weight) => updateMetric(MetadataQueueMetric, weight)
    case ShutdownCommand => context stop self
  }

  def updateMetric(metric: LoadMetric, loadLevel: Int) = {
    loadMetrics = loadMetrics + (metric -> metric.loadLevel(loadLevel))
    sendGauge(NonEmptyList.one(metric.name), loadLevel.toLong, InstrumentationPrefixes.LoadPrefix)
  }

  def updateMemoryLoad() = updateMetric(MemoryMetric, percentageMemoryUsed)

  def checkLoad(): Unit = {
    updateMemoryLoad()
    val newLoadLevel = loadMetrics.values.max
    val escalates = loadLevelOrdering.lt(loadLevel, newLoadLevel)
    val backToNormal = loadLevel != NormalLoad && newLoadLevel == NormalLoad
    if (escalates || backToNormal) {
      gossip(newLoadLevel)
    }
    loadLevel = newLoadLevel
    sendGauge(LoadLevelInstrumentation, loadLevel.level.toLong)
  }

  def percentageMemoryUsed = ((1 - ((maxMemory - runtime.freeMemory()) / maxMemory)) * 100).toInt
}
