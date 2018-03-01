package cromwell.services.loadcontroller.impl

import akka.actor.{Actor, ActorLogging, ActorRef, Timers}
import akka.routing.Listeners
import cats.data.NonEmptyList
import com.typesafe.config.Config
import cromwell.services.instrumentation.CromwellInstrumentation
import cromwell.services.loadcontroller.LoadControllerService._
import cromwell.services.loadcontroller.impl.LoadControllerServiceActor._
import cromwell.services.metadata.MetadataService.ListenToMetadataWriteActor
import cromwell.util.GracefulShutdownHelper.ShutdownCommand
import net.ceedubs.ficus.Ficus._

import scala.concurrent.duration._

object LoadControllerServiceActor {
  val LoadControllerServiceName = "LoadController"
  val LoadInstrumentationPrefix = Option("load")
  case object LoadControlTimerKey
  case object LoadControlTimerAction
}

/**
  * Service Actor that monitors load and sends alert to the rest of the system when load is determined abnormal.
  */
class LoadControllerServiceActor(serviceConfig: Config,
                                 globalConfig: Config,
                                 override val serviceRegistryActor: ActorRef
                                ) extends Actor
  with ActorLogging with Listeners with Timers with CromwellInstrumentation {
  private val controlFrequency = serviceConfig.as[Option[FiniteDuration]]("control-frequency").getOrElse(5.seconds)

  private var loadLevel: LoadLevel = NormalLoad
  private var loadMetrics: Map[LoadMetric, LoadLevel] = Map.empty

  override def receive = listenerManagement.orElse(controlReceive)

  override def preStart() = {
    serviceRegistryActor ! ListenToMetadataWriteActor
    timers.startPeriodicTimer(LoadControlTimerKey, LoadControlTimerAction, controlFrequency)
    super.preStart()
  }

  private val controlReceive: Receive = {
    case metric: LoadMetric => updateMetric(metric)
    case LoadControlTimerAction => checkLoad()
    case ShutdownCommand => context stop self
  }

  def updateMetric(metric: LoadMetric): Unit = {
    loadMetrics = loadMetrics + (metric -> metric.loadLevel)
    sendGauge(NonEmptyList.one(metric.name), loadLevel.level.toLong, LoadInstrumentationPrefix)
  }

  def checkLoad(): Unit = {
    // Simply take the max level of all load metrics for now
    val newLoadLevel = if (loadMetrics.nonEmpty) loadMetrics.values.max else NormalLoad
    // The load level escalates if the new load is higher than the previous load
    val escalates = loadLevelOrdering.gt(newLoadLevel, loadLevel)
    // Back to normal if we were not normal before but now are
    val backToNormal = loadLevel != NormalLoad && newLoadLevel == NormalLoad
    // If there's something to say, let it out !
    if (escalates || backToNormal) gossip(newLoadLevel)
    loadLevel = newLoadLevel
    sendGauge(NonEmptyList.one("global"), loadLevel.level.toLong, LoadInstrumentationPrefix)
  }
}
