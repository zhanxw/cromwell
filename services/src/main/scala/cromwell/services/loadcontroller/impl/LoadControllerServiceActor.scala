package cromwell.services.loadcontroller.impl

import akka.actor.{Actor, ActorLogging, ActorRef, Timers}
import akka.routing.Listeners
import com.typesafe.config.Config
import cromwell.services.ServiceRegistryActor.{ListenToMessage, ServiceRegistryMessage}
import cromwell.services.loadcontroller.impl.LoadControllerServiceActor._
import cromwell.util.GracefulShutdownHelper.ShutdownCommand

import scala.concurrent.duration._

object LoadControllerServiceActor {
  val LoadControllerServiceName = "LoadController"
  case object LoadControlTimerKey
  case object LoadControlTimerAction

  sealed trait LoadLevel { def level: Int }
  case object NominalLoad extends LoadLevel { val level = 0 }
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

class LoadControllerServiceActor(serviceConfig: Config, globalConfig: Config, serviceRegistry: ActorRef) extends Actor with ActorLogging with Listeners with Timers {
  private val controlFrequency = 5.seconds
  private val runtime = Runtime.getRuntime
  private val maxMemory = runtime.maxMemory().toDouble
  
  private var loadLevel: LoadLevel = NominalLoad

  override def receive = listenerManagement.orElse(controlReceive)

  override def preStart() = {
    timers.startPeriodicTimer(LoadControlTimerKey, LoadControlTimerAction, controlFrequency)
    super.preStart()
  }
  
  private def controlReceive: Receive = {
    case LoadControlTimerAction => checkLoadAndAdjust()
    case ShutdownCommand => context stop self
  }
  
  def checkLoadAndAdjust(): Unit = {
    val memory = percentageMemoryUsed
    log.info("Memory usage is {} %", memory)
    val newMemoryLevel = MemoryMetric.loadLevel(memory)
    log.info("Memory level is {}", newMemoryLevel)
    val escalates = loadLevelOrdering.lt(loadLevel, newMemoryLevel)
    val backToNormal = loadLevel != NominalLoad && newMemoryLevel == NominalLoad
    if (escalates || backToNormal) {
      gossip(newMemoryLevel)
    }
    loadLevel = newMemoryLevel
  }
  
  def percentageMemoryUsed = ((1 - ((maxMemory - runtime.freeMemory()) / maxMemory)) * 100).toInt
}
