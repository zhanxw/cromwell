package cromwell.engine.workflow.tokens

import akka.actor.{Actor, ActorLogging, ActorRef, Timers}
import akka.dispatch.ControlMessage
import common.util.ConfigUtil.PositivePercentage
import cromwell.engine.workflow.tokens.DynamicRateLimiter._
import cromwell.services.loadcontroller.impl.LoadControllerServiceActor.{CriticalLoad, HighLoad, MemoryAlertLifted, VeryHighLoad}

import scala.concurrent.duration._

trait DynamicRateLimiter { this: Actor with Timers with ActorLogging =>
  protected def startRate: Rate
  protected def nominalRate: Rate
  protected def rampUpPercentage: PositivePercentage
  protected def serviceRegistryActor: ActorRef

  private var currentRate: Rate.Normalized = startRate.normalized
  private var nextStepCursor = 1
  private var nextRate: Option[Rate.Normalized] = None
  private val targetRate: Rate.Normalized = nominalRate.normalized
  private val steps = startRate.normalized.linearize(targetRate, rampUpPercentage)

  protected def ratePreStart(): Unit = {
    if (!targetRate.isZero) {
      timers.startPeriodicTimer(ResetKey, ResetAction, currentRate.seconds.seconds)
      timers.startSingleTimer(NextRateKey, NextRateAction, 1.minute)
    }
    log.info("{} - Start rate: {}. Target rate: {}. Ramp up rate: {} % per minute.", self.path.name, startRate, nominalRate, rampUpPercentage)
  }

  protected def rateReceive: Receive = {
    case ResetAction if currentRate.n != 0 => 
      self ! TokensAvailable(currentRate.n)
      // Update the timer with the next rate
      nextRate.foreach { next =>
        currentRate = next
        nextRate = None
        timers.startPeriodicTimer(ResetKey, ResetAction, currentRate.seconds.seconds)
        log.info("{} - New rate: {}", self.path.name, currentRate)
      }
    case NextRateAction if nextStepCursor < steps.length =>
      nextRate = Option(steps(nextStepCursor))
      nextStepCursor += 1
      timers.startSingleTimer(NextRateKey, NextRateAction, 1.minute)
    case HighLoad =>
      nextRate = None
      timers.cancel(NextRateKey)
      log.info("{} - Memory Alert Level 1. Stopping rampUp. Current rate: {}", self.path.name, currentRate)
    case VeryHighLoad =>
      nextRate = None
      timers.cancel(NextRateKey)
      nextStepCursor = nextStepCursor / 2
      currentRate = steps(nextStepCursor)
      timers.startPeriodicTimer(ResetKey, ResetAction, currentRate.seconds.seconds)
      log.info("{} - Memory Alert Level 2. Cutting rampUp rate in half. Current rate: {}", self.path.name, currentRate)
    case CriticalLoad =>
      timers.cancel(NextRateKey)
      timers.cancel(ResetKey)
      log.info("{} - Memory Alert Level 3. Freezing token dispensing.", self.path.name)
    case MemoryAlertLifted =>
      if (!timers.isTimerActive(ResetKey)) {
        timers.startPeriodicTimer(ResetKey, ResetAction, currentRate.seconds.seconds)
      }
      timers.startSingleTimer(NextRateKey, NextRateAction, 1.minute)
      log.info("{} - Memory Alert Lifted. Resuming. Current rate: {}", self.path.name, currentRate)
  }
}

object DynamicRateLimiter {
  val RampUpTimeFactor = 10.seconds
  private case object ResetKey
  private case object ResetAction extends ControlMessage

  private case object NextRateKey extends ControlMessage
  private case object NextRateAction extends ControlMessage

  case class TokensAvailable(n: Int) extends ControlMessage

  implicit class RateEnhancer(val n: Int) extends AnyVal {
    def per(duration: FiniteDuration) = Rate(n, duration)
  }
  
  object Rate {
    
    object Normalized {
      def Zero = Normalized(0, 0)
    }

    /**
      * Normalized rate per number of second
      */
    case class Normalized(n: Int, seconds: Long) {
      def linearize(target: Normalized, rampUpPercentage: PositivePercentage) = {
        val sampling = 100 / rampUpPercentage.value

        val xRange = target.n - n
        val yRange = target.seconds - seconds
        val xSampleSize = xRange.toDouble / sampling
        val ySampleSize = yRange.toDouble / sampling
        
        (1 to sampling).toArray
          .map(sampleN => 
            Normalized(n + (xSampleSize * sampleN).toInt, seconds + (ySampleSize * sampleN).toInt)
          )
      }
      
      def isZero = n == 0 || seconds == 0L

      override def toString = s"$n per $seconds seconds"
    }
  }
  
  case class Rate(n: Int, per: FiniteDuration) {
    val normalized = Rate.Normalized(n.toInt, per.toSeconds)
    override def toString = s"$n per ${per.toSeconds} seconds"
  }
}
