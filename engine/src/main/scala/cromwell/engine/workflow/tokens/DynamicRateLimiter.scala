package cromwell.engine.workflow.tokens

import akka.actor.{Actor, ActorLogging, ActorRef, Timers}
import akka.dispatch.ControlMessage
import common.util.ConfigUtil.PositivePercentage
import cromwell.engine.workflow.tokens.DynamicRateLimiter._
import cromwell.services.loadcontroller.impl.LoadControllerServiceActor.{CriticalLoad, HighLoad, NormalLoad, VeryHighLoad}

import scala.concurrent.duration._

trait DynamicRateLimiter { this: Actor with Timers with ActorLogging =>
  protected def startRate: Rate
  protected def nominalRate: Rate
  protected def rampUpPercentage: PositivePercentage
  protected def serviceRegistryActor: ActorRef
  protected def onRateUpdated(newRate: Rate.Normalized): Unit

  private var stepCursor = 0
  private var needsUpdate: Boolean = false
  private val targetRate: Rate.Normalized = nominalRate.normalized
  private val steps = startRate.normalized.linearize(targetRate, rampUpPercentage)

  private def currentRate: Rate.Normalized = steps(Math.min(stepCursor, steps.length - 1))

  protected def ratePreStart(): Unit = {
    if (!targetRate.isZero) {
      timers.startPeriodicTimer(ResetKey, ResetAction, currentRate.seconds.seconds)
      timers.startSingleTimer(NextRateKey, NextRateAction, 1.minute)
    }
    log.info("{} - Start rate: {}. Target rate: {}. Ramp up rate: {} % per minute.", self.path.name, startRate, nominalRate, rampUpPercentage)
  }

  protected def rateReceive: Receive = {
    case ResetAction if currentRate.n != 0 => releaseTokens()
    case NextRateAction if stepCursor - 1 < steps.length => increaseRate()
    case HighLoad => highLoad()
    case VeryHighLoad => veryHighLoad()
    case CriticalLoad => criticalLoad()
    case NormalLoad => backToNormal()
  }

  private def updateRate() = {
    timers.startPeriodicTimer(ResetKey, ResetAction, currentRate.seconds.seconds)
    onRateUpdated(currentRate)
  }

  private def releaseTokens() = {
    self ! TokensAvailable(currentRate.n)
    // Update the timer with the next rate
    if (needsUpdate) {
      updateRate()
      needsUpdate = false
    }
  }

  private def increaseRate() = {
    stepCursor += 1
    needsUpdate = true
    timers.startSingleTimer(NextRateKey, NextRateAction, 1.minute)
  }

  // When load is high, stabilize the rate of token distribution
  private def highLoad(doLogging: Boolean = true) = {
    needsUpdate = false
    timers.cancel(NextRateKey)
    if (doLogging) log.warning("{} - High load alert. Stop increasing token distribution rate. Current rate: {}", self.path.name, currentRate)
  }

  // When load is very high, cut in half the rate of token distribution
  private def veryHighLoad() = {
    highLoad(doLogging = false)
    stepCursor = stepCursor / 2
    updateRate()
    log.warning("{} - Very high load alert. Stop increasing token distribution rate and cut it in half. Current rate: {}", self.path.name, currentRate)
  }

  // When load is critical, freeze everything
  private def criticalLoad() = {
    log.warning("{} - Critical load alert. Freeze token distribution.", self.path.name)
    timers.cancel(NextRateKey)
    timers.cancel(ResetKey)
  }

  // When back to normal, restart the token distribution timer if needed and re-initialize the "next rate" timer
  private def backToNormal() = {
    needsUpdate = true
    if (!timers.isTimerActive(ResetKey)) updateRate()
    timers.startSingleTimer(NextRateKey, NextRateAction, 1.minute)
    log.info("{} - Load back to normal. Current rate: {}", self.path.name, currentRate)
  }
}

object DynamicRateLimiter {
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
