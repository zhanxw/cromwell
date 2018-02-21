package cromwell.engine.workflow.tokens

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Terminated, Timers}
import common.util.ConfigUtil.{OneToOneHundred, PositivePercentage}
import cromwell.core.Dispatcher.EngineDispatcher
import cromwell.core.JobExecutionToken._
import cromwell.core.{ExecutionStatus, JobExecutionToken}
import cromwell.engine.instrumentation.JobInstrumentation
import cromwell.engine.workflow.tokens.DynamicRateLimiter.{Rate, TokensAvailable}
import cromwell.engine.workflow.tokens.JobExecutionTokenDispenserActor._
import cromwell.engine.workflow.tokens.TokenQueue.LeasedActor
import cromwell.services.instrumentation.CromwellInstrumentation._
import cromwell.services.instrumentation.CromwellInstrumentationScheduler
import cromwell.services.loadcontroller.impl.LoadControllerServiceActor.ListenToLoadController
import io.github.andrebeat.pool.Lease

class JobExecutionTokenDispenserActor(override val serviceRegistryActor: ActorRef,
                                      override val startRate: Rate,
                                      override val nominalRate: Rate,
                                      override val rampUpPercentage: PositivePercentage) extends Actor
  with ActorLogging
  with JobInstrumentation
  with CromwellInstrumentationScheduler
  with DynamicRateLimiter
  with Timers {

  /**
    * Lazily created token queue. We only create a queue for a token type when we need it
    */
  var tokenQueues: Map[JobExecutionTokenType, TokenQueue] = Map.empty
  var tokenAssignments: Map[ActorRef, Lease[JobExecutionToken]] = Map.empty

  scheduleInstrumentation { 
    sendGaugeJob(ExecutionStatus.Running.toString, tokenAssignments.size.toLong)
    sendGaugeJob(ExecutionStatus.QueuedInCromwell.toString, tokenQueues.values.map(_.size).sum.toLong)
  }

  override def preStart() = {
    ratePreStart()
    serviceRegistryActor ! ListenToLoadController
    super.preStart()
  }

  private def tokenReceive: Actor.Receive = {
    case JobExecutionTokenRequest(tokenType) => enqueue(sender, tokenType)
    case JobExecutionTokenReturn(token) => release(sender, token)
    case TokensAvailable(n) => distribute(n)
    case Terminated(terminee) => onTerminate(terminee)
  }
  
  override def receive = rateReceive.orElse(tokenReceive)

  private def enqueue(sndr: ActorRef, tokenType: JobExecutionTokenType): Unit = {
    if (tokenAssignments.contains(sndr)) {
      sndr ! JobExecutionTokenDispensed(tokenAssignments(sndr))
    } else {
      context.watch(sndr)
      val updatedTokenQueue = getTokenQueue(tokenType).enqueue(sndr)
      tokenQueues += tokenType -> updatedTokenQueue
    }
  }

  private def getTokenQueue(tokenType: JobExecutionTokenType): TokenQueue = {
    tokenQueues.getOrElse(tokenType, createNewQueue(tokenType))
  }

  private def createNewQueue(tokenType: JobExecutionTokenType): TokenQueue = {
    val newQueue = TokenQueue(tokenType)
    tokenQueues += tokenType -> newQueue
    newQueue
  }

  private def distribute(n: Int) = if (tokenQueues.nonEmpty) {
    val iterator = RoundRobinIterator(tokenQueues.values.toList)

    val nextTokens = iterator.take(n).toList
    val newLeases = nextTokens.foldLeft(Map.empty[ActorRef, Lease[JobExecutionToken]])({
      case (accLeases, LeasedActor(actor, lease)) =>
        accLeases + (actor -> lease)
    })
    val newQueues = iterator.updatedQueues.map(queue => queue.tokenType -> queue).toMap

    newLeases.foreach({
      case (actor, lease) =>
        incrementJob("Started")
        actor ! JobExecutionTokenDispensed(lease)
    })

    tokenQueues = newQueues
    tokenAssignments = tokenAssignments ++ newLeases
  }

  private def release(actor: ActorRef, token: Lease[JobExecutionToken]): Unit = {
    if (tokenAssignments.contains(actor) && tokenAssignments(actor) == token) {
      tokenAssignments -= actor
      token.release()
      context.unwatch(actor)
      ()
    } else {
      log.error("Job execution token returned from incorrect actor: {}", token)
    }
  }

  private def onTerminate(terminee: ActorRef): Unit = {
    tokenAssignments.get(terminee) match {
      case Some(token) =>
        log.debug("Actor {} stopped without returning its Job Execution Token. Reclaiming it!", terminee)
        self.tell(msg = JobExecutionTokenReturn(token), sender = terminee)
      case None =>
        log.debug("Actor {} stopped while we were still watching it... but it doesn't have a token. Removing it from any queues if necessary", terminee)
        tokenQueues = tokenQueues map {
          case (tokenType, tokenQueue @ TokenQueue(queue, _)) => tokenType -> tokenQueue.copy(queue = queue.filterNot(_ == terminee))
        }
    }
    context.unwatch(terminee)
    ()
  }
}

object JobExecutionTokenDispenserActor {
  import DynamicRateLimiter._
  import eu.timepit.refined.refineV

  import scala.concurrent.duration._

  val DefaultStartRate = 100 per 2.second
  val DefaultNominalRate = 1000 per 1.second
  val DefaultRampUpPercentage = refineV[OneToOneHundred](10) match {
    case Left(_) => throw new Exception("Don't break the default !")
    case Right(p) => p
  }

  def props(serviceRegistryActor: ActorRef,
            startRate: Rate = DefaultStartRate,
            nominalRate: Rate = DefaultNominalRate,
            rampupPercentage: PositivePercentage = DefaultRampUpPercentage) = {
    Props(new JobExecutionTokenDispenserActor(serviceRegistryActor, startRate, nominalRate, rampupPercentage))
      .withDispatcher(EngineDispatcher)
      .withMailbox("akka.priority-mailbox")
  }

  case class JobExecutionTokenRequest(jobExecutionTokenType: JobExecutionTokenType)
  case class JobExecutionTokenReturn(jobExecutionToken: Lease[JobExecutionToken])

  sealed trait JobExecutionTokenRequestResult
  case class JobExecutionTokenDispensed(jobExecutionToken: Lease[JobExecutionToken]) extends JobExecutionTokenRequestResult
}
