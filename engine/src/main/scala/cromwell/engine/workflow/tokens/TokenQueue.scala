package cromwell.engine.workflow.tokens

import akka.actor.ActorRef
import cromwell.core.JobExecutionToken
import cromwell.core.JobExecutionToken.JobExecutionTokenType
import cromwell.engine.workflow.tokens.TokenQueue.LeasedActor
import io.github.andrebeat.pool.Lease

import scala.collection.immutable.Queue

object TokenQueue {
  case class LeasedActor(actor: ActorRef, lease: Lease[JobExecutionToken])
  def apply(tokenType: JobExecutionTokenType) = {
    new TokenQueue(Queue.empty, TokenPool(tokenType))
  }
}

final case class TokenQueue(queue: Queue[ActorRef], private val pool: TokenPool) {
  val tokenType = pool.tokenType
  def size = queue.size

  /**
    * Enqueues an actor (or just finds its current position)
    *
    * @return the new token queue
    */
  def enqueue(actor: ActorRef): TokenQueue = {
    copy(queue = queue.enqueue(actor))
  }

  def dequeue(): Option[(LeasedActor, TokenQueue)] = {
    for {
      actorAndNewQueue <- queue.dequeueOption
      (actor, newQueue) = actorAndNewQueue
      lease <- pool.tryAcquire()
    } yield LeasedActor(actor, lease) -> copy(queue = newQueue)
  }
  
  def available: Boolean = queue.nonEmpty && pool.leased() < pool.capacity
}
