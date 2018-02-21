package cromwell.engine.workflow.tokens

import cromwell.engine.workflow.tokens.TokenQueue.LeasedActor

case class RoundRobinIterator(initialTokenQueue: List[TokenQueue]) extends Iterator[LeasedActor] {
  private var pointer: Int = 0
  private var tokenQueue = initialTokenQueue
  
  def updatedQueues = tokenQueue

  override def hasNext = tokenQueue.exists(_.available)
  override def next() = {
    val pointerToSize = pointer until tokenQueue.size
    lazy val startToPointer = 0 until pointer

    val (index, (leasedActor, newQueue)) = findFirst(pointerToSize)
      .orElse(findFirst(startToPointer))
      .getOrElse(throw new IllegalStateException("Token iterator is empty"))

    pointer = (pointer + 1) % tokenQueue.size
    tokenQueue = tokenQueue.updated(index, newQueue)
    leasedActor
  }

  private def findFirst(range: Range): Option[(Int, (TokenQueue.LeasedActor, TokenQueue))] = {
    range.toStream.map(dequeueAt).collectFirst( { case Some(a) => a } )
  }

  private def dequeueAt(n: Int): Option[(Int, (TokenQueue.LeasedActor, TokenQueue))] = {
    tokenQueue(pointer).dequeue().map(n -> _)
  }
}
