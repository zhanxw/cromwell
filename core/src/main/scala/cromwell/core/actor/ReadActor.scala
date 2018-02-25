package cromwell.core.actor

import scala.concurrent.Future
import scala.concurrent.duration.Duration

abstract class ReadActor[C] extends BatchActor[C](Duration.Zero, 1) {
  override def weightFunction(command: C) = 1
  override final def process(data: Vector[C]): Future[Int] = {
    processHead(data.head).map(_ => 1)
  }
  
  def processHead(head: C): Future[Any]
}
