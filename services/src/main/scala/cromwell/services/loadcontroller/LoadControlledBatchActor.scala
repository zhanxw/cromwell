package cromwell.services.loadcontroller

import akka.actor.ActorRef
import cromwell.core.actor.BatchActor
import cromwell.services.loadcontroller.LoadControllerService.{HighLoad, LoadMetric, NormalLoad}

trait LoadControlledBatchActor[C] { this: BatchActor[C] =>
  def threshold: Int
  def serviceRegistryActor: ActorRef

  private def weightToLoad(weight: Int) = if (weight > threshold) HighLoad else NormalLoad
  override def weightUpdate(weight: Int) = serviceRegistryActor ! LoadMetric(self.path.name, weightToLoad(weight))
}
