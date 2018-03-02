package cromwell.services.loadcontroller

import akka.actor.ActorRef
import cats.data.NonEmptyList
import cromwell.core.actor.BatchActor
import cromwell.services.loadcontroller.LoadControllerService.{HighLoad, LoadMetric, NormalLoad}

trait LoadControlledBatchActor[C] { this: BatchActor[C] =>
  def threshold: Int
  def serviceRegistryActor: ActorRef

  private val path = if (routed) NonEmptyList.of(context.parent.path.name, self.path.name) else NonEmptyList.one(self.path.name)
  private def weightToLoad(weight: Int) = if (weight > threshold) HighLoad else NormalLoad
  override def weightUpdate(weight: Int) = serviceRegistryActor ! LoadMetric(path, weightToLoad(weight))
}
