package cromwell.services.loadcontroller

import akka.actor.ActorRef
import cromwell.core.actor.BatchActor
import cromwell.services.loadcontroller.LoadControllerService.LoadMetric

trait LoadControlledBatchActor[T <: LoadMetric, C] { this: BatchActor[C] =>
  def metricClass: Class[T]
  def threshold: Int
  def serviceRegistryActor: ActorRef

  private val metricMaker = new ThresholdBasedLoadMetricMaker(metricClass, threshold)
  override def weightUpdate(weight: Int) = serviceRegistryActor ! metricMaker.make(weight)
}
