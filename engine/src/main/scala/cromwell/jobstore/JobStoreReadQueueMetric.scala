package cromwell.jobstore

import cromwell.services.loadcontroller.LoadControllerService.{LoadLevel, LoadMetric}

case class JobStoreReadQueueMetric(loadLevel: LoadLevel) extends LoadMetric {
  override val name = "jobStoreRead"
}