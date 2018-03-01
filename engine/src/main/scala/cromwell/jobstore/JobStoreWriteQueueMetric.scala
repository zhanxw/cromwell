package cromwell.jobstore

import cromwell.services.loadcontroller.LoadControllerService.{LoadLevel, LoadMetric}

case class JobStoreWriteQueueMetric(loadLevel: LoadLevel) extends LoadMetric {
  override val name = "jobStoreWrite"
}