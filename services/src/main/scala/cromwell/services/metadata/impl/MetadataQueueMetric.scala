package cromwell.services.metadata.impl

import cromwell.services.loadcontroller.LoadControllerService.{LoadLevel, LoadMetric}

case class MetadataQueueMetric(loadLevel: LoadLevel) extends LoadMetric {
  val name = "metadataQueueSize"
}
