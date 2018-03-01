package cromwell.services.keyvalue

import cromwell.services.loadcontroller.LoadControllerService.{LoadLevel, LoadMetric}

case class KeyValueQueueMetric(loadLevel: LoadLevel) extends LoadMetric {
  override val name = "keyValue"
}
