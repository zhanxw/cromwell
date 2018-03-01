package cromwell.engine.workflow.lifecycle.execution.callcaching

import cromwell.services.loadcontroller.LoadControllerService.{LoadLevel, LoadMetric}

case class CallCacheWriteQueueMetric(loadLevel: LoadLevel) extends LoadMetric {
  override val name = "callCacheWrite"
}