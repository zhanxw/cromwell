package cromwell.engine.workflow.lifecycle.execution.callcaching

import cromwell.services.loadcontroller.LoadControllerService.{LoadLevel, LoadMetric}

case class CallCacheReadQueueMetric(loadLevel: LoadLevel) extends LoadMetric {
  override val name = "callCacheRead"
}