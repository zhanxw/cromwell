package cromwell.services.loadcontroller.impl

import cromwell.services.loadcontroller.impl.LoadControllerServiceActor._

trait LoadMetric {
  def name: String
  def highLoadRange: Range
  def veryHighLoadRange: Range
  def criticalLoadRange: Range
  def loadLevel(value: Int): LoadLevel = {
    if (criticalLoadRange.contains(value)) CriticalLoad
    else if (veryHighLoadRange.contains(value)) VeryHighLoad
    else if (highLoadRange.contains(value)) HighLoad
    else NormalLoad
  }
}

