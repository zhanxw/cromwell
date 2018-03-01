package cromwell.services.loadcontroller

import cromwell.services.loadcontroller.LoadControllerService.{HighLoad, LoadLevel, LoadMetric, NormalLoad}

class ThresholdBasedLoadMetricMaker[T <: LoadMetric](clazz: Class[T], threshold: Int) {
  def make(value: Int) = {
    clazz.getConstructor(classOf[LoadLevel]).newInstance(if (value > threshold) HighLoad else NormalLoad)
  }
}
