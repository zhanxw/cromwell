package cromwell.services.loadcontroller.impl

case object MemoryMetric extends LoadMetric {
  val highLoadRange: Range = Range(60, 75)
  val veryHighLoadRange: Range = Range(75, 90)
  val criticalLoadRange: Range = Range(90, 100)
}
