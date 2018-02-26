package cromwell.services.loadcontroller.impl

case object MemoryMetric extends LoadMetric {
  val name = "percentageMemoryUsed"
  val highLoadRange: Range = Range(70, 80)
  val veryHighLoadRange: Range = Range(80, 90)
  val criticalLoadRange: Range = Range(90, 100)
}
