package cromwell.services.loadcontroller.impl

case object MetadataQueueMetric extends LoadMetric {
  val highLoadRange: Range = Range(500 * 1000, 75)
  val veryHighLoadRange: Range = Range(75, 90)
  val criticalLoadRange: Range = Range(90, 100)
}
