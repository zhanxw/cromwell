package cromwell.services.loadcontroller.impl

case object MetadataQueueMetric extends LoadMetric {
  val highLoadRange: Range = Range(100 * 1000, 500 * 1000)
  val veryHighLoadRange: Range = Range(500 * 1000, 1 * 1000 * 1000)
  val criticalLoadRange: Range = Range(1 * 1000 * 1000, Int.MaxValue)
}
