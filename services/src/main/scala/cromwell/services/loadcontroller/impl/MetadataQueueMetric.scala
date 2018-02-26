package cromwell.services.loadcontroller.impl

case object MetadataQueueMetric extends LoadMetric {
  val name = "metadataQueueSize"
  val highLoadRange: Range = Range(1 * 1000 * 1000, 5 * 1000 * 1000)
  val veryHighLoadRange: Range = Range(5 * 1000 * 1000, 10 * 1000 * 1000)
  val criticalLoadRange: Range = Range(10 * 1000 * 1000, Int.MaxValue)
}
