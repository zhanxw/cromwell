package wom.types

/** WomFloat actually wraps a double. Mimicking the structure of WomIntegerLike JIC we need to make the
  * distinction between float and double that CWL seems to be suggesting we should make. */
object WomFloatLike {
  private def parseString(string: String): Double = {
    if (string.contains("e") || string.contains("E")) {
      // More flexible floating point parsing that can handle the sort of scientific notation Circe likes to produce
      // but is not as precise as `toDouble` so only use if needed.
      java.lang.Double.valueOf(string)
    } else {
      string.toDouble
    }
  }

  implicit class EnhancedString(val string: String) extends AnyVal {
    def asDouble: Double = parseString(string)
  }
}
