package wom.types


object WomIntegerLike {
  private def parseString(string: String): Long = {
    if (string.contains("e") || string.contains("E")) {
      // More flexible integer parsing that can handle the sort of scientific notation Circe likes to produce
      // but is not as precise as `toLong` so only use if needed.
      java.lang.Double.valueOf(string).longValue()
    } else {
      string.toLong
    }
  }

  implicit class EnhancedLong(val long: Long) extends AnyVal {
    def inIntRange: Boolean = long >= Int.MinValue && long <= Int.MaxValue
  }

  implicit class EnhancedString(val string: String) extends AnyVal {
    def asInt: Int = asLong.intValue()

    def asLong: Long = parseString(string)
  }
}
