package wom.types

import spray.json.{JsNumber, JsString}
import wom.values.{WomInteger, WomLong, WomOptionalValue, WomString}
import wom.types.WomIntegerLike._

case object WomLongType extends WomPrimitiveType {
  val toDisplayString: String = "Long"

  override protected def coercion = {
    case i: Long => WomLong(i)
    case i: Integer => WomLong(i.toLong)
    case n: JsNumber if n.value.isValidLong => WomLong(n.value.longValue())
    case WomInteger(i) => WomLong(i.toLong)
    case WomOptionalValue(WomLongType, Some(l)) => l
    case i: WomLong => i
    case s: WomString => WomLong(s.value.asLong)
    case s: String => WomLong(s.asLong)
    case s: JsString => WomLong(s.value.asLong)
  }
}
