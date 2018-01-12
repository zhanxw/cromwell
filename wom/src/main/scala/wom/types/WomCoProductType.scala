package wom.types

import wom.values.WomValue

/**
  * A type that represents a logic OR of other types.
  * A WomValue CANNOT be of type WomCoProductType. However it can be used to represent an input or output that can be in
  * any of those types.
  */
case class WomCoProductType(womTypes: List[WomType]) extends WomType {

  val coercionOrder = WomType.womTypeCoercionOrder.filter(womTypes.contains)
  
  override protected def coercion = {
    // If the womType is in the list, return the value
    case womValue: WomValue if womTypes.contains(womValue.womType) => womValue
    case other if womTypes.exists(_.coercionDefined(other)) => coercionOrder.collectFirst({
      case t if t.coercionDefined(other) => t
    }).get.coerceRawValue(other).get
  }

  override def toDisplayString = s"coproduct type: (${womTypes.map(_.toDisplayString).mkString(", ")})"
}
