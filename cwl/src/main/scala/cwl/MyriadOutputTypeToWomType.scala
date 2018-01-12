package cwl

import cwl.CwlType.CwlType
import cwl.command.ParentName
import mouse.all._
import shapeless.Poly1
import wom.types._

object MyriadOutputTypeToWomType extends Poly1{

  import Case._

  implicit def cwlType: Aux[MyriadOutputInnerType, WomType] = at[MyriadOutputInnerType]{
    _.fold(MyriadOutputInnerTypeToWomType)
  }

  implicit def acwl: Aux[Array[MyriadOutputInnerType], WomType] = at[Array[MyriadOutputInnerType]] { types =>
    // If one of the type is "null", it means this is an optional type
    types.partition(_.select[String].contains("null")) match {
      // If there's no non null type, create a coproduct of the types
      case (nullTypes, nonNullTypes) if nullTypes.isEmpty =>
        WomCoProductType(nonNullTypes.map(_.fold(MyriadOutputInnerTypeToWomType)).toList)
      // If there's a null type and a single non null type, it's a classic WomOptionalType
      case (nullTypes, Array(singleNonNullType)) if nullTypes.nonEmpty =>
        WomOptionalType(singleNonNullType.fold(MyriadOutputInnerTypeToWomType))
      // If there's a null type and multiple non null types, it's a WomOptionalType(WomCoProductType)
      case (nullTypes, nonNullTypes) if nullTypes.nonEmpty =>
        WomOptionalType(WomCoProductType(nonNullTypes.map(_.fold(MyriadOutputInnerTypeToWomType)).toList))
    }
  }
}

object MyriadOutputInnerTypeToWomType extends Poly1 {

  import Case._

  def ex(component: String) = throw new RuntimeException(s"input type $component not yet suported by WOM!")

  implicit def cwlType: Aux[CwlType, WomType] = at[CwlType]{
    cwl.cwlTypeToWomType
  }

  implicit def ors: Aux[OutputRecordSchema, WomType] = at[OutputRecordSchema] {
    case OutputRecordSchema(_, Some(fields), _) =>
      val typeMap = fields.map({ field =>
        val parsedName = FullyQualifiedName(field.name)(ParentName.empty).id
        parsedName -> field.`type`.fold(MyriadOutputTypeToWomType)
      }).toMap
      WomCompositeType(typeMap)
    case ors => ors.toString |> ex
  }

  implicit def oes: Aux[OutputEnumSchema, WomType] = at[OutputEnumSchema] {
    _.toString |> ex
  }

  implicit def oas: Aux[OutputArraySchema, WomType] = at[OutputArraySchema] {
    oas =>
      val arrayType: WomType = oas.items.fold(MyriadOutputTypeToWomType)

      WomArrayType(arrayType)
  }
  implicit def s: Aux[String, WomType] =  at[String] { 
    _.toString |> ex
  }
}
