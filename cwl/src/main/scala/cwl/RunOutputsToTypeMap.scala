package cwl

import cwl.Workflow.WorkflowOutputParameter
import shapeless.Poly1
import wom.types.WomType

object RunOutputsToTypeMap extends Poly1 {

  def handleCommandLine(clt: CommandLineTool): Map[String, WomType] = {
    clt.outputs.toList.foldLeft(Map.empty[String, WomType]) {
      (acc, out) =>
        acc ++
          out.
            `type`.
            map(_.fold(MyriadOutputTypeToWomType)).
            map(out.id -> _).
            toList.
            toMap
    }
  }

  def handleExpressionTool(et: ExpressionTool): Map[String, WomType] = {
    et.outputs.toList.foldLeft(Map.empty[String, WomType]) {
      (acc, out) =>
        acc ++
          out.
            `type`.
            map(_.fold(MyriadOutputTypeToWomType)).
            map(out.id -> _).
            toList.
            toMap
    }
  }

  implicit def commandLineTool =
    at[CommandLineTool] {
      clt =>
          handleCommandLine(clt)
    }

  implicit def string = at[String] {
    _ =>
      Map.empty[String, WomType]
  }

  implicit def expressionTool = at[ExpressionTool] {
    et =>
      handleExpressionTool(et)
  }

  implicit def workflow = at[Workflow] {
    wf =>
//      val stepsOutputs = wf.steps.toList.flatMap(_.typedOutputs.toList).toMap

      wf.outputs.flatMap({ workflowOutputParameter: WorkflowOutputParameter =>
        workflowOutputParameter
          .`type`
          .map(_.fold(MyriadOutputTypeToWomType))
          .map(workflowOutputParameter.id -> _)
      }).toMap
  }
}

