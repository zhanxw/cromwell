package cromwell.engine.workflow.lifecycle.execution.preparation

import cats.syntax.validated._
import cromwell.core.ExecutionIndex.ExecutionIndex
import cromwell.engine.workflow.lifecycle.execution.OutputStore
import lenthall.validation.ErrorOr.ErrorOr
import shapeless.Poly1
import wdl4s.wdl.values.WdlValue
import wdl4s.wom.expression.{IoFunctionSet, WomExpression}
import wdl4s.wom.graph.GraphNodePort.OutputPort
import wdl4s.wom.graph.InstantiatedExpression

object InputPointerToWdlValue extends Poly1 {
  implicit def fromWdlValue = at[WdlValue] { wdlValue => (_: Map[String, WdlValue],
                                                          _: IoFunctionSet,
                                                          _: OutputStore,
                                                          _: ExecutionIndex) => wdlValue.validNel: ErrorOr[WdlValue]
  }

  implicit def fromOutputPort = at[OutputPort] { port => (_: Map[String, WdlValue],
                                                          _: IoFunctionSet,
                                                          outputStore: OutputStore,
                                                          index : ExecutionIndex) =>
    outputStore
      .get(port, index).map(_.validNel)
      .getOrElse(s"Cannot find a value for ${port.name}".invalidNel): ErrorOr[WdlValue]
  }

  implicit def fromWomExpression = at[WomExpression] { womExpression => (knownValues: Map[String, WdlValue],
                                                                         ioFunctions: IoFunctionSet,
                                                                         _: OutputStore,
                                                                         _ : ExecutionIndex) =>
    womExpression.evaluateValue(knownValues, ioFunctions): ErrorOr[WdlValue]
  }

  implicit def fromInstantiatedExpression = at[InstantiatedExpression] { _ =>(_: Map[String, WdlValue],
                                                                                 _: IoFunctionSet,
                                                                                 _: OutputStore,
                                                                                 _ : ExecutionIndex) =>
    s"Cannot find a value for blah".invalidNel: ErrorOr[WdlValue]
  }
}
