package wom.graph.expression

import common.Checked
import common.validation.ErrorOr.ErrorOr
import wom.expression.WomExpression
import wom.graph.GraphNode.GraphNodeSetter
import wom.graph.GraphNodePort.{InputPort, OutputPort}
import wom.graph.{CommandCallNode, WomIdentifier}
import wom.types.WomType
import wom.values.WomValue

object AnonymousExpressionNode {
  type AnonymousExpressionConstructor[T] = (WomIdentifier, WomExpression, WomType, Map[String, InputPort]) => T

  def fromInputMapping[T <: AnonymousExpressionNode](identifier: WomIdentifier,
                                                     expression: WomExpression,
                                                     inputMapping: Map[String, OutputPort],
                                                     constructor: AnonymousExpressionConstructor[T]): ErrorOr[T] = {
    ExpressionNode.buildFromConstructor(constructor)(identifier, expression, inputMapping)
  }
}

/**
  * An expression node that is purely an internal expression and shouldn't be visible outside the graph
  */
trait AnonymousExpressionNode extends ExpressionNode

case class PlainAnonymousExpressionNode(override val identifier: WomIdentifier,
                                        override val womExpression: WomExpression,
                                        override val womType: WomType,
                                        override val inputMapping: Map[String, InputPort])
  extends ExpressionNode(identifier, womExpression, womType, inputMapping) with AnonymousExpressionNode

object TaskCallInputExpressionNode {
  def default(identifier: WomIdentifier, womExpression: WomExpression, womType: WomType, inputMapping: Map[String, InputPort]) = {
    apply(identifier, womExpression, womType, inputMapping)
  }
  def apply(identifier: WomIdentifier, womExpression: WomExpression, womType: WomType, inputMapping: Map[String, InputPort]) = {
    new TaskCallInputExpressionNode(identifier, womExpression, womType, inputMapping, Set.empty)
  }
}

case class TaskCallInputExpressionNode(override val identifier: WomIdentifier,
                                       override val womExpression: WomExpression,
                                       override val womType: WomType,
                                       override val inputMapping: Map[String, InputPort],
                                       additionalInputs: Set[OutputPort])
  extends ExpressionNode(identifier, womExpression, womType, inputMapping) with AnonymousExpressionNode {

  override def resolvedInputMappings(outputPortLookup: OutputPort => ErrorOr[WomValue]): Checked[Map[String, WomValue]] = {
    import cats.syntax.either._

    val taskOutputPorts = taskCallNodeReceivingInput.get(()).outputPorts.map(outputPort => outputPort.name -> outputPort).toMap
    for {
      defaultMappings <- super.resolvedInputMappings(outputPortLookup)
      taskMappings <- resolvePorts(outputPortLookup, taskOutputPorts)
    } yield defaultMappings ++ taskMappings
  }

  /**
    * The `GraphNodeSetter` that will have a `TaskCallNode` set into it. This is needed in the `WorkflowExecutionActor`
    * to be able to look up backend mapping for the target call in order to have the correct `IoFunctionSet` to
    * evaluate task call input expressions.
    */
  val taskCallNodeReceivingInput: GraphNodeSetter[CommandCallNode] = new GraphNodeSetter[CommandCallNode]
}
