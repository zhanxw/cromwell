package cwl

import cwl.ExpressionEvaluator.ECMAScriptExpressionWitness
import org.scalatest.{FlatSpec, Matchers}
import eu.timepit.refined.refineMV
import eu.timepit.refined.string._
import wom.expression.PlaceholderIoFunctionSet
import wom.values.WomInteger

import scala.util.Success

class ExpressionEvaluatorSpec extends FlatSpec with Matchers {

  it should "evaluate simple addition expressions" in {
    val parameterContext = ParameterContext().withInputs(Map.empty, PlaceholderIoFunctionSet)
    val x: ExpressionEvaluator.ECMAScriptExpression = refineMV[MatchesRegex[ECMAScriptExpressionWitness.T]]("$(3 + 3)")
    ExpressionEvaluator.evalExpression(x, parameterContext) should be(Success(WomInteger(6)))
  }

  it should "dereference inputs" in {
    val parameterContext = ParameterContext().withInputs(Map("x" -> WomInteger(3)), PlaceholderIoFunctionSet)
    val x: ExpressionEvaluator.ECMAScriptExpression = refineMV[MatchesRegex[ECMAScriptExpressionWitness.T]]("$(inputs.x + 3)")
    ExpressionEvaluator.evalExpression(x, parameterContext) should be(Success(WomInteger(6)))
  }

  it should "be able to run JSON.stringify" in {
    val parameterContext = ParameterContext().withInputs(Map("x" -> WomInteger(3)), PlaceholderIoFunctionSet)
    val x: ExpressionEvaluator.ECMAScriptExpression = refineMV[MatchesRegex[ECMAScriptExpressionWitness.T]]("""$(print("hello"))""")
    ExpressionEvaluator.evalExpression(x, parameterContext) should be(Success(WomInteger(6)))
  }
}
