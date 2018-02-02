package cwlpreprocessor

import better.files.File
import common.Checked
import org.scalamock.function.MockFunction1
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.language.postfixOps

class CwlPreProcessorSpec extends FlatSpec with Matchers with MockFactory {
  behavior of "CwlPreProcessor"

  val resourcesRoot = File(getClass.getResource(".").getPath)
  val echoFileTool = resourcesRoot / "echo_tool.cwl"

  it should "flatten a simple file" in {
    validate("simple_workflow", None) { mockSaladingFunction =>
      mockSaladingFunction.expects(echoFileTool).onCall(CwlPreProcessor.saladCwlFile)
    }
  }

  it should "flatten file with a self reference" in {
    validate("self_reference", Option("echo-workflow-2")) { _ => }
  }

  it should "flatten file with a sub workflow and self reference" in {
    val subWorkflow =  resourcesRoot / "complex_workflow" / "sub" / "sub_workflow.cwl"
    validate("complex_workflow", Option("echo-workflow-2")) { mockSaladingFunction =>
      mockSaladingFunction.expects(echoFileTool).onCall(CwlPreProcessor.saladCwlFile)
      mockSaladingFunction.expects(subWorkflow).onCall(CwlPreProcessor.saladCwlFile)
    }
  }

  it should "detect cyclic dependencies in the same file and fail" in {
    validate("same_file_cyclic_dependency", Option("echo-workflow-2")) {  _ => }
  }

  def validate[T](testDirectory: String, root: Option[String])(additionalValidation: MockFunction1[File, Checked[String]] => T) = {
    val testRoot = resourcesRoot / testDirectory
    val rootWorkflow = testRoot / "root_workflow.cwl"

    // Mocking the salad function allows us to validate how many times it is called exactly and with which parameters
    val mockSaladingFunction = mockFunction[File, Checked[String]]
    val preProcessor = new CwlPreProcessor(mockSaladingFunction)

    val saladExpectations = additionalValidation
      // Always validate that the root is saladed
      .andThen(_ => mockSaladingFunction.expects(rootWorkflow).onCall(CwlPreProcessor.saladCwlFile))

    // Asserts that dependencies are only saladed once and exactly once
    inAnyOrder(saladExpectations(mockSaladingFunction))

    val preProcessed = preProcessor.preProcessCwlFile(rootWorkflow, root) match {
      case Left(errors) => fail("Failed to pre-process workflow: " + errors.toList.mkString(", "))
      case Right(processed) => processed
    }

    val expectationContent = (testRoot / "expected_result.json")
      .contentAsString
      .replaceAll("<<RESOURCES_ROOT>>", resourcesRoot.pathAsString)

    io.circe.parser.parse(preProcessed) shouldBe io.circe.parser.parse(expectationContent)
  }
}
