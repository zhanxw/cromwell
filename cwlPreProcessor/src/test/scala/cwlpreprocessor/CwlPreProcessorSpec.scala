package cwlpreprocessor

import better.files.File
import cats.data.NonEmptyList
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
    validate(makeTestRoot("simple_workflow"), None) { mockSaladingFunction =>
      mockSaladingFunction.expects(echoFileTool).onCall(CwlPreProcessor.saladCwlFile)
    }
  }

  it should "flatten file with a self reference" in {
    validate(makeTestRoot("self_reference"), Option("echo-workflow-2")) { _ => }
  }

  it should "flatten file with a sub workflow and self reference" in {
    val testRoot = makeTestRoot("complex_workflow")
    val subWorkflow =  testRoot / "sub" / "sub_workflow.cwl"

    validate(testRoot, Option("echo-workflow-2")) { mockSaladingFunction =>
      mockSaladingFunction.expects(echoFileTool).onCall(CwlPreProcessor.saladCwlFile)
      mockSaladingFunction.expects(subWorkflow).onCall(CwlPreProcessor.saladCwlFile)
    }
  }

  it should "detect cyclic dependencies in the same file and fail" in {
    val testRoot = makeTestRoot("same_file_cyclic_dependency")

    validate(testRoot, Option("echo-workflow-2"),
      expectedFailure = Option(
        NonEmptyList.one(s"Found a circular dependency on file://$testRoot/root_workflow.cwl#echo-workflow-2")
      )
    ) { _ => }
  }

  it should "detect transitive cyclic dependencies (A => B => C => A) and fail" in {
    val testRoot = makeTestRoot("transitive_cyclic_dependency")

    val subWorkflow1 = testRoot / "sub_workflow_1.cwl"
    val subWorkflow2 =  testRoot / "sub_workflow_2.cwl"

    validate(testRoot, None,
      expectedFailure = Option(
        NonEmptyList.one(s"Found a circular dependency on file://$testRoot/root_workflow.cwl")
      )
    ) { mockSaladingFunction =>
      mockSaladingFunction.expects(subWorkflow1).onCall(CwlPreProcessor.saladCwlFile)
      mockSaladingFunction.expects(subWorkflow2).onCall(CwlPreProcessor.saladCwlFile)
    }
  }

  def makeTestRoot(testDirectoryName: String) = resourcesRoot / testDirectoryName

  def validate[T](testRoot: File,
                  root: Option[String],
                  expectedFailure: Option[NonEmptyList[String]] = None
                 )(additionalValidation: MockFunction1[File, Checked[String]] => T) = {
    val rootWorkflow = testRoot / "root_workflow.cwl"

    // Mocking the salad function allows us to validate how many times it is called exactly and with which parameters
    val mockSaladingFunction = mockFunction[File, Checked[String]]
    val preProcessor = new CwlPreProcessor(mockSaladingFunction)

    val saladExpectations = additionalValidation
      // Always validate that the root is saladed
      .andThen(_ => mockSaladingFunction.expects(rootWorkflow).onCall(CwlPreProcessor.saladCwlFile))

    // Asserts that dependencies are only saladed once and exactly once
    inAnyOrder(saladExpectations(mockSaladingFunction))

    val process = preProcessor.preProcessCwlFile(rootWorkflow, root)

    (process, expectedFailure) match {
      case (Left(errors), Some(failures)) => errors shouldBe failures
      case (Left(errors), None) => fail("Unexpected failure to pre-process workflow: " + errors.toList.mkString(", "))
      case (Right(result), None) =>
        val expectationContent = (testRoot / "expected_result.json").contentAsString
          .replaceAll("<<RESOURCES_ROOT>>", resourcesRoot.pathAsString)

        io.circe.parser.parse(result) shouldBe io.circe.parser.parse(expectationContent)
      case (Right(_), Some(failures)) => fail("Unexpected success to pre-process workflow, was expecting failures: " + failures.toList.mkString(", "))
    }
  }
}
