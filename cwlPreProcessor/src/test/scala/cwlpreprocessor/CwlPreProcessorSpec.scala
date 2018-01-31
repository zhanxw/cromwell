package cwlpreprocessor

import better.files.File
import org.scalatest.{FlatSpec, Matchers}

class CwlPreProcessorSpec extends FlatSpec with Matchers {
  val tempDir = File.newTemporaryDirectory("CwlPreProcessorSpec")

  behavior of "CwlPreProcessor"

  def validate(result: String, expectation: String) = {
    io.circe.parser.parse(result) shouldBe io.circe.parser.parse(expectation)
  }

  it should "resolve relative imports" in {
    val workflow =
      """|cwlVersion: v1.0
         |class: Workflow
         |
         |requirements:
         |  - class: StepInputExpressionRequirement
         |
         |inputs:
         |  tool: File
         |
         |outputs: []
         |
         |steps:
         |  root:
         |    run: echo-file-tool.cwl
         |    in:
         |      tool: tool
         |      in:
         |        valueFrom: $(inputs.tool.nameroot)
         |    out: [out]
      """.stripMargin

    val echoFileTool =
      """|cwlVersion: v1.0
         |class: CommandLineTool
         |baseCommand: [echo]
         |inputs:
         |  in:
         |    type: string
         |    inputBinding:
         |      position: 1
         |outputs:
         |  out:
         |    type: string
         |    valueFrom: "hello"
      """.stripMargin

    tempDir./("echo-file-tool.cwl").write(echoFileTool)
    val workflowFile = tempDir./("workflow.cwl")
    workflowFile.write(workflow)

    val preProcessed = CwlPreProcessor.preProcessCwlFileAndZipDependencies(workflowFile).right.getOrElse(fail("Failed to pre process workflow"))

    val dependencies = preProcessed.dependencyZip.unzip()
  
    (dependencies / tempDir.pathAsString / "workflow.cwl").exists shouldBe true
    (dependencies / tempDir.pathAsString / "echo-file-tool.cwl").exists shouldBe true
    
    validate(
      preProcessed.workflow,
      s"""|{
          |  "cwlVersion" : "v1.0",
          |  "class" : "Workflow",
          |  "requirements" : [
          |    {
          |      "class" : "StepInputExpressionRequirement"
          |    }
          |  ],
          |  "inputs" : [
          |    {
          |      "type" : "File",
          |      "id" : "file://${tempDir.pathAsString}/workflow.cwl#tool"
          |    }
          |  ],
          |  "outputs" : [
          |  ],
          |  "steps" : [
          |    {
          |      "run" : "file://${tempDir.pathAsString}/echo-file-tool.cwl",
          |      "in" : [
          |        {
          |          "valueFrom" : "$$(inputs.tool.nameroot)",
          |          "id" : "file://${tempDir.pathAsString}/workflow.cwl#root/in"
          |        },
          |        {
          |          "source" : "file://${tempDir.pathAsString}/workflow.cwl#tool",
          |          "id" : "file://${tempDir.pathAsString}/workflow.cwl#root/tool"
          |        }
          |      ],
          |      "out" : [
          |        "file://${tempDir.pathAsString}/workflow.cwl#root/out"
          |      ],
          |      "id" : "file://${tempDir.pathAsString}/workflow.cwl#root"
          |    }
          |  ],
          |  "id" : "file://${tempDir.pathAsString}/workflow.cwl"
          |}
       """.stripMargin
    )

  }
}
