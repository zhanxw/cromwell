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
    val rootWorkflow =
      """|cwlVersion: v1.0
         |$graph:
         |- id: echo-workflow-1
         |  class: Workflow
         |  requirements:
         |    - class: StepInputExpressionRequirement
         |  inputs:
         |    tool: File
         |  outputs: []
         |  steps:
         |    root:
         |      run: echo-file-tool.cwl
         |      in:
         |        tool: tool
         |        in:
         |          valueFrom: $(inputs.tool.nameroot)
         |      out: [out]
         |
         |- id: echo-workflow-2
         |  class: Workflow
         |  requirements:
         |    - class: StepInputExpressionRequirement
         |  inputs:
         |    tool: File
         |  outputs: []
         |  steps:
         |    root1:
         |      run: inner-workflow.cwl
         |      in:
         |        tool: tool
         |        in:
         |          valueFrom: $(inputs.tool.nameroot)
         |      out: [out]
         |    root2:
         |      run: "#echo-workflow-1"
         |      in:
         |        tool: tool
         |        in:
         |          valueFrom: $(inputs.tool.nameroot)
         |      out: [out]
      """.stripMargin
    
    val innerWorkflow =
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
    tempDir./("inner-workflow.cwl").write(innerWorkflow)
    val workflowFile = tempDir./("workflow.cwl")
    workflowFile.write(rootWorkflow)

    val preProcessed = CwlPreProcessor.preProcessCwlFile(workflowFile, Option("echo-workflow-2")) match {
      case Left(errors) => fail(errors.toList.mkString(", "))
      case Right(processed) => processed
    }
    
    validate(
      preProcessed,
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
          |      "run" : {
          |        "cwlVersion" : "v1.0",
          |        "class" : "CommandLineTool",
          |        "baseCommand" : [
          |          "echo"
          |        ],
          |        "inputs" : [
          |          {
          |            "type" : "string",
          |            "inputBinding" : {
          |              "position" : 1
          |            },
          |            "id" : "file://${tempDir.pathAsString}/echo-file-tool.cwl#in"
          |          }
          |        ],
          |        "outputs" : [
          |          {
          |            "type" : "string",
          |            "valueFrom" : "hello",
          |            "id" : "file://${tempDir.pathAsString}/echo-file-tool.cwl#out"
          |          }
          |        ],
          |        "id" : "file://${tempDir.pathAsString}/echo-file-tool.cwl"
          |      },
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
