package cwlpreprocessor

import better.files.File
import common.Checked
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.language.postfixOps

class CwlPreProcessorSpec extends FlatSpec with Matchers with MockFactory {
  val tempDir = File.newTemporaryDirectory("CwlPreProcessorSpec")

  behavior of "CwlPreProcessor"

  def validate(result: String, expectation: String) = {
    io.circe.parser.parse(result) shouldBe io.circe.parser.parse(expectation)
  }

  it should "flatten workflow with relative imports" in {
    val rootWorkflowContent =
      """|cwlVersion: v1.0
         |$graph:
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
         |
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
      """.stripMargin

    val innerWorkflowContent =
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

    val echoFileToolContent =
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

    val echoFileTool = tempDir./("echo-file-tool.cwl")
    echoFileTool.write(echoFileToolContent)

    val innerWorkflow = tempDir./("inner-workflow.cwl")
    innerWorkflow.write(innerWorkflowContent)

    val rootWorkflow = tempDir./("workflow.cwl")
    rootWorkflow.write(rootWorkflowContent)

    val mockSaladingFunction = mockFunction[File, Checked[String]]

    val preProcessor = new CwlPreProcessor(mockSaladingFunction)

    // Asserts that dependencies are only saladed once, even if they appear multiple times throughout the whole workflow
    // In this case echoFileTool appears in 2 different places but should only be saladed once
    inAnyOrder {
      mockSaladingFunction.expects(echoFileTool).onCall(CwlPreProcessor.saladCwlFile)
      mockSaladingFunction.expects(innerWorkflow).onCall(CwlPreProcessor.saladCwlFile)
      mockSaladingFunction.expects(rootWorkflow).onCall(CwlPreProcessor.saladCwlFile)
    }

    val preProcessed = preProcessor.preProcessCwlFile(rootWorkflow, Option("echo-workflow-2")) match {
      case Left(errors) => fail("Failed to pre-process workflow: " + errors.toList.mkString(", "))
      case Right(processed) => processed
    }

    // This is intense but is the saladed, flattened version of the cwls above.
    // Notice specifically that the run sections have been inlined.
    val expectation =
      s"""|{
         |  "id": "file://${tempDir.pathAsString}/workflow.cwl#echo-workflow-2",
         |  "class": "Workflow",
         |  "requirements": [
         |    {
         |      "class": "StepInputExpressionRequirement"
         |    }
         |  ],
         |  "inputs": [
         |    {
         |      "type": "File",
         |      "id": "file://${tempDir.pathAsString}/workflow.cwl#echo-workflow-2/tool"
         |    }
         |  ],
         |  "outputs": [],
         |  "steps": [
         |    {
         |      "run": {
         |        "cwlVersion": "v1.0",
         |        "class": "Workflow",
         |        "requirements": [
         |          {
         |            "class": "StepInputExpressionRequirement"
         |          }
         |        ],
         |        "inputs": [
         |          {
         |            "type": "File",
         |            "id": "file://${tempDir.pathAsString}/inner-workflow.cwl#tool"
         |          }
         |        ],
         |        "outputs": [],
         |        "steps": [
         |          {
         |            "run": {
         |              "cwlVersion": "v1.0",
         |              "class": "CommandLineTool",
         |              "baseCommand": [
         |                "echo"
         |              ],
         |              "inputs": [
         |                {
         |                  "type": "string",
         |                  "inputBinding": {
         |                    "position": 1
         |                  },
         |                  "id": "file://${tempDir.pathAsString}/echo-file-tool.cwl#in"
         |                }
         |              ],
         |              "outputs": [
         |                {
         |                  "type": "string",
         |                  "valueFrom": "hello",
         |                  "id": "file://${tempDir.pathAsString}/echo-file-tool.cwl#out"
         |                }
         |              ],
         |              "id": "file://${tempDir.pathAsString}/echo-file-tool.cwl"
         |            },
         |            "in": [
         |              {
         |                "valueFrom": "$$(inputs.tool.nameroot)",
         |                "id": "file://${tempDir.pathAsString}/inner-workflow.cwl#root/in"
         |              },
         |              {
         |                "source": "file://${tempDir.pathAsString}/inner-workflow.cwl#tool",
         |                "id": "file://${tempDir.pathAsString}/inner-workflow.cwl#root/tool"
         |              }
         |            ],
         |            "out": [
         |              "file://${tempDir.pathAsString}/inner-workflow.cwl#root/out"
         |            ],
         |            "id": "file://${tempDir.pathAsString}/inner-workflow.cwl#root"
         |          }
         |        ],
         |        "id": "file://${tempDir.pathAsString}/inner-workflow.cwl"
         |      },
         |      "in": [
         |        {
         |          "valueFrom": "$$(inputs.tool.nameroot)",
         |          "id": "file://${tempDir.pathAsString}/workflow.cwl#echo-workflow-2/root1/in"
         |        },
         |        {
         |          "source": "file://${tempDir.pathAsString}/workflow.cwl#echo-workflow-2/tool",
         |          "id": "file://${tempDir.pathAsString}/workflow.cwl#echo-workflow-2/root1/tool"
         |        }
         |      ],
         |      "out": [
         |        "file://${tempDir.pathAsString}/workflow.cwl#echo-workflow-2/root1/out"
         |      ],
         |      "id": "file://${tempDir.pathAsString}/workflow.cwl#echo-workflow-2/root1"
         |    },
         |    {
         |      "run": {
         |        "id": "file://${tempDir.pathAsString}/workflow.cwl#echo-workflow-1",
         |        "class": "Workflow",
         |        "requirements": [
         |          {
         |            "class": "StepInputExpressionRequirement"
         |          }
         |        ],
         |        "inputs": [
         |          {
         |            "type": "File",
         |            "id": "file://${tempDir.pathAsString}/workflow.cwl#echo-workflow-1/tool"
         |          }
         |        ],
         |        "outputs": [],
         |        "steps": [
         |          {
         |            "run": {
         |              "cwlVersion": "v1.0",
         |              "class": "CommandLineTool",
         |              "baseCommand": [
         |                "echo"
         |              ],
         |              "inputs": [
         |                {
         |                  "type": "string",
         |                  "inputBinding": {
         |                    "position": 1
         |                  },
         |                  "id": "file://${tempDir.pathAsString}/echo-file-tool.cwl#in"
         |                }
         |              ],
         |              "outputs": [
         |                {
         |                  "type": "string",
         |                  "valueFrom": "hello",
         |                  "id": "file://${tempDir.pathAsString}/echo-file-tool.cwl#out"
         |                }
         |              ],
         |              "id": "file://${tempDir.pathAsString}/echo-file-tool.cwl"
         |            },
         |            "in": [
         |              {
         |                "valueFrom": "$$(inputs.tool.nameroot)",
         |                "id": "file://${tempDir.pathAsString}/workflow.cwl#echo-workflow-1/root/in"
         |              },
         |              {
         |                "source": "file://${tempDir.pathAsString}/workflow.cwl#echo-workflow-1/tool",
         |                "id": "file://${tempDir.pathAsString}/workflow.cwl#echo-workflow-1/root/tool"
         |              }
         |            ],
         |            "out": [
         |              "file://${tempDir.pathAsString}/workflow.cwl#echo-workflow-1/root/out"
         |            ],
         |            "id": "file://${tempDir.pathAsString}/workflow.cwl#echo-workflow-1/root"
         |          }
         |        ]
         |      },
         |      "in": [
         |        {
         |          "valueFrom": "$$(inputs.tool.nameroot)",
         |          "id": "file://${tempDir.pathAsString}/workflow.cwl#echo-workflow-2/root2/in"
         |        },
         |        {
         |          "source": "file://${tempDir.pathAsString}/workflow.cwl#echo-workflow-2/tool",
         |          "id": "file://${tempDir.pathAsString}/workflow.cwl#echo-workflow-2/root2/tool"
         |        }
         |      ],
         |      "out": [
         |        "file://${tempDir.pathAsString}/workflow.cwl#echo-workflow-2/root2/out"
         |      ],
         |      "id": "file://${tempDir.pathAsString}/workflow.cwl#echo-workflow-2/root2"
         |    }
         |  ]
         |}
      """.stripMargin

      validate(preProcessed, expectation)
  }
}
