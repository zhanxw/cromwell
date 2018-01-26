import better.files.File
import centaur.cwl.CentaurCwlRunnerRunMode.ProcessedWorkflow
import centaur.cwl.PAPIPreprocessor
import com.typesafe.config.ConfigFactory
import org.scalatest.{Assertion, BeforeAndAfterAll, FlatSpec, Matchers}

class PAPIPreprocessorSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  behavior of "PAPIPreProcessor"

  val tempDir = File.newTemporaryDirectory()
  val pAPIPreprocessor = new PAPIPreprocessor(ConfigFactory.load())

  def validate(result: String, expectation: String): Assertion = {
    val parsedResult = io.circe.yaml.parser.parse(result).right.get
    val parsedExpectation = io.circe.yaml.parser.parse(expectation).right.get

    // This is an actual Json comparison from circe
    parsedResult shouldBe parsedExpectation
  }

  def validate(result: ProcessedWorkflow, expectation: String): Assertion = {
    validate(result.content, expectation)
  }

  override def afterAll(): Unit = {
    tempDir.delete(swallowIOExceptions = true)
    super.afterAll()
  }

  it should "prefix files and directories in inputs" in {
    validate(
      pAPIPreprocessor.preProcessInput(
        """|{
           |  "input": {
           |    "null": null,
           |    "file": {
           |      "location": "whale.txt",
           |      "class": "File",
           |      "secondaryFiles": [
           |        {
           |          "class": File,
           |          "location": "hello.txt"
           |        }
           |      ],
           |      "default": {
           |        "location": "default_whale.txt",
           |        "class": "File",
           |      }
           |    },
           |    "directory": {
           |      "location": "ref.fasta",
           |      "class": "Directory",
           |      "listing": [
           |        {
           |          "class": File,
           |          "location": "hello.txt"
           |        }
           |      ]
           |    }
           |  }
           |}
           |""".stripMargin),
      """|{
         |  "input": {
         |    "null": null,
         |    "file": {
         |      "location": "gs://centaur-cwl-conformance/cwl-inputs/whale.txt",
         |      "class": "File",
         |      "secondaryFiles": [
         |        {
         |          "class": File,
         |          "location": "gs://centaur-cwl-conformance/cwl-inputs/hello.txt"
         |        }
         |      ],
         |      "default": {
         |        "location": "gs://centaur-cwl-conformance/cwl-inputs/default_whale.txt",
         |        "class": "File",
         |      }
         |    },
         |    "directory": {
         |      "location": "gs://centaur-cwl-conformance/cwl-inputs/ref.fasta",
         |      "class": "Directory",
         |      "listing": [
         |        {
         |          "class": File,
         |          "location": "gs://centaur-cwl-conformance/cwl-inputs/hello.txt"
         |        }
         |      ]
         |    }
         |  }
         |}
         |""".stripMargin
    )
  }

  // Ignored because until cwltool accepts gcs paths in workflows we can't prefix default locations
  it should "prefix files and directories in workflow" ignore {
    validate(
      pAPIPreprocessor.preProcessWorkflow(
        """|class: CommandLineTool
           |cwlVersion: v1.0
           |requirements:
           |  - class: DockerRequirement
           |    dockerPull: ubuntu:latest
           |inputs:
           |  - id: reference
           |    type: File
           |    inputBinding: { position: 2 }
           |    default:
           |      class: File
           |      location: args.py
           |
           |outputs:
           |  args: string[]
           |
           |baseCommand: python
           |arguments: ["bwa", "mem"]
           |""".stripMargin, tempDir),
      """|class: CommandLineTool
         |cwlVersion: v1.0
         |requirements:
         |  - class: DockerRequirement
         |    dockerPull: ubuntu:latest
         |inputs:
         |  - id: reference
         |    type: File
         |    inputBinding: { position: 2 }
         |    default:
         |      class: File
         |      location: gs://centaur-cwl-conformance/cwl-inputs/args.py
         |
         |outputs:
         |  args: string[]
         |
         |baseCommand: python
         |arguments: ["bwa", "mem"]
         |""".stripMargin
    )
  }

  it should "add default docker image if there's no requirements" in {
    validate(
      pAPIPreprocessor.preProcessWorkflow(
        """|class: CommandLineTool
           |cwlVersion: v1.0
           |inputs:
           |  - id: reference
           |    type: File
           |    inputBinding: { position: 2 }
           |
           |outputs:
           |  args: string[]
           |
           |baseCommand: python
           |arguments: ["bwa", "mem"]
           |""".stripMargin, tempDir),
      """|class: CommandLineTool
         |requirements:
         |  - class: DockerRequirement
         |    dockerPull: ubuntu:latest
         |cwlVersion: v1.0
         |inputs:
         |  - id: reference
         |    type: File
         |    inputBinding: { position: 2 }
         |
         |outputs:
         |  args: string[]
         |
         |baseCommand: python
         |arguments: ["bwa", "mem"]
         |""".stripMargin
    )
  }

  it should "append default docker image to existing requirements as an array" in {
    validate(
      pAPIPreprocessor.preProcessWorkflow(
        """|class: CommandLineTool
           |requirements:
           |  - class: EnvVarRequirement
           |    envDef:
           |      TEST_ENV: $(inputs.in)
           |cwlVersion: v1.0
           |inputs:
           |  - id: reference
           |    type: File
           |    inputBinding: { position: 2 }
           |
           |outputs:
           |  args: string[]
           |
           |baseCommand: python
           |arguments: ["bwa", "mem"]
           |""".stripMargin, tempDir),
      """|class: CommandLineTool
         |requirements:
         |  - class: EnvVarRequirement
         |    envDef:
         |      TEST_ENV: $(inputs.in)
         |  - class: DockerRequirement
         |    dockerPull: ubuntu:latest
         |cwlVersion: v1.0
         |inputs:
         |  - id: reference
         |    type: File
         |    inputBinding: { position: 2 }
         |
         |outputs:
         |  args: string[]
         |
         |baseCommand: python
         |arguments: ["bwa", "mem"]
         |""".stripMargin
    )
  }

  it should "append default docker image to existing requirements as an object" in {
    validate(
      pAPIPreprocessor.preProcessWorkflow(
        """|class: CommandLineTool
           |cwlVersion: v1.0
           |inputs:
           |  in: string
           |outputs:
           |  out:
           |    type: File
           |    outputBinding:
           |      glob: out
           |
           |requirements:
           |  EnvVarRequirement:
           |    envDef:
           |      TEST_ENV: $(inputs.in)
           |
           |baseCommand: ["/bin/bash", "-c", "echo $TEST_ENV"]
           |
           |stdout: out
           |""".stripMargin, tempDir),
      """|class: CommandLineTool
         |cwlVersion: v1.0
         |inputs:
         |  in: string
         |outputs:
         |  out:
         |    type: File
         |    outputBinding:
         |      glob: out
         |
         |requirements:
         |  EnvVarRequirement:
         |    envDef:
         |      TEST_ENV: $(inputs.in)
         |  DockerRequirement:
         |    dockerPull: ubuntu:latest
         |
         |baseCommand: ["/bin/bash", "-c", "echo $TEST_ENV"]
         |
         |stdout: out
         |""".stripMargin
    )
  }

  it should "add default docker image in multi tool/workflow files" in {
    validate(
      pAPIPreprocessor.preProcessWorkflow(
        """|#!/usr/bin/env cwl-runner
           |
           |cwlVersion: v1.0
           |$graph:
           |
           |- id: echo
           |  class: CommandLineTool
           |  inputs: []
           |  outputs: []
           |  baseCommand: "echo"
           |  arguments: ["-n", "foo"]
           |
           |- id: main
           |  class: Workflow
           |  inputs: []
           |  requirements:
           |    - class: ScatterFeatureRequirement
           |  steps:
           |    step1:
           |      scatter: [echo_in1, echo_in2]
           |      scatterMethod: flat_crossproduct
           |      in: []
           |      out: []
           |
           |  outputs: []
           |""".stripMargin, tempDir),
      """|#!/usr/bin/env cwl-runner
         |
         |cwlVersion: v1.0
         |$graph:
         |
         |- id: echo
         |  class: CommandLineTool
         |  requirements:
         |  - class: DockerRequirement
         |    dockerPull: ubuntu:latest
         |  inputs: []
         |  outputs: []
         |  baseCommand: "echo"
         |  arguments: ["-n", "foo"]
         |
         |- id: main
         |  class: Workflow
         |  inputs: []
         |  requirements:
         |    - class: ScatterFeatureRequirement
         |    - class: DockerRequirement
         |      dockerPull: ubuntu:latest
         |  steps:
         |    step1:
         |      scatter: [echo_in1, echo_in2]
         |      scatterMethod: flat_crossproduct
         |      in: []
         |      out: []
         |
         |  outputs: []
         |""".stripMargin
    )
  }

  it should "not replace existing docker requirement in an object" in {
    val workflow = """|class: CommandLineTool
                      |cwlVersion: v1.0
                      |requirements:
                      |  DockerRequirement:
                      |    dockerPull: python27:slim
                      |inputs:
                      |  - id: reference
                      |    type: File
                      |    inputBinding: { position: 2 }
                      |
                      |outputs:
                      |  args: string[]
                      |
                      |baseCommand: python
                      |arguments: ["bwa", "mem"]
                      |""".stripMargin
    validate(pAPIPreprocessor.preProcessWorkflow(workflow, tempDir), workflow)
  }

  it should "not replace existing docker hint in an object" in {
    val workflow = """|class: CommandLineTool
                      |cwlVersion: v1.0
                      |hints:
                      |  DockerRequirement:
                      |    dockerPull: python27:slim
                      |inputs:
                      |  - id: reference
                      |    type: File
                      |    inputBinding: { position: 2 }
                      |
                      |outputs:
                      |  args: string[]
                      |
                      |baseCommand: python
                      |arguments: ["bwa", "mem"]
                      |""".stripMargin
    validate(pAPIPreprocessor.preProcessWorkflow(workflow, tempDir), workflow)
  }

  it should "not replace existing docker hint in an array" in {
    val workflow = """|class: CommandLineTool
                      |cwlVersion: v1.0
                      |hints:
                      |- class: DockerRequirement
                      |  dockerPull: python27:slim
                      |inputs:
                      |  - id: reference
                      |    type: File
                      |    inputBinding: { position: 2 }
                      |
                      |outputs:
                      |  args: string[]
                      |
                      |baseCommand: python
                      |arguments: ["bwa", "mem"]
                      |""".stripMargin
    validate(pAPIPreprocessor.preProcessWorkflow(workflow, tempDir), workflow)
  }

  it should "not replace existing docker requirement in an array" in {
    val workflow = """|class: CommandLineTool
                      |cwlVersion: v1.0
                      |requirements:
                      |- class: DockerRequirement
                      |  dockerPull: python27:slim
                      |inputs:
                      |  - id: reference
                      |    type: File
                      |    inputBinding: { position: 2 }
                      |
                      |outputs:
                      |  args: string[]
                      |
                      |baseCommand: python
                      |arguments: ["bwa", "mem"]
                      |""".stripMargin
    validate(pAPIPreprocessor.preProcessWorkflow(workflow, tempDir), workflow)
  }

  it should "throw an exception if yaml / json can't be parse" in {
    val invalid =
      """
        |{ [invalid]: }
      """.stripMargin

    an[Exception] shouldBe thrownBy(pAPIPreprocessor.preProcessWorkflow(invalid, tempDir))
  }

  it should "process nested files" in {
    val rootWorkflow = """|class: Workflow
                          |cwlVersion: v1.0
                          |steps:
                          |  step1:
                          |    run: nestedWorkflow.cwl
                          |  step2:
                          |    run: tool1.cwl
                          |""".stripMargin

    val nestedWorkflowContent = """|class: Workflow
                                   |cwlVersion: v1.0
                                   |steps:
                                   |  step1:
                                   |    run: tool2.cwl
                                   |""".stripMargin

    val toolContent = """|class: CommandLineTool
                         |cwlVersion: v1.0
                         |""".stripMargin

    (tempDir / "nestedWorkflow.cwl").write(nestedWorkflowContent)
    (tempDir / "tool1.cwl").write(toolContent)
    (tempDir / "tool2.cwl").write(toolContent)

    val processed = pAPIPreprocessor.preProcessWorkflow(rootWorkflow, tempDir)

    validate(processed, """|class: Workflow
                           |cwlVersion: v1.0
                           |requirements:
                           |  - class: DockerRequirement
                           |    dockerPull: ubuntu:latest
                           |steps:
                           |  step1:
                           |    run: nestedWorkflow.cwl
                           |  step2:
                           |    run: tool1.cwl
                           |""".stripMargin)

    val dependencies = processed.dependencies
    dependencies.size shouldBe 3

    def dependencyContent(name: String) = {
      dependencies.find(_.name == name).get.content
    }

    validate(dependencyContent("nestedWorkflow.cwl"), """|class: Workflow
                                                         |cwlVersion: v1.0
                                                         |requirements:
                                                         |  - class: DockerRequirement
                                                         |    dockerPull: ubuntu:latest
                                                         |steps:
                                                         |  step1:
                                                         |    run: tool2.cwl
                                                         |""".stripMargin)

    validate(dependencyContent("tool1.cwl"), """|class: CommandLineTool
                                                |requirements:
                                                |  - class: DockerRequirement
                                                |    dockerPull: ubuntu:latest
                                                |cwlVersion: v1.0
                                                |""".stripMargin)

    validate(dependencyContent("tool2.cwl"), """|class: CommandLineTool
                                                |requirements:
                                                |  - class: DockerRequirement
                                                |    dockerPull: ubuntu:latest
                                                |cwlVersion: v1.0
                                                |""".stripMargin)
  }
}
