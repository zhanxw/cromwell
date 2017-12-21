# A very common use case: stringy the inputs JSON and provide that file as another input file.

cwlVersion: v1.0
$graph:
- id: stringify_inputs
  class: CommandLineTool
  baseCommand: ['grep', 'number', 'inputs.json']
  requirements:
      - class: DockerRequirement
        dockerPull: "python:3.5.0"
      - class: InitialWorkDirRequirement
        listing:
            - entryname: 'inputs.json'
              entry: $(JSON.stringify(inputs))

  stdout: "number_field"

  # TODO CWL: Set the types more appropriately (depends on issue #3059)
  inputs:
      - id: number
        type: string
        default: 27
      - id: str
        type: string
        default: wooooo
      - id: boolean
        type: string
        default: True
  outputs:
      - id: number_field_output
        type: string
        outputBinding:
          glob: number_field
          loadContents: true
          outputEval: $(self[0].contents.trim())
