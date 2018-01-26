package centaur.cwl

import better.files.File
import centaur.cwl.CentaurCwlRunnerRunMode.{ProcessedDependency, ProcessedWorkflow}
import io.circe.optics.JsonPath.root
import io.circe.{Json, yaml}

class CwlPreprocessor {

  protected def parse(value: String): Json = {
    yaml.parser.parse(value) match {
      case Left(error) => throw new Exception(error.getMessage)
      case Right(json) => json
    }
  }
  
  protected def printYaml(json: Json) = yaml.Printer.spaces2.copy(stringStyle = yaml.Printer.StringStyle.DoubleQuoted).pretty(json)

  // Parse value, apply f to it, and print it back to String using the printer
  protected def process(value: String, f: Json => Json, printer: Json => String) = {
    printer(f(parse(value)))
  }

  // Process and print back as JSON
  protected def processJson(value: String)(f: Json => Json): String = process(value, f, io.circe.Printer.spaces2.pretty)
  
  // Look for referenced cwls and pre-process them
  private def processDependencies(parentDirectory: File, f: Json => Json)(json: Json): List[ProcessedDependency] = {
    
    // The run section of a step can link to a cwl
    val runFileNames = root.steps.each.run.string.getAll(json).filterNot(_.startsWith("#"))

    runFileNames.flatMap({ fileName =>
      // For each file, process it, and add the result to the list of nested workflows
      val file = parentDirectory / fileName
      val fileContent = file.contentAsString
      val processed = collectDependencies(fileContent, parentDirectory, f)
      processed.dependencies :+ ProcessedDependency(fileName, processed.content)
    })
  }

  /**
    * - Apply f to each Workflow / Tool in input
    * - Recursively collect all dependencies in input and apply f to them
    * @param input raw input string
    * @param f function to be applied to each workflow / tool
    * @return a ProcessedWorkflow containing the top level processed workflow as well as all processed dependencies
    */
  def collectDependencies(input: String, parentDirectory: File, f: Json => Json = identity[Json]): ProcessedWorkflow = { 
    val json = parse(input)

    // Some files contain a list of tools / workflows under the "$graph" field. In this case recursively add docker default to them
    val processedWorkflow = root.$graph.arr.modifyOption(_.map(f))(json)
      // otherwise just process the file as a single workflow / tool
      .getOrElse(f(json))

    val processedEmbeddedFiles = root.$graph.arr.getOption(json) match {
      case Some(workflowsAndTools) => workflowsAndTools.flatMap(processDependencies(parentDirectory, f)).toList
      case None => processDependencies(parentDirectory, f)(json)
    }

    ProcessedWorkflow(printYaml(processedWorkflow), processedEmbeddedFiles)
  }
}
