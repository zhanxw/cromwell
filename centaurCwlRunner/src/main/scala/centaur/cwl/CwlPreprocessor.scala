package centaur.cwl

import better.files.File
import centaur.cwl.CentaurCwlRunnerRunMode.{ProcessedDependency, ProcessedWorkflow}
import io.circe.optics.JsonPath.root
import io.circe.{Json, yaml}

class CwlPreprocessor(rootDirectory: File) {

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
  
  private def processDependencies(f: Json => Json)(json: Json): List[ProcessedDependency] = {
    // Fet all the objects in the steps field, and collect the "run" values which represent another cwl file
    val embeddedFileNames = root.steps.each.obj.getAll(json).flatMap(
      _.kleisli("run")
        // If there's a "#" it means it's pointing to another Workflow / Tool in the same file, we don't want that
        .flatMap(_.asString.filterNot(_.startsWith("#")))
    )

    embeddedFileNames.flatMap({ fileName =>
      // For each file, process it, and add the result to the list of nested workflows
      val processed = collectDependencies(rootDirectory./(fileName).contentAsString, f)
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
  def collectDependencies(input: String, f: Json => Json = identity[Json]): ProcessedWorkflow = { 
    val json = parse(input)

    // Some files contain a list of tools / workflows under the "$graph" field. In this case recursively add docker default to them
    val processedWorkflow = root.$graph.arr.modifyOption(_.map(f))(json)
      // otherwise just process the file as a single workflow / tool
      .getOrElse(f(json))

    val processedEmbeddedFiles = root.$graph.arr.getOption(json) match {
      case Some(workflowsAndTools) => workflowsAndTools.flatMap(processDependencies(f)).toList
      case None => processDependencies(f)(json)
    }

    ProcessedWorkflow(printYaml(processedWorkflow), processedEmbeddedFiles)
  }
}
