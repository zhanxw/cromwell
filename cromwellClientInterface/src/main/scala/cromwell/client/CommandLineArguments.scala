package cromwell.client

import java.net.URL

import better.files.File
import cats.syntax.apply._
import cats.syntax.validated._
import common.validation.ErrorOr.ErrorOr
import cromwell.client.CommandLineArguments.ValidSubmission
import cromwell.core.WorkflowOptions
import cromwell.core.path.Path
import cwl.preprocessor.CwlPreProcessor
import org.slf4j.Logger

object CommandLineArguments {
  val DefaultCromwellHost = new URL("http://localhost:8000")
  case class ValidSubmission(
                              workflowSource: String,
                              worflowInputs: String,
                              workflowOptions: String,
                              workflowLabels: String,
                              dependencies: Option[File])
}

case class CommandLineArguments(command: Option[String] = None,
                                workflowSource: Option[Path] = None,
                                workflowRoot: Option[String] = None,
                                workflowInputs: Option[Path] = None,
                                workflowOptions: Option[Path] = None,
                                workflowType: Option[String] = WorkflowOptions.defaultWorkflowType,
                                workflowTypeVersion: Option[String] = WorkflowOptions.defaultWorkflowTypeVersion,
                                workflowLabels: Option[Path] = None,
                                imports: Option[Path] = None,
                                metadataOutput: Option[Path] = None,
                                host: URL = CommandLineArguments.DefaultCromwellHost
                               ) {
  private lazy val cwlPreProcessor = new CwlPreProcessor()
  private lazy val isCwl = workflowType.exists(_.equalsIgnoreCase("cwl"))

  def validateSubmission(logger: Logger): ErrorOr[ValidSubmission] = {
    val workflowPath = File(workflowSource.get.pathAsString)

    val workflowAndDependencies: ErrorOr[(String, Option[File])] = if (isCwl) {
      logger.info("Pre Processing Workflow...")
      lazy val preProcessedCwl = cwlPreProcessor.preProcessCwlFileToString(workflowPath, None)

      imports match {
        case Some(explicitImports) => readContent("Workflow source", workflowSource.get).map(_ -> Option(File(explicitImports.pathAsString)))
        case None => preProcessedCwl.map(_ -> None)
      }
    } else readContent("Workflow source", workflowSource.get).map(_ -> imports.map(p => File(p.pathAsString)))

    val inputsJson = readJson("Workflow inputs", workflowInputs)
    val optionsJson = readJson("Workflow options", workflowOptions)
    val labelsJson = readJson("Workflow labels", workflowLabels)

    (workflowAndDependencies, inputsJson, optionsJson, labelsJson) mapN {
      case ((w, z), i, o, l) =>
        ValidSubmission(w, i, o, l, z)
    }
  }

  /** Read the path to a string. */
  private def readContent(inputDescription: String, path: Path): ErrorOr[String] = {
    if (!path.exists) {
      s"$inputDescription does not exist: $path".invalidNel
    } else if (!path.isReadable) {
      s"$inputDescription is not readable: $path".invalidNel
    } else path.contentAsString.validNel
  }

  /** Read the path to a string, unless the path is None, in which case returns "{}". */
  private def readJson(inputDescription: String, pathOption: Option[Path]): ErrorOr[String] = {
    pathOption match {
      case Some(path) => readContent(inputDescription, path)
      case None => "{}".validNel
    }
  }
}
