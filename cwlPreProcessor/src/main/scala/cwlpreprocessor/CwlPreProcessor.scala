package cwlpreprocessor

import better.files.{File => BFile}
import cats.data.{NonEmptyList, Validated}
import cats.instances.list._
import cats.syntax.either._
import cats.syntax.option._
import cats.syntax.traverse._
import cats.syntax.validated._
import common.Checked
import common.validation.ErrorOr.ErrorOr
import cwl.command.ParentName
import cwl.{CwlDecoder, FileAndId, FullyQualifiedName}
import io.circe.Json
import io.circe.optics.JsonPath._
import mouse.all._

object CwlPreProcessor {
  private val LocalScheme = "file://"

  case class PreProcessedCwl(workflow: String, dependencyZip: BFile)
  case class SaladPair(originalFile: BFile, saladedContent: String)

  private def stripFile(in: String) = in.stripPrefix(LocalScheme)

  private def fileFromRunId(in: String): Option[BFile] = {
    in.startsWith(LocalScheme).option {
      FullyQualifiedName.maybeApply(stripFile(in))(ParentName.empty) match {
        case Some(FileAndId(file, _, _)) => BFile(file)
        case _ => BFile(in)
      }
    }
  }

  def saladCwlFile(file: BFile): Checked[String] = {
    CwlDecoder.saladCwlFile(file).value.unsafeRunSync()
  }

  def preProcessCwlFileAndZipDependencies(file: BFile, dependencyPath: BFile = BFile.newTemporaryFile("CwlDependencies")): Checked[PreProcessedCwl] = {
    def prepareDependencyZip(saladedFiles: List[SaladPair]): BFile = {
      val tempDir = BFile.newTemporaryDirectory("CwlPreProcessing")

      saladedFiles.distinct.foreach({
        case SaladPair(original, saladedContent) =>
          val destination = tempDir / original.pathAsString.stripPrefix("/")
          destination.parent.createDirectories()
          destination.write(saladedContent)
      })

      tempDir.zipTo(dependencyPath)
    }

    def findProcessedWorkflow(saladedFiles: List[SaladPair]): Validated[NonEmptyList[String], String] = {
      saladedFiles.collectFirst({
        case SaladPair(original, saladedContent) if original.equals(file) => saladedContent
      })
        // This should not happen but since we return all the dependencies (including the original file) in a single list we have to look for it
        .toValid(NonEmptyList.one(s"Cannot find original file ${file.pathAsString} in list of saladed files."))
    }

    for {
      saladedFiles <- saladFileAndAllDependencies(file).toEither
      saladedWorkflow <- findProcessedWorkflow(saladedFiles).toEither
      dependencyZip = prepareDependencyZip(saladedFiles)
    } yield PreProcessedCwl(saladedWorkflow, dependencyZip)
  }

  private def saladFileAndAllDependencies(file: BFile): ErrorOr[List[SaladPair]] = {
    (for {
      saladed <- saladCwlFile(file)
      saladPair = SaladPair(file, saladed)
      saladedJson <- parse(saladed)
      runFiles <- getRunFiles(saladedJson).toEither
      processedDependencies <- runFiles.flatTraverse(saladFileAndAllDependencies).toEither
    } yield processedDependencies :+ saladPair).toValidated
  }

  private def parse(cwlContent: String): Checked[Json] = {
    io.circe.parser.parse(cwlContent)
      .leftMap(error => NonEmptyList.one(error.message))
  }

  // Organize the tools and workflows in the json by pairing them with their ID
  private def getRunFiles(json: Json): ErrorOr[List[BFile]] = {
    json.asArray match {
      case Some(cwls) => cwls.toList.flatTraverse[ErrorOr, BFile](getRunFiles)
      case _ => root.steps.each.run.string.getAll(json).flatMap(fileFromRunId).distinct.validNel
    }
  }
}
