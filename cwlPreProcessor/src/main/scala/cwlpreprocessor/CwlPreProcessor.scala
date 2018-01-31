package cwlpreprocessor

import better.files.{File => BFile}
import cats.data.NonEmptyList
import cats.instances.list._
import cats.syntax.either._
import cats.syntax.traverse._
import common.Checked
import common.validation.ErrorOr.ErrorOr
import cwl.command.ParentName
import cwl.{CwlDecoder, FileAndId, FullyQualifiedName}
import io.circe.Json
import io.circe.optics.JsonPath._
import mouse.all._

object CwlPreProcessor {
  case class CwlReference(file: BFile, fullString: String)
  
  private implicit class PrintableJson(val json: Json) extends AnyVal {
    def print = io.circe.Printer.noSpaces.pretty(json)
  }
  
  private val LocalScheme = "file://"

  case class PreProcessedCwl(workflow: String, dependencyZip: BFile)
  case class SaladPair(originalFile: CwlReference, saladedContent: Json)

  private def stripFile(in: String) = in.stripPrefix(LocalScheme)

  private def fileAndIdFromRunId(in: String): Option[CwlReference] = {
    in.startsWith(LocalScheme).option {
      FullyQualifiedName.maybeApply(in)(ParentName.empty) match {
        case Some(FileAndId(file, _, _)) => CwlReference(BFile(stripFile(file)), in)
        case _ => CwlReference(BFile(stripFile(in)), in)
      }
    }
  }

  def saladCwlFile(file: BFile): Checked[String] = {
    println("Salading " + file.pathAsString)
    CwlDecoder.saladCwlFile(file).value.unsafeRunSync()
  }

  def preProcessCwlFile(file: BFile, root: Option[String]): Checked[String] = {
    val fullString = root.map(r => s"${file.pathAsString}#$r").getOrElse(file.pathAsString)
    val cwlReference = CwlReference(file, fullString)
    
    def findJsonRoot(referenceMap: Map[CwlReference, Json]): Checked[Json] = {
      referenceMap.collectFirst({ 
        case (reference, json) if stripFile(reference.fullString) == cwlReference.fullString => json
      }).map(Right.apply).getOrElse(Left(NonEmptyList.one("Can't find json root")))
    }

    for {
      referenceMap <- createReferenceMap(file).toEither
      jsonRoot <- findJsonRoot(referenceMap)
      flattened = flattenCwl(jsonRoot, referenceMap).print
    } yield flattened
  }

  private def createReferenceMap(file: BFile): ErrorOr[Map[CwlReference, Json]] = {
    def saladFileAndAllDependenciesRec(currentReferenceMap: List[(CwlReference, Json)])(file: BFile): ErrorOr[List[(CwlReference, Json)]] = {
      (for {
        saladed <- saladCwlFile(file)
        saladedJson <- parse(saladed)
        newReferenceMap = mapIdToContent(saladedJson) ++ currentReferenceMap
        runFiles = getRunFiles(saladedJson).map(_.file).filterNot(newReferenceMap.map(_._1.file).contains)
        processedDependencies <- runFiles.flatTraverse(saladFileAndAllDependenciesRec(newReferenceMap)).toEither
      } yield processedDependencies ++ newReferenceMap).toValidated
    }

    saladFileAndAllDependenciesRec(List.empty)(file).map(_.toMap)
  }
  
  private def flattenCwl(rootJson: Json, referenceMap: Map[CwlReference, Json]): Json = {
    def lookupSaladedJsonForRunId(json: Json): Json = {
      val fromMap = for {
        asString <- json.asString
        runFile <- fileAndIdFromRunId(asString)
        embbeddedJson <- referenceMap.get(runFile)
      } yield flattenCwl(embbeddedJson, referenceMap)

      fromMap.getOrElse(json)
    }

    replaceRuns(lookupSaladedJsonForRunId, rootJson)(rootJson)
  }

  private def parse(cwlContent: String): Checked[Json] = {
    io.circe.parser.parse(cwlContent)
      .leftMap(error => NonEmptyList.one(error.message))
  }

  // Organize the tools and workflows in the json by pairing them with their ID
  private def getRunFiles(json: Json): List[CwlReference] = {
    json.asArray match {
      case Some(cwls) => cwls.toList.flatMap(getRunFiles)
      case _ => root.steps.each.run.string.getAll(json).flatMap(fileAndIdFromRunId).distinct
    }
  }

  // Organize the tools and workflows in the json by pairing them with their ID
  private def mapIdToContent(json: Json): List[(CwlReference, Json)] = {
    json.asArray match {
      case Some(cwls) => cwls.toList.flatMap(mapIdToContent)
      case None => root.id.string.getOption(json).flatMap(fileAndIdFromRunId).map(_ -> json).toList
    }
  }

  // Organize the tools and workflows in the json by pairing them with their ID
  private def replaceRuns(f: Json => Json, json: Json): Json => Json = {
    json.isArray
      .option(root.each.steps.each.run.json.modify(f)).
      getOrElse(root.steps.each.run.json.modify(f))
  }
    
}
