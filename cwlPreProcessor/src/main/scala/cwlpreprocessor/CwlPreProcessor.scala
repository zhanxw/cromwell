package cwlpreprocessor

import better.files.{File => BFile}
import cats.data.NonEmptyList
import cats.syntax.either._
import common.Checked
import cwl.command.ParentName
import cwl.{CwlDecoder, FileAndId, FullyQualifiedName}
import cwlpreprocessor.CwlPreProcessor._
import io.circe.Json
import io.circe.optics.JsonPath._
import mouse.all._

object CwlPreProcessor {
  private val LocalScheme = "file://"

  case class CwlReference(file: BFile, fullString: String)

  implicit class PrintableJson(val json: Json) extends AnyVal {
    def print = io.circe.Printer.noSpaces.pretty(json)
  }

  implicit class EnhancedCwlId(val id: String) extends AnyVal {
    def asReference: Option[CwlReference] = {
      id.startsWith(LocalScheme).option {
        FullyQualifiedName.maybeApply(id)(ParentName.empty) match {
          case Some(FileAndId(file, _, _)) => CwlReference(BFile(file.stripFilePrefix), id)
          case _ => CwlReference(BFile(id.stripFilePrefix), id)
        }
      }
    }

    def stripFilePrefix = id.stripPrefix(LocalScheme)
  }

  case class PreProcessedCwl(workflow: String, dependencyZip: BFile)
  case class SaladPair(originalFile: CwlReference, saladedContent: Json)

  val saladCwlFile: BFile => Checked[String] = { file =>
    println("Salading " + file.pathAsString)
    CwlDecoder.saladCwlFile(file).value.unsafeRunSync()
  }
}

class CwlPreProcessor(saladFunction: BFile => Checked[String] = saladCwlFile) {
  def preProcessCwlFile(file: BFile, cwlRoot: Option[String]): Checked[String] = {
    val fullString = cwlRoot.map(r => s"$LocalScheme${file.pathAsString}#$r").getOrElse(file.pathAsString)
    val cwlReference = CwlReference(file, fullString)

    for {
      parsed <- saladAndParse(file)
      alNodes = mapIdToContent(parsed).toMap
      jsonRoot = alNodes(cwlReference)
      (flattened, _) = flattenJson(jsonRoot, alNodes - cwlReference, Map.empty)
    } yield flattened.print
  }

  private def saladAndParse(file: BFile): Checked[Json] = for {
    saladed <- saladFunction(file)
    saladedJson <- parse(saladed)
  } yield saladedJson


  private def flattenJson(saladedJson: Json, unProcessedSiblings: Map[CwlReference, Json], processedReferences: Map[CwlReference, Json]): (Json, Map[CwlReference, Json]) = {

    def processRunReferences(runReferences: List[CwlReference]): Map[CwlReference, Json] = {
      runReferences.foldLeft(processedReferences)({
        case (accumulatedReferences, currentReference) =>
          accumulatedReferences.get(currentReference) match {
            case Some(_) =>
              accumulatedReferences
            case None =>
              unProcessedSiblings.get(currentReference) match {
                case Some(referencedSibling) =>
                  val (processed, newReferences) = flattenJson(referencedSibling, unProcessedSiblings - currentReference, accumulatedReferences)
                  accumulatedReferences ++ newReferences + (currentReference -> processed)
                case None =>
                  val parsed = saladAndParse(currentReference.file).right.get
                  val unProcessedCwlNodes = mapIdToContent(parsed).toMap
                  // we know it's in there because we just parsed the file of the reference
                  val jsonToProcess = unProcessedCwlNodes(currentReference)
                  val unProcessedSiblings = unProcessedCwlNodes - currentReference
                  val (processed, newReferences) = flattenJson(jsonToProcess, unProcessedSiblings, accumulatedReferences)
                  accumulatedReferences ++ newReferences + (currentReference -> processed)
              }
          }
      })
    }

    val runReferences = findRunReferences(saladedJson)
    val newKnownReferences = processRunReferences(runReferences)
    
    val lookupFunction = {
      json: Json => {
        val fromMap = for {
          asString <- json.asString
          reference <- asString.asReference
          embbeddedJson <- newKnownReferences.get(reference)
        } yield embbeddedJson

        fromMap.getOrElse(json)
      }
    }

    root.steps.each.run.json.modify(lookupFunction)(saladedJson) -> newKnownReferences
  }

  /**
    * Parse a string to Json
    */
  private def parse(cwlContent: String): Checked[Json] = {
    io.circe.parser.parse(cwlContent).leftMap(error => NonEmptyList.one(error.message))
  }

  /**
    * Given a json, collects all "steps.run" values that are JSON Strings, and convert them to CwlReferences.
    * Handles a JSON object representing a single CWL "node" (workflow or tool),
    * as well as a (possibly nested) array of CWL nodes.
    * A saladed json is assumed.
    * For instance:
    * 
    * [
    *   {
    *     "id": "file:///path/to/workflow/workflow.cwl#my_first_workflow",
    *     ...
    *     "steps": [
    *       {
    *         "run": "file:///path/to/workflow/other_workflow.cwl",
    *         ...
    *       },
    *       {
    *         "run": "file:///path/to/workflow/not_the_same_workflow.cwl#sometool",
    *         ...
    *       }
    *     ]
    *   },
    *   {
    *     "id": "file:///path/to/workflow/workflow.cwl#my_second_workflow",
    *     ...
    *     "steps": [
    *       {
    *         "run": "file:///path/to/workflow/workflow.cwl#my_first_workflow",
    *         ...
    *       }
    *     ]
    *   }
    * ]
    * 
    * will return 
    * List(
    *   CwlReference(file:///path/to/workflow/other_workflow.cwl,        "file:///path/to/workflow/other_workflow.cwl")
    *   CwlReference(file:///path/to/workflow/not_the_same_workflow.cwl, "file:///path/to/workflow/not_the_same_workflow.cwl#sometool")
    *   CwlReference(file:///path/to/workflow/workflow.cwl,              "file:///path/to/workflow/workflow.cwl#my_first_workflow")
    * )
    * 
    * Note that the second workflow has a step that references the first workflow, in the same file
    */
  private def findRunReferences(json: Json): List[CwlReference] = {
    json.asArray match {
      case Some(cwls) => cwls.toList.flatMap(findRunReferences)
      case _ => root.steps.each.run.string.getAll(json).flatMap(_.asReference).distinct
    }
  }

  /**
    * Given a json, collect all "steps.run" values that are JSON Strings as CwlReferences.
    */
  private def mapIdToContent(json: Json): List[(CwlReference, Json)] = {
    json.asArray match {
      case Some(cwls) => cwls.toList.flatMap(mapIdToContent)
      case None => root.id.string.getOption(json).flatMap(_.asReference).map(_ -> json).toList
    }
  }
}
