package cwlpreprocessor

import better.files.{File => BFile}
import cats.data.NonEmptyList
import cats.syntax.either._
import common.Checked
import common.validation.Validation._
import common.validation.Checked._
import cwl.command.ParentName
import cwl.{CwlDecoder, FileAndId, FullyQualifiedName}
import cwlpreprocessor.CwlPreProcessor._
import io.circe.Json
import io.circe.optics.JsonPath._
import mouse.all._

object CwlPreProcessor {
  private val LocalScheme = "file://"

  object CwlReference {
    def fromString(in: String) = {
      in.startsWith(LocalScheme).option {
        FullyQualifiedName.maybeApply(in)(ParentName.empty) match {
          case Some(FileAndId(file, _, _)) => CwlReference(BFile(file.stripFilePrefix), in)
          case _ => CwlReference(BFile(in.stripFilePrefix), in)
        }
      }
    }

    def apply(file: BFile, pointer: Option[String]) = {
      // prepends file:// to the absolute file path
      val prefixedFile = s"$LocalScheme${file.pathAsString}"
      val fullReference = pointer.map(p => s"$prefixedFile#$p").getOrElse(prefixedFile)
      new CwlReference(file, fullReference)
    }
  }

  /**
    * Saladed CWLs reference other local CWL "node" (workflow or tool) using a URI as follow:
    * file:///path/to/file/containing/node.cwl[#pointer_to_node]
    * #pointer_to_node to node is optional, and will specify which workflow or tool is being targeted in the file.
    * 
    * e.g:
    *   {
    *     "class": "Workflow",
    *     "id": "file:///path/to/workflow/workflow.cwl",
    *     ...
    *     "steps": [
    *       {
    *         "run": "file:///path/to/workflow/multi_tools.cwl#my_tool",
    *         ...
    *       }
    *     ]  
    *   }
    *   
    * This snippet contains 2 references, one that is the ID of this workflow, the other one is the run step pointing to "my_tool" in "/path/to/workflow/multi_tools.cwl"
    * 
    * @param file: the file containing the referenced node. e.g: File(/path/to/file/containing/node.cwl)
    * @param fullReference: the full reference string as it is found in the saladed json. e.g: "file:///path/to/file/containing/node.cwl#pointer_to_node"
    */
  case class CwlReference(file: BFile, fullReference: String)

  type ProcessedReferences = Map[CwlReference, Json]
  type UnProcessedReferences = Map[CwlReference, Json]
  /**
    * A Cwl node that has been processed (saladed and flattened)
    */
  case class ProcessedJsonAndDependencies(processedJson: Json, processedDependencies: ProcessedReferences)

  val saladCwlFile: BFile => Checked[String] = { file => CwlDecoder.saladCwlFile(file).value.unsafeRunSync() }

  implicit class PrintableJson(val json: Json) extends AnyVal {
    def print = io.circe.Printer.noSpaces.pretty(json)
  }

  implicit class EnhancedCwlId(val id: String) extends AnyVal {
    def asReference: Option[CwlReference] = CwlReference.fromString(id)
    def stripFilePrefix = id.stripPrefix(LocalScheme)
  }
}

/**
  * Class to create a standalone version of a CWL file.
  * @param saladFunction function that takes a file and produce a saladed version of the content
  */
class CwlPreProcessor(saladFunction: BFile => Checked[String] = saladCwlFile) {

  /**
    * Pre-process a CWL file and create a standalone, runnable (given proper inputs), inlined version of its content.
    * 
    * The general idea is to work on CwlReferences, starting from the one coming to this function in the form of file and optional root.
    * The goal is to look at the steps in this workflow that point to other references, and recursively flatten them until we can replace the step with
    * its flat version.
    * Along the way we build a Map[CwlReference, Json] that contains all the processed references we've gathered so far.
    * We also carry another Map[CwlReference, Json] that contains references for which we have the json but that haven't been flattened yet.
    * This can happen because a file can contain multiple tools / workflows. When we salad / parse this file, we get (CwlReference, Json) pairs
    * for all the workflow / tools in the file, but they are not flattened yet. Keeping them around avoid having to re-parse this file later.
    */
  def preProcessCwlFile(file: BFile, cwlRoot: Option[String]): Checked[String] = {
    flattenCwlReference(CwlReference(file, cwlRoot), Map.empty) map {
      case ProcessedJsonAndDependencies(result, _) => result.json.print
    }
  }

  /**
    * Flatten the cwl reference given already known processed references.
    */
  private def flattenCwlReference(cwlReference: CwlReference, processedReferences: ProcessedReferences): Checked[ProcessedJsonAndDependencies] = {
    for {
      // parse the file containing the reference
      parsed <- saladAndParse(cwlReference.file)
      // Get a Map[CwlReference, Json] from the parsed file. If the file is a JSON object and only contains one node, the map will only have 1 element 
      unProcessedReferences = mapIdToContent(parsed).toMap
      // The reference json in the file
      referenceJson <- unProcessedReferences.get(cwlReference).toChecked(s"Cannot find a tool or workflow with ID ${cwlReference.fullReference} in file ${cwlReference.file.pathAsString}")
      // Process the reference json
      processed <- flattenJson(referenceJson, unProcessedReferences, processedReferences)
    } yield processed
  }

  /**
    * Given a reference, processes it (meaning flattens it and return it)
    * @param unProcessedRerences references that have been parsed and saladed (we have the json), but not flattened yet.
    * @param checkedProcessedReferences references that are fully processed
    * @param cwlReference reference being processed
    * @return a new ProcessedReferences Map including this cwlReference processed along with all the dependencies
    *         that might have been processed recursively.
    */
  private def processCwlReference(unProcessedRerences: UnProcessedReferences)
                                 (checkedProcessedReferences: Checked[ProcessedReferences],
                                  cwlReference: CwlReference): Checked[ProcessedReferences] = {
    checkedProcessedReferences flatMap { processedReferences =>
      // If the reference has already been processed, no need to do anything
      if (processedReferences.contains(cwlReference)) processedReferences.validNelCheck
      else {
        // Otherwise let's see if we already have the json for it or if we need to process the file
        val result: Checked[ProcessedJsonAndDependencies] = unProcessedRerences.get(cwlReference) match {
          case Some(unProcessedReferenceJson) => 
            // Found the json in the unprocessed map, no need to reparse the file, just flatten this json
            flattenJson(unProcessedReferenceJson, unProcessedRerences, processedReferences)
          case None =>
            // This is the first time we're seeing this reference, we need to parse its file and flatten the reference 
            flattenCwlReference(cwlReference, processedReferences)
        }

        result map {
          // Return everything we got ("processedReferences", "processed" and whatever was done to process our reference ("newReferences"))
          case ProcessedJsonAndDependencies(processed, newReferences) => processedReferences ++ newReferences + (cwlReference -> processed)
        }
      }
    }
  }

  /**
    * Given a Json representing a tool or workflow, flattens it and return the other processed references that were generated in the process.
    * @param saladedJson json to process
    * @param unProcessedReferences references that have been prased and saladed (we have the json), but not flattened yet
    * @param processedReferences references that are fully processed
    */
  private def flattenJson(saladedJson: Json, unProcessedReferences: UnProcessedReferences, processedReferences: ProcessedReferences): Checked[ProcessedJsonAndDependencies] = {
    findRunReferences(saladedJson.json)
      .foldLeft(processedReferences.validNelCheck)(processCwlReference(unProcessedReferences)) map { newKnownReferences =>

      val lookupFunction = {
        json: Json => {
          val fromMap = for {
            asString <- json.asString
            reference <- asString.asReference
            embbeddedJson <- newKnownReferences.get(reference)
          } yield embbeddedJson.json

          fromMap.getOrElse(json)
        }
      }

      ProcessedJsonAndDependencies(root.steps.each.run.json.modify(lookupFunction)(saladedJson.json),  newKnownReferences)
    }
  }

  /**
    * Salad and parse a string to Json
    */
  private def saladAndParse(file: BFile): Checked[Json] = for {
    saladed <- saladFunction(file)
    saladedJson <- io.circe.parser.parse(saladed).leftMap(error => NonEmptyList.one(error.message))
  } yield saladedJson

  /**
    * Given a json, collects all "steps.run" values that are JSON Strings, and convert them to CwlReferences.
    * A saladed JSON is assumed.
    */
  private def findRunReferences(json: Json): List[CwlReference] = {
    json.asArray match {
      case Some(cwls) => cwls.toList.flatMap(findRunReferences)
      case _ => root.steps.each.run.string.getAll(json).flatMap(_.asReference).distinct
    }
  }

  /**
    * Given a json, collect all tools or workflows and map them with their reference id.
    * A saladed JSON is assumed.
    */
  private def mapIdToContent(json: Json): List[(CwlReference, Json)] = {
    json.asArray match {
      case Some(cwls) => cwls.toList.flatMap(mapIdToContent)
      case None => root.id.string.getOption(json).flatMap(_.asReference).map(_ -> json).toList
    }
  }
}
