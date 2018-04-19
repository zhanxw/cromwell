package cromwell.backend.google.pipelines.v2alpha1.api

import com.google.api.services.genomics.v2alpha1.model.{Action, Mount}
import common.util.StringUtil._
import cromwell.backend.google.pipelines.common.api.PipelinesApiRequestFactory.CreatePipelineParameters
import cromwell.backend.google.pipelines.v2alpha1.PipelinesConversions._
import cromwell.backend.google.pipelines.v2alpha1.api.ActionBuilder._
import cromwell.backend.google.pipelines.v2alpha1.api.DeLocalization._
import cromwell.core.StandardPaths

object DeLocalization {
  private val logsRoot = "/google/logs"
  private val actionsLogRoot = logsRoot + "/action"
}

trait DeLocalization {
  private def actionLogRoot(number: Int) = s"$actionsLogRoot/$number"

  private def stdout(number: Int) = s"${actionLogRoot(number)}/stdout"
  private def stderr(number: Int) = s"${actionLogRoot(number)}/stderr"
  private def aggregatedLog = s"$logsRoot/output"

  private def delocalizeLogsAction(gcsLogPath: String) = {
    gsutilAsText("cp", "-r", "/google/logs", gcsLogPath)(flags = List(ActionFlag.AlwaysRun))
  }

  // The logs are now located in the pipelines-logs directory
  // To keep the behavior similar to V1, we copy stdout/stderr from the user action to the call directory,
  // along with the aggregated log file
  private def copyLogsToLegacyPaths(standardPaths: StandardPaths, userActionNumber: Int, gcsLegacyLogPath: String) = List (
    gsutilAsText("cp", stdout(userActionNumber), standardPaths.output.pathAsString)(flags = List(ActionFlag.AlwaysRun)),
    gsutilAsText("cp", stderr(userActionNumber), standardPaths.error.pathAsString)(flags = List(ActionFlag.AlwaysRun)),
    gsutilAsText("cp", aggregatedLog, gcsLegacyLogPath)(flags = List(ActionFlag.AlwaysRun))
  )

  def deLocalizeActions(createPipelineParameters: CreatePipelineParameters,
                        mounts: List[Mount],
                        userActionNumber: Int): List[Action] = {
    val gcsLogDirectoryPath = createPipelineParameters.callRootPath.ensureSlashed + "pipelines-logs"
    val gcsLegacyLogPath = createPipelineParameters.logGcsPath

    createPipelineParameters.outputParameters.map(_.toAction(mounts)) ++
      copyLogsToLegacyPaths(createPipelineParameters.standardPaths, userActionNumber, gcsLegacyLogPath) :+
      delocalizeLogsAction(gcsLogDirectoryPath)
  }
}
