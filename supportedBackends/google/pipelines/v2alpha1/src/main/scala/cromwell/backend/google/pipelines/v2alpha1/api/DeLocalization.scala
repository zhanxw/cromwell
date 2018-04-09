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
  def actionLogRoot(number: Int) = s"$actionsLogRoot/$number"

  def stdout(number: Int) = s"${actionLogRoot(number)}/stdout"
  def stderr(number: Int) = s"${actionLogRoot(number)}/stderr"

  private def delocalizeLogsAction(gcsLogPath: String) = {
    gsutilAsText("cp", "-r", "/google/logs", gcsLogPath)(flags = List(ActionFlag.AlwaysRun))
  }

  private def copyStdoutStderrActions(standardPaths: StandardPaths, userActionNumber: Int) = List (
    gsutilAsText("cp", stdout(userActionNumber), standardPaths.output.pathAsString)(flags = List(ActionFlag.AlwaysRun)),
    gsutilAsText("cp", stderr(userActionNumber), standardPaths.error.pathAsString)(flags = List(ActionFlag.AlwaysRun))
  )

  def deLocalizeActions(createPipelineParameters: CreatePipelineParameters,
                        mounts: List[Mount],
                        userActionNumber: Int): List[Action] = {
    val gcsLogPath = createPipelineParameters.callRootPath.ensureSlashed + "pipelines-logs"

    createPipelineParameters.outputParameters.map(_.toAction(mounts)) ++
      copyStdoutStderrActions(createPipelineParameters.standardPaths, userActionNumber) :+
      delocalizeLogsAction(gcsLogPath)
  }
}
