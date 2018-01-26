package centaur.cwl

import centaur.cwl.CentaurCwlRunnerRunMode.ProcessedWorkflow
import com.typesafe.config.Config
import common.validation.Validation._
import cromwell.cloudsupport.gcp.GoogleConfiguration
import cromwell.core.path.{DefaultPathBuilderFactory, PathBuilderFactory}
import cromwell.filesystems.gcs.GcsPathBuilderFactory

sealed trait CentaurCwlRunnerRunMode {
  /**
    * Returns a user friendly of this run mode.
    */
  def description: String

  /**
    * Returns a factory that can create a path builder for this run mode.
    */
  def pathBuilderFactory: PathBuilderFactory

  /**
    * Runs any preprocessing as needed on a workflow.
    *
    * For example, may prefix relative paths so that absolute URLs are used.
    */
  def preProcessWorkflow(workflow: String): ProcessedWorkflow

  /**
    * Runs any preprocessing as needed on inputs.
    *
    * For example, may prefix relative paths so that absolute URLs are used.
    */
  def preProcessInput(input: String): String = input
}

object CentaurCwlRunnerRunMode {
  case class ProcessedDependency(name: String, content: String)
  case class ProcessedWorkflow(content: String, dependencies: List[ProcessedDependency])

  def fromConfig(conf: Config): CentaurCwlRunnerRunMode = {
    conf.getString("mode") match {
      case "local" => LocalRunMode
      case "papi" => PapiRunMode(conf)
      case unknown => throw new UnsupportedOperationException(s"mode not recognized: $unknown")
    }
  }
}

case object LocalRunMode extends CentaurCwlRunnerRunMode {
  override lazy val description: String = "local"
  override lazy val pathBuilderFactory: PathBuilderFactory = DefaultPathBuilderFactory
  private lazy val preprocessor = new CwlPreprocessor()
  override def preProcessWorkflow(workflow: String): ProcessedWorkflow = preprocessor.collectDependencies(workflow)
}

case class PapiRunMode(conf: Config) extends CentaurCwlRunnerRunMode {
  private lazy val googleConf = GoogleConfiguration(conf)
  private lazy val authName = conf.getString("google.auth")
  private lazy val auth = googleConf.auth(authName).toTry.get
  private lazy val preprocessor = new PAPIPreprocessor(conf)

  override lazy val description: String = s"papi $authName"

  override lazy val pathBuilderFactory: PathBuilderFactory = {
    GcsPathBuilderFactory(auth, googleConf.applicationName, None)
  }

  override def preProcessWorkflow(workflow: String): ProcessedWorkflow = preprocessor.preProcessWorkflow(workflow)

  override def preProcessInput(input: String): String = preprocessor.preProcessInput(input)
}
