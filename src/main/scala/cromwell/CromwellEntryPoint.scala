package cromwell

import akka.pattern.GracefulStopSupport
import cats.syntax.apply._
import cats.syntax.validated._
import com.typesafe.config.ConfigFactory
import common.validation.ErrorOr._
import cromwell.CommandLineParser._
import cromwell.client.CommandLineArguments.ValidSubmission
import cromwell.client.{CommandLineArguments, CromwellEntryPointHelper}
import cromwell.core.path.Path
import cromwell.core.{WorkflowSourceFilesCollection, WorkflowSourceFilesWithDependenciesZip, WorkflowSourceFilesWithoutImports}
import cromwell.engine.workflow.SingleWorkflowRunnerActor
import cromwell.engine.workflow.SingleWorkflowRunnerActor.RunWorkflow
import cromwell.server.{CromwellServer, CromwellSystem}
import cwl.preprocessor.CwlPreProcessor
import net.ceedubs.ficus.Ficus._
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.language.postfixOps
import scala.util.{Failure, Try}

object CromwellEntryPoint extends GracefulStopSupport with CromwellEntryPointHelper {

  lazy val EntryPointLogger = LoggerFactory.getLogger("Cromwell EntryPoint")
  private lazy val config = ConfigFactory.load()

  // Only abort jobs on SIGINT if the config explicitly sets system.abort-jobs-on-terminate = true.
  val abortJobsOnTerminate = config.as[Option[Boolean]]("system.abort-jobs-on-terminate")

  val gracefulShutdown = config.as[Boolean]("system.graceful-server-shutdown")

  lazy val cwlPreProcessor = new CwlPreProcessor()

  /**
    * Run Cromwell in server mode.
    */
  def runServer() = {
    val system = buildCromwellSystem(Server)
    waitAndExit(CromwellServer.run(gracefulShutdown, abortJobsOnTerminate.getOrElse(false)) _, system)
  }

  /**
    * Run a single workflow using the successfully parsed but as yet not validated arguments.
    */
  def runSingle(args: CommandLineArguments): Unit = {
    val cromwellSystem = buildCromwellSystem(Run)
    implicit val actorSystem = cromwellSystem.actorSystem

    val sources = toWorkflowSourcesCollection(args)
    val runnerProps = SingleWorkflowRunnerActor.props(sources, args.metadataOutput, gracefulShutdown, abortJobsOnTerminate.getOrElse(true))(cromwellSystem.materializer)

    val runner = cromwellSystem.actorSystem.actorOf(runnerProps, "SingleWorkflowRunnerActor")

    import cromwell.util.PromiseActor.EnhancedActorRef
    waitAndExit(_ => runner.askNoTimeout(RunWorkflow), cromwellSystem)
  }

  private def buildCromwellSystem(command: String): CromwellSystem = {
    initLogging(command)
    lazy val Log = LoggerFactory.getLogger("cromwell")
    Try {
      new CromwellSystem {}
    } recoverWith {
      case t: Throwable =>
        Log.error("Failed to instantiate Cromwell System. Shutting down Cromwell.")
        Log.error(t.getMessage)
        System.exit(1)
        Failure(t)
    } get
  }

  /**
    * If a cromwell server is going to be run, makes adjustments to the default logback configuration.
    * Overwrites LOG_MODE system property used in our logback.xml, _before_ the logback classes load.
    * Restored from similar functionality in
    *   https://github.com/broadinstitute/cromwell/commit/2e3f45b#diff-facc2160a82442932c41026c9a1e4b2bL28
    * TODO: Logback is configurable programmatically. We don't have to overwrite system properties like this.
    *
    * Also copies variables from config/system/environment/defaults over to the system properties.
    * Fixes issue where users are trying to specify Java properties as environment variables.
    */
  private def initLogging(command: String): Unit = {
    val logbackSetting = command match {
      case Server => "STANDARD"
      case _ => "PRETTY"
    }

    val defaultProps = Map(
      "LOG_MODE" -> logbackSetting,
      "LOG_LEVEL" -> "INFO"
    )

    val configWithFallbacks = config
      .withFallback(ConfigFactory.systemEnvironment())
      .withFallback(ConfigFactory.parseMap(defaultProps.asJava, "Defaults"))

    val props = sys.props
    defaultProps.keys foreach { key =>
      props += key -> configWithFallbacks.getString(key)
    }

    /*
    We've possibly copied values from the environment, or our defaults, into the system properties.
    Make sure that the next time one uses the ConfigFactory that our updated system properties are loaded.
     */
    ConfigFactory.invalidateCaches()
  }

  def toWorkflowSourcesCollection(args: CommandLineArguments): WorkflowSourceFilesCollection = {
    val validation = (args.validateSubmission(EntryPointLogger), writeableMetadataPath(args.metadataOutput)) mapN {
      case (ValidSubmission(w, i, o, l, Some(z)), _) =>
        WorkflowSourceFilesWithDependenciesZip.apply(
          workflowSource = w,
          workflowRoot = args.workflowRoot,
          workflowType = args.workflowType,
          workflowTypeVersion = args.workflowTypeVersion,
          inputsJson = i,
          workflowOptionsJson = o,
          labelsJson = l,
          importsZip = z.loadBytes,
          warnings = Vector.empty)
      case (ValidSubmission(w, i, o, l, None), _) =>
        WorkflowSourceFilesWithoutImports.apply(
          workflowSource = w,
          workflowRoot = args.workflowRoot,
          workflowType = args.workflowType,
          workflowTypeVersion = args.workflowTypeVersion,
          inputsJson = i,
          workflowOptionsJson = o,
          labelsJson = l,
          warnings = Vector.empty)
    }
    
    validOrFailSubmission(validation)
  }

  private def waitAndExit(runner: CromwellSystem => Future[Any], workflowManagerSystem: CromwellSystem): Unit = {
    super.waitAndExit(() => runner(workflowManagerSystem), () => workflowManagerSystem.shutdownActorSystem())
  }
  
  private def writeableMetadataPath(path: Option[Path]): ErrorOr[Unit] = {
    path match {
      case Some(p) if !metadataPathIsWriteable(p) => s"Unable to write to metadata directory: $p".invalidNel
      case _ => ().validNel
    }
  }

  private def metadataPathIsWriteable(metadataPath: Path): Boolean =
    Try(metadataPath.createIfNotExists(createParents = true).append("")).isSuccess

}
