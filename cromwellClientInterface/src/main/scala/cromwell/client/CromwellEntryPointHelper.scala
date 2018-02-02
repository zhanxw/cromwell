package cromwell.client

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import common.exception.MessageAggregation
import common.validation.ErrorOr.ErrorOr
import cromwell.api.CromwellClient
import cromwell.api.model.{Label, LabelsJsonFormatter, WorkflowSingleSubmission}
import cromwell.client.CommandLineArguments.ValidSubmission
import cromwell.client.CromwellEntryPointHelper._
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{Await, Future, TimeoutException}
import scala.util.{Failure, Success}

object CromwellEntryPointHelper {
  val SubmitLogger = LoggerFactory.getLogger("cromwell-submit")
}

trait CromwellEntryPointHelper {
  def submitToServer(args: CommandLineArguments): Unit = {
    implicit val actorSystem = ActorSystem("SubmitSystem")
    implicit val materializer = ActorMaterializer()
    implicit val ec = actorSystem.dispatcher

    val cromwellClient = new CromwellClient(args.host, "v2")

    val singleSubmission = toSingleWorkflowSubmission(args)
    val submissionFuture = () => cromwellClient.submit(singleSubmission).andThen({
      case Success(submitted) =>
        SubmitLogger.info(s"Workflow ${submitted.id} submitted to ${args.host}")
      case Failure(t) =>
        SubmitLogger.error(s"Failed to submit workflow to cromwell server at ${args.host}")
        SubmitLogger.error(t.getMessage)
    })

    waitAndExit(submissionFuture, () => actorSystem.terminate())
  }

  def toSingleWorkflowSubmission(args: CommandLineArguments): WorkflowSingleSubmission = {
    import LabelsJsonFormatter._
    import spray.json._

    val validation = args.validateSubmission(SubmitLogger) map {
      case ValidSubmission(w, i, o, l, z) =>
        WorkflowSingleSubmission(
          workflowSource = w,
          workflowRoot = args.workflowRoot,
          workflowType = args.workflowType,
          workflowTypeVersion = args.workflowTypeVersion,
          inputsJson = Option(i),
          options = Option(o),
          labels = Option(l.parseJson.convertTo[List[Label]]),
          zippedImports = z)
    }

    validOrFailSubmission(validation)
  }

  def validOrFailSubmission[A](validation: ErrorOr[A]): A = {
    validation.valueOr(errors => throw new RuntimeException with MessageAggregation {
      override def exceptionContext: String = "ERROR: Unable to submit workflow to Cromwell:"
      override def errorMessages: Traversable[String] = errors.toList
    })
  }

  protected def waitAndExit[A](operation: () => Future[A], shutdown: () => Future[Any]) = {
    val futureResult = operation()
    Await.ready(futureResult, Duration.Inf)

    try {
      Await.ready(shutdown(), 30.seconds)
    } catch {
      case _: TimeoutException => Console.err.println("Timed out trying to shutdown actor system")
      case other: Exception => Console.err.println(s"Unexpected error trying to shutdown actor system: ${other.getMessage}")
    }

    val returnCode = futureResult.value.get match {
      case Success(_) => 0
      case Failure(e) =>
        Console.err.println(e.getMessage)
        1
    }

    sys.exit(returnCode)
  }
}
