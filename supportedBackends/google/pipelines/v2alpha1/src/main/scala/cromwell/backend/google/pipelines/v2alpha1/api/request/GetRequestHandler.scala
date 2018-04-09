package cromwell.backend.google.pipelines.v2alpha1.api.request

import java.time.OffsetDateTime

import akka.actor.ActorRef
import com.google.api.client.googleapis.batch.BatchRequest
import com.google.api.client.googleapis.json.GoogleJsonError
import com.google.api.services.genomics.v2alpha1.model.{Event, Operation, WorkerAssignedEvent}
import cromwell.backend.google.pipelines.common.api.PipelinesApiRequestManager._
import cromwell.backend.google.pipelines.common.api.RunStatus
import cromwell.backend.google.pipelines.common.api.RunStatus.{Initializing, Running, Success, UnsuccessfulRunStatus}
import cromwell.backend.google.pipelines.v2alpha1.api.Deserialization._
import cromwell.cloudsupport.gcp.auth.GoogleAuthMode
import cromwell.core.ExecutionEvent
import io.grpc.Status

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try, Success => TrySuccess}

trait GetRequestHandler { this: RequestHandler =>
  def handleRequest(pollingRequest: PAPIStatusPollRequest, batch: BatchRequest, pollingManager: ActorRef)(implicit ec: ExecutionContext): Future[Try[Unit]] = Future(pollingRequest.httpRequest.execute()) map {
    case response if response.isSuccessStatusCode =>
    val operation = response.parseAs(classOf[Operation])
    pollingRequest.requester ! interpretOperationStatus(operation)
    TrySuccess(())
    case response =>
      val failure = Try(GoogleJsonError.parse(GoogleAuthMode.jsonFactory, response)) match {
        case TrySuccess(googleError) => new PAPIApiException(GoogleJsonException(googleError, response.getHeaders))
        case Failure(_) => new PAPIApiException(new RuntimeException(s"Failed to get status for operation ${pollingRequest.jobId.jobId}: HTTP Status Code: ${response.getStatusCode}"))
      }
      pollingManager ! JesApiStatusQueryFailed(pollingRequest, failure)
      Failure(failure)
  } recover {
    case e =>
      pollingManager ! JesApiStatusQueryFailed(pollingRequest, new PAPIApiException(e))
      Failure(e)
  }

  private def interpretOperationStatus(operation: Operation): RunStatus = {
    require(operation != null, "Operation must not be null.")
    try {
      if (operation.getDone) {
        // Deserialized attributes
        val metadata = operation.getMetadata.asScala.toMap
        val events = operation.events
        val pipeline = operation.pipeline

        val eventList = getEventList(metadata, events)

        val workerAssignedEvent = events.map(_.details[WorkerAssignedEvent]).collectFirst({ case Some(defined) => defined })

        val machineType = pipeline.getResources.getVirtualMachine.getMachineType
        val instanceName = workerAssignedEvent.map(_.getInstance())
        val zone = workerAssignedEvent.map(_.getZone)
        val preemptible = pipeline.getResources.getVirtualMachine.getPreemptible

        // If there's an error, generate an unsuccessful status. Otherwise, we were successful!
        Option(operation.getError) match {
          case Some(error) =>
            val errorCode = Status.fromCodeValue(error.getCode)
            UnsuccessfulRunStatus(errorCode, Option(error.getMessage), eventList, Option(machineType), zone, instanceName, preemptible)
          case None => Success(eventList, Option(machineType), zone, instanceName)
        }
      } else if (operation.hasStarted) {
        Running
      } else {
        Initializing
      }
    } catch {
      case npe: NullPointerException =>
        throw new RuntimeException(s"Caught NPE while processing operation ${operation.getName}: $operation", npe)
    }
  }

  private def getEventList(metadata: Map[String, AnyRef], events: List[Event]): Seq[ExecutionEvent] = {
    val starterEvent: Option[ExecutionEvent] = {
      metadata.get("createTime") map { time => ExecutionEvent("waiting for quota", OffsetDateTime.parse(time.toString)) }
    }

    starterEvent.toList ++ events.map(_.toExecutionEvent)
  }
}

