package cromwell.jobstore

import akka.actor.{ActorLogging, ActorRef, Props}
import cats.data.NonEmptyList
import cromwell.core.Dispatcher.EngineDispatcher
import cromwell.core.actor.BatchActor.CommandAndReplyTo
import cromwell.core.actor.ThrottlerActor
import cromwell.core.instrumentation.InstrumentationPrefixes
import cromwell.jobstore.JobStoreActor.{JobComplete, JobNotComplete, JobStoreReadFailure, QueryJobCompletion}
import cromwell.services.instrumentation.{CromwellInstrumentation, InstrumentedBatchActor}

import scala.util.{Failure, Success}

object JobStoreReaderActor {
  def props(database: JobStore, registryActor: ActorRef) = Props(new JobStoreReaderActor(database, registryActor)).withDispatcher(EngineDispatcher)
}

class JobStoreReaderActor(database: JobStore, override val serviceRegistryActor: ActorRef) extends ThrottlerActor[CommandAndReplyTo[QueryJobCompletion]] with InstrumentedBatchActor[CommandAndReplyTo[QueryJobCompletion]] with ActorLogging with CromwellInstrumentation {
  override def processHead(head: CommandAndReplyTo[QueryJobCompletion]) = instrumentedProcess {
    val action = database.readJobResult(head.command.jobKey, head.command.taskOutputs) 
    action onComplete {
      case Success(Some(result)) => head.replyTo ! JobComplete(result)
      case Success(None) => head.replyTo ! JobNotComplete
      case Failure(t) =>
        log.error(t, "JobStoreReadFailure")
        head.replyTo ! JobStoreReadFailure(t)
    }
    action.map(_ => 1)
  }
  
  override def receive = instrumentationReceive.orElse(super.receive)

  override def commandToData(snd: ActorRef) = {
    case query: QueryJobCompletion => CommandAndReplyTo(query, sender())
  }

  override protected def instrumentationPath = NonEmptyList.of("store", "read")
  override protected def instrumentationPrefix = InstrumentationPrefixes.JobPrefix
}
