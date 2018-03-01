package cromwell.services.metadata.impl

import akka.actor.{ActorLogging, ActorRef, Props}
import cats.data.NonEmptyVector
import cromwell.core.Dispatcher.ServiceDispatcher
import cromwell.core.Mailbox.PriorityMailbox
import cromwell.core.actor.BatchActor
import cromwell.core.instrumentation.InstrumentationPrefixes
import cromwell.services.MetadataServicesStore
import cromwell.services.instrumentation.{CromwellInstrumentation, InstrumentedBatchActor}
import cromwell.services.metadata.MetadataEvent
import cromwell.services.metadata.MetadataService._

import scala.concurrent.duration._
import scala.util.{Failure, Success}


class WriteMetadataActor(override val batchSize: Int,
                         override val flushRate: FiniteDuration,
                         override val serviceRegistryActor: ActorRef)
  extends BatchActor[MetadataWriteAction](flushRate, batchSize) with InstrumentedBatchActor[MetadataWriteAction] with ActorLogging with
    MetadataDatabaseAccess with MetadataServicesStore with CromwellInstrumentation {
  
  def commandToData(snd: ActorRef): PartialFunction[Any, MetadataWriteAction] = {
    case command: MetadataWriteAction => command
  }
  
  override def receive = instrumentationReceive.orElse(super.receive)
  
  override def process(e: NonEmptyVector[MetadataWriteAction]) = instrumentedProcess {
    val empty = (Vector.empty[MetadataEvent], Map.empty[Iterable[MetadataEvent], ActorRef])

    val (putWithoutResponse, putWithResponse) = e.foldLeft(empty)({
      case ((putEvents, putAndRespondEvents), action: PutMetadataAction) =>
        (putEvents ++ action.events, putAndRespondEvents)
      case ((putEvents, putAndRespondEvents), action: PutMetadataActionAndRespond) =>
        (putEvents, putAndRespondEvents + (action.events -> action.replyTo))
    })
    val allPutEvents: Iterable[MetadataEvent] = putWithoutResponse ++ putWithResponse.keys.flatten
    val dbAction = addMetadataEvents(allPutEvents)

    dbAction onComplete {
      case Success(_) =>
        putWithResponse foreach { case (ev, replyTo) => replyTo ! MetadataWriteSuccess(ev) }
      case Failure(regerts) =>
        putWithResponse foreach { case (ev, replyTo) => replyTo ! MetadataWriteFailure(regerts, ev) }
    }

    dbAction.map(_ => allPutEvents.size)
  }

  override protected def weightFunction(command: MetadataWriteAction) = command.size
  override protected def instrumentationPath = MetadataServiceActor.MetadataInstrumentationPrefix
  override protected def instrumentationPrefix = InstrumentationPrefixes.ServicesPrefix
}

object WriteMetadataActor {
  val MetadataWritePath = MetadataServiceActor.MetadataInstrumentationPrefix.::("writes")

  def props(dbBatchSize: Int,
            flushRate: FiniteDuration,
            serviceRegistryActor: ActorRef): Props =
    Props(new WriteMetadataActor(dbBatchSize, flushRate, serviceRegistryActor))
      .withDispatcher(ServiceDispatcher)
      .withMailbox(PriorityMailbox)
}
