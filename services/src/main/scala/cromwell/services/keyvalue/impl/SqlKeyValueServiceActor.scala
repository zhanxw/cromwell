package cromwell.services.keyvalue.impl

import akka.actor.{ActorRef, Props}
import com.typesafe.config.Config
import cromwell.core.Dispatcher.ServiceDispatcher
import cromwell.services.keyvalue.KeyValueServiceActor
import cromwell.services.keyvalue.KeyValueServiceActor._

import scala.concurrent.Future

object SqlKeyValueServiceActorRoutee {
  def props(serviceConfig: Config, globalConfig: Config, serviceRegistryActor: ActorRef) = Props(SqlKeyValueServiceActor(serviceConfig, globalConfig, serviceRegistryActor)).withDispatcher(ServiceDispatcher)
}

//final case class SqlKeyValueServiceActor(serviceConfig: Config, globalConfig: Config, serviceRegistryActor: ActorRef) extends Actor {
//  val routees = Vector.fill(5) {
//    ActorRefRoutee(context.actorOf(SqlKeyValueServiceActorRoutee.props(serviceConfig, globalConfig, serviceRegistryActor)))
//  }
//  val router = Router(RoundRobinRoutingLogic(), routees)
//
//  override def receive = {
//    case ShutdownCommand => routees.foreach(_.send(ShutdownCommand, sender()))
//    case msg => router.route(msg, sender())
//  }
//}

final case class SqlKeyValueServiceActor(serviceConfig: Config, globalConfig: Config, serviceRegistryActor: ActorRef)
  extends KeyValueServiceActor with BackendKeyValueDatabaseAccess {
  override implicit val ec = context.dispatcher
  private implicit val system = context.system

  override def doPut(put: KvPut): Future[KvResponse] = {
    put.pair.value match {
      case Some(_) => updateBackendKeyValuePair(put.pair.key.workflowId,
        put.pair.key.jobKey,
        put.pair.key.key,
        put.pair.value.get).map(_ => KvPutSuccess(put))
      case None => Future.successful(KvFailure(put, new RuntimeException(s"Failed to find the value associated to key: ${put.pair.key.key}. This key cannot be added to the BackendKVStore.")))
    }
  }

  override def doGet(get: KvGet): Future[KvResponse] = {
    val backendValue = getBackendValueByKey(
      get.key.workflowId,
      get.key.jobKey,
      get.key.key
    )

    backendValue map {
      case Some(maybeValue) => KvPair(get.key, Option(maybeValue))
      case None => KvKeyLookupFailed(get)
    }
  }
}
