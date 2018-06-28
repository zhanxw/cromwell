package cromwell.backend.google.pipelines.common

import akka.actor.ActorRef
import com.google.auth.Credentials
import cromwell.backend.google.pipelines.common.api.PipelinesApiRequestFactory
import cromwell.backend.standard.{StandardInitializationActor, StandardInitializationActorParams, StandardValidatedRuntimeAttributesBuilder}
import cromwell.backend.{BackendConfigurationDescriptor, BackendInitializationData, BackendWorkflowDescriptor}
import cromwell.core.Dispatcher
import cromwell.cloudsupport.gcp.auth.{GoogleAuthMode, UserServiceAccountMode}
import cromwell.core.io.AsyncIoActorClient
import cromwell.filesystems.gcs.GoogleUtil._
import cromwell.filesystems.gcs.batch.GcsBatchCommandBuilder
import org.apache.commons.codec.binary.Base64
import spray.json.{JsObject, JsString}
import wom.graph.CommandCallNode

import scala.concurrent.Future

case class PipelinesApiInitializationActorParams
(
  workflowDescriptor: BackendWorkflowDescriptor,
  ioActor: ActorRef,
  calls: Set[CommandCallNode],
  jesConfiguration: PipelinesApiConfiguration,
  serviceRegistryActor: ActorRef,
  restarting: Boolean
) extends StandardInitializationActorParams {
  override val configurationDescriptor: BackendConfigurationDescriptor = jesConfiguration.configurationDescriptor
}

class PipelinesApiInitializationActor(pipelinesParams: PipelinesApiInitializationActorParams)
  extends StandardInitializationActor(pipelinesParams) with AsyncIoActorClient {

  override lazy val ioActor = pipelinesParams.ioActor
  protected val pipelinesConfiguration = pipelinesParams.jesConfiguration
  protected val workflowOptions = workflowDescriptor.workflowOptions
  private lazy val ioEc = context.system.dispatchers.lookup(Dispatcher.IoDispatcher)

  override lazy val runtimeAttributesBuilder: StandardValidatedRuntimeAttributesBuilder =
    PipelinesApiRuntimeAttributes.runtimeAttributesBuilder(pipelinesConfiguration)

  // Credentials object for the GCS API
  private lazy val gcsCredentials: Future[Credentials] =
    pipelinesConfiguration.jesAttributes.auths.gcs.retryCredential(workflowOptions)

  // Credentials object for the Genomics API
  private lazy val genomicsCredentials: Future[Credentials] =
    pipelinesConfiguration.jesAttributes.auths.genomics.retryCredential(workflowOptions)

  // Genomics object to access the Genomics API
  private lazy val genomics: Future[PipelinesApiRequestFactory] = {
    genomicsCredentials map pipelinesConfiguration.genomicsFactory.fromCredentials
  }

  val privateDockerEncryptionKeyName: Option[String] = {
    val optionsEncryptionKey = workflowOptions.get(GoogleAuthMode.DockerCredentialsEncryptionKeyNameKey).toOption
    optionsEncryptionKey.orElse(pipelinesConfiguration.dockerEncryptionKeyName)
  }

  val privateDockerEncryptedToken: Option[String] = {
    val effectiveAuth: Option[GoogleAuthMode] = {
      val userServiceAccountAuth = pipelinesConfiguration.googleConfig.authsByName.values collectFirst { case u: UserServiceAccountMode => u }
      // If there's no user service account auth fall back to an auth specified in config.
      def encryptionAuthFromConfig: Option[GoogleAuthMode] = pipelinesConfiguration.dockerEncryptionAuthName.flatMap { name =>
        pipelinesConfiguration.googleConfig.auth(name).toOption
      }
      userServiceAccountAuth orElse encryptionAuthFromConfig
    }

    val unencrypted: Option[String] = pipelinesConfiguration.dockerCredentials.flatMap { dockerCreds =>
      new String(Base64.decodeBase64(dockerCreds.token)).split(':') match {
        case Array(username, password) =>
          // unencrypted tokens are base64-encoded username:password
          Option(JsObject(
            Map(
              "username" -> JsString(username),
              "password" -> JsString(password)
            )).compactPrint)
        case _ => throw new RuntimeException(s"provided dockerhub token '${dockerCreds.token}' is not a base64-encoded username:password")
      }
    }

    for {
      plain <- unencrypted
      auth <- effectiveAuth
      key <- privateDockerEncryptionKeyName
    } yield GoogleAuthMode.encryptKms(key, auth.apiClientGoogleCredential(identity), plain)
  }

  override lazy val workflowPaths: Future[PipelinesApiWorkflowPaths] = for {
    gcsCred <- gcsCredentials
    genomicsCred <- genomicsCredentials
    validatedPathBuilders <- pathBuilders
  } yield new PipelinesApiWorkflowPaths(workflowDescriptor, gcsCred, genomicsCred, pipelinesConfiguration, validatedPathBuilders)(ioEc)

  override lazy val initializationData: Future[PipelinesApiBackendInitializationData] = for {
    jesWorkflowPaths <- workflowPaths
    gcsCreds <- gcsCredentials
    genomicsFactory <- genomics
  } yield PipelinesApiBackendInitializationData(
    workflowPaths = jesWorkflowPaths,
    runtimeAttributesBuilder = runtimeAttributesBuilder,
    jesConfiguration = pipelinesConfiguration,
    gcsCredentials = gcsCreds,
    genomicsRequestFactory = genomicsFactory,
    privateDockerEncryptionKeyName = privateDockerEncryptionKeyName,
    privateDockerEncryptedToken = privateDockerEncryptedToken)

  override def beforeAll(): Future[Option[BackendInitializationData]] = {
    for {
      paths <- workflowPaths
      _ = publishWorkflowRoot(paths.workflowRoot.pathAsString)
      data <- initializationData
    } yield Option(data)
  }

  override lazy val ioCommandBuilder = GcsBatchCommandBuilder
}
