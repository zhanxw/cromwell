package cromwell.backend.google.pipelines.common

import cromwell.backend.BackendConfigurationDescriptor
import cromwell.backend.google.pipelines.common.api.PipelinesApiFactoryInterface
import cromwell.backend.google.pipelines.common.authentication.PipelinesApiDockerCredentials
import cromwell.cloudsupport.gcp.GoogleConfiguration
import cromwell.cloudsupport.gcp.auth.GoogleAuthMode
import cromwell.core.BackendDockerConfiguration
import net.ceedubs.ficus.Ficus._
import org.apache.commons.codec.binary.Base64
import spray.json._

object PipelinesApiConfiguration {
  def apply(configurationDescriptor: BackendConfigurationDescriptor,
            genomicsFactory: PipelinesApiFactoryInterface,
            googleConfig: GoogleConfiguration,
            jesAttributes: PipelinesApiAttributes) = {
    new PipelinesApiConfiguration(configurationDescriptor, genomicsFactory, googleConfig, jesAttributes)
  }
}

class PipelinesApiConfiguration(val configurationDescriptor: BackendConfigurationDescriptor,
                                val genomicsFactory: PipelinesApiFactoryInterface,
                                val googleConfig: GoogleConfiguration,
                                val jesAttributes: PipelinesApiAttributes) extends DefaultJsonProtocol {

  val jesAuths = jesAttributes.auths
  val root = configurationDescriptor.backendConfig.getString("root")
  val runtimeConfig = configurationDescriptor.backendRuntimeConfig
  val jesComputeServiceAccount = jesAttributes.computeServiceAccount

  val dockerCredentials = {
    BackendDockerConfiguration.build(configurationDescriptor.backendConfig).dockerCredentials map { creds =>
      PipelinesApiDockerCredentials.apply(creds, googleConfig)
    }
  }

  val dockerEncryptionKeyName: Option[String] = dockerCredentials flatMap { _.keyName }

  // FIXME
  // If there is a dockerhub section in a PAPI v2 config the key and auth names must be set in config. This is
  // not the right place for this code ultimately since the encrypt/decrypt credentials will change with each
  // workflow as workflow options. For now assume these are defined, fix later.
  val encryptedDockerCredentials = dockerCredentials map {
    case PipelinesApiDockerCredentials(_, token, Some(keyName), _, Some(credential)) =>
      val Array(username, password) = new String(Base64.decodeBase64(token)).split(':')
      val map = JsObject(
        Map(
          "username" -> JsString(username),
          "password" -> JsString(password)
        )
      ).compactPrint

      GoogleAuthMode.encryptKms(keyName, credential, map)
    case x => throw new RuntimeException("This is just wrong: " + x.toString)
  }
  val needAuthFileUpload = jesAuths.gcs.requiresAuthFile || dockerCredentials.isDefined || jesAttributes.restrictMetadataAccess
  val qps = jesAttributes.qps
  val papiRequestWorkers = jesAttributes.requestWorkers
  val jobShell = configurationDescriptor.backendConfig.as[Option[String]]("job-shell").getOrElse(
    configurationDescriptor.globalConfig.getOrElse("system.job-shell", "/bin/bash"))
}
