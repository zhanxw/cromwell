package cromwell.backend.google.pipelines.common.authentication

import cromwell.cloudsupport.gcp.auth.ClientSecrets
import cromwell.core.DockerCredentials
import spray.json.{JsString, JsValue}

/**
 * Interface for Authentication information that can be included as a json object in the file uploaded to GCS
 * upon workflow creation and used in the VM.
 */
sealed trait PipelinesApiAuthObject {
  def context: String
  def map: Map[String, JsValue]

  def toMap: Map[String, Map[String, JsValue]] =  Map(context -> map)
}

/**
 * Authentication information for data (de)localization as the user.
 */
case class GcsLocalizing(clientSecrets: ClientSecrets, token: String) extends PipelinesApiAuthObject {
  override val context = "boto"
  override val map = Map(
    "client_id" -> JsString(clientSecrets.clientId),
    "client_secret" -> JsString(clientSecrets.clientSecret),
    "refresh_token" -> JsString(token)
  )
}

object PipelinesApiDockerCredentials {
  def apply(dockerCredentials: DockerCredentials): PipelinesApiDockerCredentials = apply(dockerCredentials.account, dockerCredentials.token)
  def apply(account: String, token: String): PipelinesApiDockerCredentials = new PipelinesApiDockerCredentials(account, token)
}

/**
 * Authentication information to pull docker images as the user.
 */
case class PipelinesApiDockerCredentials(override val account: String, override val token: String) extends DockerCredentials(account, token) with PipelinesApiAuthObject {
  override val context = "docker"
  override val map = Map(
    "account" -> JsString(account),
    "token" -> JsString(token)
  )
}
