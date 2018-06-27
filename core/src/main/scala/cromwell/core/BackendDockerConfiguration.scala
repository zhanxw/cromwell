package cromwell.core

import com.typesafe.config.Config
import cromwell.core.ConfigUtil._

object DockerCredentials {
  def unapply(arg: DockerCredentials): Option[(String, String)] = Option(arg.account -> arg.token)
}

/**
  * Encapsulate docker credential information.
  */
class DockerCredentials(val account: String, val token: String, val keyName: Option[String], val authName: Option[String])

case class BackendDockerConfiguration(dockerCredentials: Option[DockerCredentials])

/**
  * Singleton encapsulating a DockerConf instance.
  */
object BackendDockerConfiguration {

  private val dockerKeys = Set("account", "token", "auth", "key-name")

  def build(config: Config) = {
    import net.ceedubs.ficus.Ficus._
    val dockerConf: Option[DockerCredentials] = for {
      dockerConf <- config.as[Option[Config]]("dockerhub")
      _ = dockerConf.warnNotRecognized(dockerKeys, "dockerhub")
      account <- dockerConf.validateString("account").toOption
      token <- dockerConf.validateString("token").toOption
      authName = dockerConf.as[Option[String]]("auth")
      keyName = dockerConf.as[Option[String]]("key-name")
    } yield new DockerCredentials(account = account, token = token, authName = authName, keyName = keyName)

    new BackendDockerConfiguration(dockerConf)
  }
}
