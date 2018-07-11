package cromwell.backend.google.pipelines.v2alpha1.api

import akka.http.scaladsl.model.ContentTypes
import com.google.api.services.genomics.v2alpha1.model.{Action, Mount, Secret}
import cromwell.backend.google.pipelines.v2alpha1.api.ActionBuilder.Labels._
import cromwell.backend.google.pipelines.v2alpha1.api.ActionFlag.ActionFlag
import mouse.all._

import scala.collection.JavaConverters._

/**
  * Utility singleton to create high level actions.
  */
object ActionBuilder {
  object Labels {
    object Key {
      /**
        * Very short description of the action
        */
      val Tag = "tag"
      val InputName = "inputName"
    }
    object Value {
      val UserAction = "UserAction"
      val Localization = "Localization"
      val Delocalization = "Delocalization"
      val Background = "Background"
    }
  }

  implicit class EnhancedAction(val action: Action) extends AnyVal {
    private def javaFlags(flags: List[ActionFlag]) = flags.map(_.toString).asJava

    def withCommand(command: String*): Action = action.setCommands(command.toList.asJava)
    def withFlags(flags: List[ActionFlag]): Action = action.setFlags(flags |> javaFlags)
    def withMounts(mounts: List[Mount]): Action = action.setMounts(mounts.asJava)
    def withLabels(labels: Map[String, String]): Action = action.setLabels(labels.asJava)
  }

  object Gsutil {
    private val contentTypeText = ContentTypes.`text/plain(UTF-8)`.toString()
    val ContentTypeTextHeader = s"Content-Type: $contentTypeText"
  }

  private val cloudSdkImage = "google/cloud-sdk:alpine"
  def cloudSdkAction: Action = new Action().setImageUri(cloudSdkImage)

  def withImage(image: String) = new Action()
    .setImageUri(image)

  def userAction(docker: String,
                 scriptContainerPath: String,
                 mounts: List[Mount],
                 jobShell: String,
                 dockerEncryptionKeyName: Option[String],
                 encryptedDockerCredentials: Option[String]): Action = {
    val secret = for {
      cred <- encryptedDockerCredentials
      key <- dockerEncryptionKeyName
    } yield new Secret().setKeyName(key).setCipherText(cred)


    new Action()
      .setImageUri(docker)
      .setCommands(List(jobShell, scriptContainerPath).asJava)
      .setMounts(mounts.asJava)
      .setEntrypoint("")
      .setLabels(Map(Key.Tag -> Value.UserAction).asJava)
      .setCredentials(secret.orNull)
  }

  def cloudSdkShellAction(shellCommand: String)(mounts: List[Mount] = List.empty, flags: List[ActionFlag] = List.empty, labels: Map[String, String] = Map.empty): Action =
    cloudSdkAction
      .withCommand("/bin/sh", "-c", shellCommand)
      .withFlags(flags)
      .withMounts(mounts)
      .withLabels(labels)
}
