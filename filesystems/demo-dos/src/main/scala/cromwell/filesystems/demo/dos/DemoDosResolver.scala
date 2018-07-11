package cromwell.filesystems.demo.dos

import com.fasterxml.jackson.core.JsonPointer
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.google.auth.Credentials
import com.google.auth.oauth2.{AccessToken, OAuth2Credentials}
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import net.ceedubs.ficus.Ficus._
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpHeaders, HttpResponse, HttpStatus}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Try

case class DemoDosResolver(gcsCredentials: Credentials, config: Config) extends StrictLogging {

  private val GcsScheme = "gs"
  private val DosPathToken = s"$${dosPath}"
  private lazy val objectMapper = new ObjectMapper()
  private lazy val marthaUri = config.getString("demo.dos.martha.url")
  private lazy val marthaRequestJsonTemplate = config.getString("demo.dos.martha.request.json-template")
  private lazy val marthaResponseJsonPointerString = config.getString("demo.dos.martha.response.json-pointer")
  private lazy val marthaResponseJsonPointer = JsonPointer.compile(marthaResponseJsonPointerString)
  private lazy val marthaDebug = config.getOrElse("demo.dos.martha.debug", false)

  private def debugResponse(response: HttpResponse): String = if (marthaDebug) s"\n$response" else ""

  protected def createClient(): CloseableHttpClient = HttpClientBuilder.create().build()

  def getContainerRelativePath(demoDosPath: DemoDosPath): String = {
    val client = createClient()
    try {
      val post = new HttpPost(marthaUri)
      val accessToken = freshAccessToken(gcsCredentials.asInstanceOf[OAuth2Credentials])
      post.setHeader(HttpHeaders.AUTHORIZATION, s"Bearer $accessToken")
      val requestJson = marthaRequestJsonTemplate.replace(DosPathToken, demoDosPath.pathAsString)
      post.setEntity(new StringEntity(requestJson, ContentType.APPLICATION_JSON))

      if (marthaDebug) {
        logger.info(s"Martha Request:\n$post\n${post.getEntity}\n${EntityUtils.toString(post.getEntity)}")
      }

      val response = client.execute(post)
      val responseEntityOption = Option(response.getEntity).map(EntityUtils.toString)

      if (marthaDebug) {
        logger.info(s"Martha Response:\n$response${responseEntityOption.mkString("\n", "", "")}")
      }

      def throwUnexpectedResponse: Nothing = {
        throw new RuntimeException(
          s"Unexpected response looking up ${demoDosPath.pathAsString} from $marthaUri.${debugResponse(response)}")
      }

      try {
        val content = if (response.getStatusLine.getStatusCode == HttpStatus.SC_OK) {
          responseEntityOption.getOrElse(throwUnexpectedResponse)
        } else {
          throwUnexpectedResponse
        }

        val resolvedUrlOption = getResolvedUrl(objectMapper.readTree(content).at(marthaResponseJsonPointer))
        val pathWithoutSchemeOption = resolvedUrlOption.map(_.substring(GcsScheme.length + 3))

        pathWithoutSchemeOption getOrElse throwUnexpectedResponse
      } finally {
        Try(response.close())
        ()
      }
    } finally {
      Try(client.close())
      ()
    }
  }

  private def getResolvedUrl(node: JsonNode): Option[String] = {
    node match {
      case _ if node.isMissingNode => None
      case _ if node.isArray =>
        val array = node.asInstanceOf[ArrayNode]
        array.elements.asScala.map(getResolvedUrl) collectFirst {
          case Some(element) => element
        }
      case _ if node.isTextual =>
        Option(node.asText) flatMap { text =>
          if (text.startsWith(GcsScheme + "://")) Option(text) else None
        }
      case _ => None
    }
  }

  /*
  Begin copy/update from cromwell.docker.registryv2.flows.gcr.GcrAbstractFlow

  Adds local synchronization for refreshing the access token. Synchronization should be refactored elsewhere and shared
  by all code using this cred.
   */
  private val AccessTokenAcceptableTTL = 1.minute
  private val freshAccessTokenMutex = new Object()

  private def freshAccessToken(credential: OAuth2Credentials): String = {
    def accessTokenTTLIsAcceptable(accessToken: AccessToken): Boolean = {
      val timeLeft = (accessToken.getExpirationTime.getTime - System.currentTimeMillis()).millis
      timeLeft >= AccessTokenAcceptableTTL
    }

    freshAccessTokenMutex synchronized {
      Option(credential.getAccessToken) match {
        case Some(accessToken) if accessTokenTTLIsAcceptable(accessToken) => accessToken.getTokenValue
        case _ =>
          credential.refresh()
          credential.getAccessToken.getTokenValue
      }
    }
  }

  /*
  End copy/update from cromwell.docker.registryv2.flows.gcr.GcrAbstractFlow
   */
}
