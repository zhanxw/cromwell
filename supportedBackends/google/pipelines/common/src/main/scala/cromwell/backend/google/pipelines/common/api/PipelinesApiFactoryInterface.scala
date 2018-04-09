package cromwell.backend.google.pipelines.common.api

import com.google.api.client.http.{HttpRequest, HttpRequestInitializer}
import com.google.auth.Credentials
import com.google.auth.http.HttpCredentialsAdapter
import scala.collection.JavaConverters._
import mouse.all._

object PipelinesApiFactoryInterface {
  val GenomicsScopes = List(
    "https://www.googleapis.com/auth/genomics",
    "https://www.googleapis.com/auth/compute",
    "https://www.googleapis.com/auth/devstorage.full_control"
  ).asJava
}

/**
  * The interface provides a single method to build a PipelinesApiRequestFactory
  * There should be one PipelinesApiRequestFactory created per workflow.
  * That is because they need credentials and those can be different for each workflow
  * A PipelinesApiRequestFactory is able to generate all the API requests that are needed to run jobs on the
  * Pipelines API for this workflow.
  */
abstract class PipelinesApiFactoryInterface {
  private def httpRequestInitializerFromCredentials(credentials: Credentials) = {
    val delegate = new HttpCredentialsAdapter(credentials)
    new HttpRequestInitializer() {
      def initialize(httpRequest: HttpRequest) = {
        delegate.initialize(httpRequest)
      }
    }
  }
  
  final def fromCredentials(credentials: Credentials): PipelinesApiRequestFactory = build(credentials |> httpRequestInitializerFromCredentials)
  
  protected def build(httpRequestInitializer: HttpRequestInitializer): PipelinesApiRequestFactory
}
