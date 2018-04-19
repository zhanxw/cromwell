package cromwell.backend.google.pipelines.common.api

import com.google.api.client.http.HttpRequest
import cromwell.backend.BackendJobDescriptor
import cromwell.backend.google.pipelines.common.api.PipelinesApiRequestFactory.CreatePipelineParameters
import cromwell.backend.google.pipelines.common._
import cromwell.backend.standard.StandardAsyncJob
import cromwell.core.StandardPaths
import cromwell.core.labels.Labels
import common.util.StringUtil._

/**
  * The PipelinesApiRequestFactory defines the HttpRequests needed to run jobs
  */
trait PipelinesApiRequestFactory {
  def runRequest(createPipelineParameters: CreatePipelineParameters): HttpRequest
  def getRequest(job: StandardAsyncJob): HttpRequest
  def cancelRequest(job: StandardAsyncJob): HttpRequest
}

object PipelinesApiRequestFactory {
  case class InputOutputParameters(
                                    fileInputParameters: List[PipelinesApiFileInput],
                                    fileOutputParameters: List[PipelinesApiFileOutput],
                                    literalInputParameters: List[PipelinesApiLiteralInput]
                                  )
  
  case class CreatePipelineParameters(jobDescriptor: BackendJobDescriptor,
                                      runtimeAttributes: PipelinesApiRuntimeAttributes,
                                      dockerImage: String,
                                      callRootPath: String,
                                      commandLine: String,
                                      logFileName: String,
                                      inputOutputParameters: InputOutputParameters,
                                      projectId: String,
                                      computeServiceAccount: String,
                                      labels: Labels,
                                      preemptible: Boolean,
                                      standardPaths: StandardPaths) {
    def inputParameters = inputOutputParameters.literalInputParameters ++ inputOutputParameters.fileInputParameters
    def outputParameters = inputOutputParameters.fileOutputParameters
    def allParameters = inputParameters ++ outputParameters

    val logGcsPath = callRootPath.ensureSlashed + logFileName
  }
}
