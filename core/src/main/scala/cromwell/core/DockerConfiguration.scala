package cromwell.core

import com.typesafe.config.ConfigFactory

import scala.concurrent.duration.FiniteDuration
import cats.data.Validated._
import cats.syntax.cartesian._
import cromwell.core.DockerConfiguration.DockerCacheMode
import lenthall.exception.AggregatedMessageException
import net.ceedubs.ficus.Ficus._
import lenthall.validation.Validation._

object DockerConfiguration {

  sealed trait DockerCacheMode {
    def cacheEntryTTL: FiniteDuration
  }

  case class CacheExpiresAfterWrite(cacheEntryTTL: FiniteDuration) extends DockerCacheMode
  case class CacheExpiresAfterAccess(cacheEntryTTL: FiniteDuration) extends DockerCacheMode
  
  private lazy val dockerConfig = ConfigFactory.load().getConfig("docker")
  private lazy val dockerHashLookupConfig = dockerConfig.getConfig("hash-lookup")
  
  lazy val instance: DockerConfiguration = {
    val gcrApiQueriesPer100Seconds = validate { dockerHashLookupConfig.as[Int]("gcr-api-queries-per-100-seconds") }
    val cacheEntryTtl = validate { dockerHashLookupConfig.as[FiniteDuration]("cache-entry-ttl") }
    val cacheEntryMode = validate { dockerHashLookupConfig.as[String]("cache-expire-mode") }
    val cacheSize = validate { dockerHashLookupConfig.as[Long]("cache-size") }
    val method = validate { dockerHashLookupConfig.as[String]("method") } map {
      case "local" => DockerLocalLookup
      case "remote" => DockerRemoteLookup
      case other => throw new IllegalArgumentException(s"Unrecognized docker hash lookup method: $other")
    }
    
    val cacheMode = (cacheEntryMode |@| cacheEntryTtl) map {
      case ("write", ttl) => CacheExpiresAfterWrite(ttl)
      case ("access", ttl) => CacheExpiresAfterAccess(ttl)
      case (other, _) => throw new IllegalArgumentException(s"Unrecognized docker cache mode: $other")
    }

    val dockerConfiguration = (gcrApiQueriesPer100Seconds |@| cacheMode |@| cacheSize |@| method) map  DockerConfiguration.apply
    
    dockerConfiguration match {
      case Valid(conf) => conf
      case Invalid(errors) => throw AggregatedMessageException("Invalid docker configuration", errors.toList)
    }
  }
}

case class DockerConfiguration(
                                gcrApiQueriesPer100Seconds: Int,
                                cacheMode: DockerCacheMode,
                                cacheSize: Long,
                                method: DockerHashLookupMethod
                              )

sealed trait DockerHashLookupMethod

case object DockerLocalLookup extends DockerHashLookupMethod
case object DockerRemoteLookup extends DockerHashLookupMethod
