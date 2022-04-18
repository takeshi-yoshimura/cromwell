package cromwell.backend.impl.k8s

import cats.data.Validated._
import cats.syntax.either._
import com.typesafe.config.{Config, ConfigValue}
import common.exception.MessageAggregation
import common.validation.ErrorOr.ErrorOr
import common.validation.Validation.{validate, warnNotRecognized}
import cromwell.backend.CommonBackendConfigurationAttributes
import cromwell.cloudsupport.k8s.auth.K8sAuthMode
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric._
import eu.timepit.refined.refineV
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ValueReader
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

case class K8sAttributes(var auth: K8sAuthMode)

object K8sAttributes {
  lazy val Logger = LoggerFactory.getLogger(this.getClass)

  private val availableConfigKeys = CommonBackendConfigurationAttributes.commonValidConfigurationAttributeKeys ++ Set(
    "concurrent-job-limit",
    "root",
    "auth",
    "filesystems",
  )

  private val deprecatedKeys: Map[String, String] = Map()

  private val context = "k8s"

  def fromConfigs(k8sConfig: cromwell.cloudsupport.k8s.K8sConfiguration, backendConfig: Config): K8sAttributes = {
    val configKeys = backendConfig.entrySet().asScala.toSet map { entry: java.util.Map.Entry[String, ConfigValue] => entry.getKey }
    warnNotRecognized(configKeys, availableConfigKeys, context, Logger)

    configKeys.intersect(deprecatedKeys.keySet).foreach {
      key => Logger.warn(s"Found deprecated configuration key $key, replaced with ${deprecatedKeys.get(key)}")
    }

    val authMode: ErrorOr[K8sAuthMode] = {
      (for {
        authName <- validate {
          backendConfig.as[String](s"auth")
        }.toEither
        validAuth <- k8sConfig.auth(authName).toEither
      } yield validAuth).toValidated
    }

    authMode match {
      case Valid(a) => K8sAttributes(a)
      case Invalid(f) =>
        throw new IllegalArgumentException with MessageAggregation {
          override val exceptionContext = "K8s Configuration is not valid: Errors"
          override val errorMessages = f.toList
        }
    }
  }

  implicit val ficusPositiveInt: ValueReader[ErrorOr[Int Refined Positive]] =
    new ValueReader[ErrorOr[Int Refined Positive]] {
      override def read(config: Config, path: String): ErrorOr[Refined[Int, Positive]] = {
        val int = config.getInt(path)
        refineV[Positive](int).toValidatedNel
      }
    }
}
