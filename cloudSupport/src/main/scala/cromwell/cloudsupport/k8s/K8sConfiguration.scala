package cromwell.cloudsupport.k8s

import cats.data.Validated._
import cats.instances.list._
import cats.syntax.traverse._
import cats.syntax.validated._
import com.typesafe.config.Config
import common.exception.MessageAggregation
import common.validation.ErrorOr._
import common.validation.Validation._
import cromwell.cloudsupport.k8s.auth.{DefaultMode, InClusterMode, K8sAuthMode}
import net.ceedubs.ficus.Ficus._
import org.slf4j.LoggerFactory

final case class K8sConfiguration private(authsByName: Map[String, K8sAuthMode]) {

  def auth(name: String): ErrorOr[K8sAuthMode] = {
    authsByName.get(name) match {
      case None =>
        val knownAuthNames = authsByName.keys.mkString(", ")
        s"`k8s` configuration stanza does not contain an auth named '$name'.  Known auth names: $knownAuthNames".invalidNel
      case Some(a) => a.validNel
    }
  }
}


object K8sConfiguration {
  import scala.language.postfixOps

  private val log = LoggerFactory.getLogger("K8sConfiguration")

  final case class K8sConfigurationException(errorMessages: List[String]) extends MessageAggregation {
    override val exceptionContext = "K8s configuration"
  }

  def apply(config: Config): K8sConfiguration = {

    val k8sConfig = config.getConfig("k8s")

    def buildAuth(authConfig: Config): ErrorOr[K8sAuthMode] = {

      val name = authConfig.getString("name")
      val scheme = authConfig.getString("scheme")

      scheme match {
        case "default" => validate(DefaultMode(name))
        case "incluster" => validate(InClusterMode(name))
        //case "custom_keys" => customKeyAuth(authConfig, name) // TODO
        case wut => s"Unsupported authentication scheme: $wut".invalidNel
      }
    }

    val errorOrAuthList = k8sConfig
        .as[List[Config]]("auths")
        .map(buildAuth)
        .sequence[ErrorOr, K8sAuthMode]

    def uniqueAuthNames(list: List[K8sAuthMode]): ErrorOr[Unit] = {
      val duplicateAuthNames = list.groupBy(_.name) collect { case (n, as) if as.size > 1 => n }
      if (duplicateAuthNames.nonEmpty) {
        ("Duplicate auth names: " + duplicateAuthNames.mkString(", ")).invalidNel
      } else {
        ().validNel
      }
    }

    errorOrAuthList.flatMap { list =>
      uniqueAuthNames(list) map { _ =>
        K8sConfiguration(list map { a => a.name -> a } toMap)
      }
    } match {
      case Valid(r) => r
      case Invalid(f) =>
        val errorMessages = f.toList.mkString(", ")
        log.error(errorMessages)
        throw K8sConfigurationException(f.toList)
    }
  }
}

