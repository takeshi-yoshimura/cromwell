package cromwell.cloudsupport.k8s.auth

import java.nio.file.{Files, Paths}

import cromwell.cloudsupport.aws.auth.AwsAuthMode.OptionLookup
import io.kubernetes.client.openapi.apis.CoreV1Api
import org.slf4j.LoggerFactory
import io.kubernetes.client.openapi.{ApiClient, Configuration}
import io.kubernetes.client.util.{ClientBuilder, KubeConfig}

import scala.util.{Failure, Success, Try}

object K8sAuthMode {
  type OptionLookup = String => String
}

sealed trait K8sAuthMode {
  protected lazy val log = LoggerFactory.getLogger(getClass.getSimpleName)

  def validate(options: OptionLookup): Unit = ()

  def name: String

  def client(): ApiClient

  private[auth] var credentialValidation: (ApiClient) => Unit =
    (client: ApiClient) => {
      // make a dummy API call just to assure ourselves the credentials from our client are valid
      new CoreV1Api(client).listPodForAllNamespaces(null, null, null, null, null, null, null, null, null, null)
      ()
    }

  protected def validateCredential(client: ApiClient): ApiClient = {
    var ret: ApiClient = null
    var lastEx: Throwable = null
    for (_ <- 0 until 10) {
      ret = Try(credentialValidation(client)) match {
        case Failure(ex) =>
          lastEx = ex
          null
        case Success(_) => client
      }
      if (ret != null) {
        return ret
      }
      Thread.sleep(1000L * 10)
    }
    if (ret == null) {
      throw new RuntimeException(s"Credentials produced by the k8s ${name} are invalid: ${lastEx.getMessage}", lastEx)
    }
    ret
  }
}

final case class DefaultMode(override val name: String) extends K8sAuthMode {
  // client() should not be variable. we need to refresh authorization
  override def client(): ApiClient = {
    val p = ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(Files.newBufferedReader(Paths.get(System.getProperty("user.home"), ".kube", "config")))).build();
    p.setConnectTimeout(24 * 60 * 60 * 1000)
    p.setReadTimeout(24 * 60 * 60 * 1000)
    p.setWriteTimeout(24 * 60 * 60 * 1000)
    Configuration.setDefaultApiClient(p)
    validateCredential(p)
  }
}

final case class InClusterMode(override val name: String) extends K8sAuthMode {
  override def client(): ApiClient = {
    val p = ClientBuilder.cluster.build
    p.setConnectTimeout(24 * 60 * 60 * 1000)
    p.setReadTimeout(24 * 60 * 60 * 1000)
    p.setWriteTimeout(24 * 60 * 60 * 1000)
    Configuration.setDefaultApiClient(p)
    validateCredential(p)
  }
}

class OptionLookupException(val key: String, cause: Throwable) extends RuntimeException(key, cause)
