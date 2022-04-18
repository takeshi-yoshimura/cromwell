package cromwell.backend.impl.k8s

import cromwell.backend.BackendConfigurationDescriptor
import cromwell.core.path.DefaultPathBuilderFactory

import collection.JavaConverters._

object K8sConfiguration {
  val workingDir = "/cromwell_root"
}

class K8sConfiguration(val configurationDescriptor: BackendConfigurationDescriptor) {

  val k8sConfig = cromwell.cloudsupport.k8s.K8sConfiguration(configurationDescriptor.globalConfig)

  val root: String = configurationDescriptor.backendConfig.getString("root")
  val runtimeConfig = configurationDescriptor.backendRuntimeAttributesConfig
  val k8sAttributes = K8sAttributes.fromConfigs(k8sConfig, configurationDescriptor.backendConfig)
  val k8sAuth = k8sAttributes.auth
  val fileSystem: String = "local"
  val pathBuilderFactory = DefaultPathBuilderFactory

  def getSeqConfOr(path: String): Seq[String] = {
    if (configurationDescriptor.backendConfig.hasPath(path)) {
      configurationDescriptor.backendConfig.getStringList(path).asScala
    } else {
      Seq.empty
    }
  }

  def getConfOr(path: String, default: String): String = {
    if (configurationDescriptor.backendConfig.hasPath(path)) {
      configurationDescriptor.backendConfig.getString(path)
    } else {
      default
    }
  }

  val imageSecretKey: Seq[String] = getSeqConfOr("pullImageSecrets")
  val namespace: String = getConfOr("namespace", "default")
  val pvcName: String = getConfOr("pvcName", "my-pvc")
  val k8sServiceAccountName: String = getConfOr("k8sServiceAccountName", "default")
  val nodeSelector: Map[String, String] = {
    val nodeSelectorString = getConfOr("nodeSelector", "")
    if (nodeSelectorString != "") {
      val split = nodeSelectorString.split(":")
      if (split.length == 2) {
        Map(split(0).trim -> split(1).trim)
      } else {
        throw new Exception("nodeSelector must be key:value")
      }
    } else {
      Map.empty
    }
  }
  val tolerations: Map[String, String] = {
    val regx = "(.*)=(.*):(.*)".r
    getConfOr("tolerations", "") match {
      case regx(key, value, effect) => Map("key" -> key, "operator" -> "Equal", "value" -> value, "effect" -> effect)
      case _ => Map.empty
    }
  }
  val schedulerName: String = getConfOr("schedulerName", "default-scheduler")
}

object K8sStorageSystems {
  val s3:String = "s3"
  val local:String = "local"
}

