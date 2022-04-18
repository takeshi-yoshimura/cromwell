package cromwell.backend.impl.k8s

import akka.actor.ActorRef
import cromwell.backend.io.WorkflowPaths
import cromwell.backend.standard.{StandardExpressionFunctions, StandardExpressionFunctionsParams, StandardInitializationActor, StandardInitializationActorParams, StandardInitializationData, StandardValidatedRuntimeAttributesBuilder}
import cromwell.backend.wfs.WorkflowPathBuilder
import cromwell.backend.{BackendConfigurationDescriptor, BackendWorkflowDescriptor}
import cromwell.core.path.DefaultPathBuilder
import wom.graph.CommandCallNode

case class K8sBackendInitializationData
(
  override val workflowPaths: WorkflowPaths,
  override val runtimeAttributesBuilder: StandardValidatedRuntimeAttributesBuilder,
  configuration: K8sConfiguration
) extends StandardInitializationData(workflowPaths, runtimeAttributesBuilder, K8sBackendInitializationDataUtility.getExpressionFunctionsClass(configuration.fileSystem))

object  K8sBackendInitializationDataUtility {
  def getExpressionFunctionsClass(fs: String) = classOf[K8sExpressionFunctionsForFS]
}

// preMapping resolves relative path at WDL (e.g., read_tsv("sequence_grouping.txt") at output)
class K8sExpressionFunctions(standardParams: StandardExpressionFunctionsParams)
  extends StandardExpressionFunctions(standardParams) {
  override def preMapping(str: String) = {
    if (DefaultPathBuilder.get(str).isAbsolute) str
    else callContext.root.resolve(str.stripPrefix("/")).pathAsString
  }
}

class K8sExpressionFunctionsForFS(standardParams: StandardExpressionFunctionsParams)
  extends StandardExpressionFunctions(standardParams) {
  override def preMapping(str: String) = {
    if (DefaultPathBuilder.get(str).isAbsolute) str
    else callContext.root.resolve(str.stripPrefix("/")).pathAsString
  }
}

case class K8sInitializationActorParams
(
  workflowDescriptor: BackendWorkflowDescriptor,
  ioActor: ActorRef,
  calls: Set[CommandCallNode],
  configuration: K8sConfiguration,
  serviceRegistryActor: ActorRef,
  restarting: Boolean
) extends StandardInitializationActorParams {
  override val configurationDescriptor: BackendConfigurationDescriptor = configuration.configurationDescriptor
}

class K8sInitializationActor(params: K8sInitializationActorParams) extends StandardInitializationActor(params) {
  override lazy val pathBuilders = standardParams.configurationDescriptor.pathBuildersWithDefault(workflowDescriptor.workflowOptions)
  override lazy val workflowPaths = pathBuilders map { WorkflowPathBuilder.workflowPaths(configurationDescriptor, workflowDescriptor, _) }
  override lazy val runtimeAttributesBuilder = K8sRuntimeAttributes.runtimeAttributesBuilder(params.configuration)
  override lazy val initializationData = for {
    workflowPaths <- workflowPaths
  } yield K8sBackendInitializationData(workflowPaths, runtimeAttributesBuilder, params.configuration)
}
