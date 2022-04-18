package cromwell.backend.impl.k8s

import akka.actor.ActorRef
import cromwell.backend.standard._
import cromwell.backend.{BackendConfigurationDescriptor, BackendWorkflowDescriptor}
import wom.graph.CommandCallNode

/**
  * Factory to create `Actor` objects to manage the lifecycle of a backend job on k8s. This factory provides an
  * object from the `K8sBatchAsyncBackendJobExecutionActor` class to create and manage the job.
  * @param name Factory name
  * @param configurationDescriptor configuration descriptor for the backend
  */
case class K8sBackendLifecycleActorFactory(
  name: String,
  configurationDescriptor: BackendConfigurationDescriptor)
    extends StandardLifecycleActorFactory {

  override lazy val initializationActorClass: Class[_ <: StandardInitializationActor]
    = classOf[K8sInitializationActor]

  override lazy val asyncExecutionActorClass: Class[_ <: StandardAsyncExecutionActor]
    = classOf[K8sAsyncBackendJobExecutionActor]

  override lazy val jobIdKey: String
    = K8sAsyncBackendJobExecutionActor.K8sOperationIdKey

  val configuration = new K8sConfiguration(configurationDescriptor)

  override def workflowInitializationActorParams(workflowDescriptor: BackendWorkflowDescriptor,
                                                 ioActor: ActorRef,
                                                 calls: Set[CommandCallNode],
                                                 serviceRegistryActor: ActorRef,
                                                 restart: Boolean): StandardInitializationActorParams = {
    K8sInitializationActorParams(workflowDescriptor, ioActor, calls, configuration, serviceRegistryActor, restart)
  }
}
