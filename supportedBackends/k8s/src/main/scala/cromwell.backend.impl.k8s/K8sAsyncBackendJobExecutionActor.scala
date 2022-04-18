package cromwell.backend.impl.k8s

import java.io.FileNotFoundException
import java.nio.file.{Files, Paths}
import akka.pattern.AskSupport
import common.util.StringUtil._
import cromwell.backend._
import cromwell.backend.async.{ExecutionHandle, PendingExecutionHandle}
import cromwell.backend.standard.{StandardAsyncExecutionActor, StandardAsyncExecutionActorParams, StandardAsyncJob, StandardCachingActorHelper}
import cromwell.core.retry.SimpleExponentialBackoff
import cromwell.services.keyvalue.KvClient
import org.slf4j.{Logger, LoggerFactory}
import _root_.io.kubernetes.client.openapi.ApiClient
import _root_.io.kubernetes.client.openapi.apis.{BatchV1Api, CoreV1Api}
import _root_.io.kubernetes.client.openapi.models.{V1DeleteOptionsBuilder, V1EnvVarBuilder, V1Job, V1JobBuilder, V1JobStatus, V1LocalObjectReferenceBuilder, V1TolerationBuilder, V1VolumeBuilder, V1VolumeMountBuilder}
import _root_.io.kubernetes.client.custom.Quantity
import _root_.io.kubernetes.client.openapi.models.V1Toleration._
import cromwell.backend.io.JobPaths
import cromwell.core.ExecutionEvent
import cromwell.core.path.{DefaultPath, DefaultPathBuilder, Path, PathBuilder, PathFactory}
import net.ceedubs.ficus.Ficus._
import wom.callable.Callable.OutputDefinition
import wom.expression.NoIoFunctionSet
import wom.values._

import scala.concurrent.duration._
import scala.concurrent.Future
import scala.language.postfixOps
import collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object K8sAsyncBackendJobExecutionActor {
  val K8sOperationIdKey = "__k8s_operation_id"
  var jobCount = 0
  def getJobCount(): String = classOf[K8sAsyncBackendJobExecutionActor].synchronized {
    val j = jobCount
    jobCount += 1
    j.toHexString
  }
}

class K8sAsyncBackendJobExecutionActor(override val standardParams: StandardAsyncExecutionActorParams)
  extends BackendJobLifecycleActor with StandardAsyncExecutionActor with StandardCachingActorHelper
    with KvClient with AskSupport {

  val Log: Logger = LoggerFactory.getLogger(K8sAsyncBackendJobExecutionActor.getClass)

  lazy val initializationData: K8sBackendInitializationData = backendInitializationDataAs[K8sBackendInitializationData]

  lazy val configuration: K8sConfiguration = initializationData.configuration

  lazy val callPaths: JobPaths = jobPaths

  lazy val runtimeAttributes: K8sRuntimeAttributes = K8sRuntimeAttributes(validatedRuntimeAttributes, configuration.runtimeConfig)

  lazy val workingDiskMount: Path = DefaultPathBuilder.get(K8sConfiguration.workingDir)

  lazy val callRootPath: Path = callPaths.callExecutionRoot
  lazy val returnCodeFilename: String = callPaths.returnCodeFilename

  def client(): ApiClient = configuration.k8sAuth.client()

  override type StandardAsyncRunInfo = V1Job

  override type StandardAsyncRunState = V1JobStatus

  def statusEquivalentTo(thiz: StandardAsyncRunState)(that: StandardAsyncRunState): Boolean = thiz == that
  override lazy val dockerImageUsed: Option[String] = Option(jobDescriptor.maybeCallCachingEligible.dockerHash.getOrElse(runtimeAttributes.dockerImage))

  lazy val localMountPoint = DefaultPathBuilder.get(K8sConfiguration.workingDir)
  lazy val workDir = "/tmp/scratch"

  lazy val reconfiguredScript: String = {
    var commandScript = commandScriptContents.toEither.right.get
    val insertionPoint = commandScript.indexOf("\n", commandScript.indexOf("#!")) +1 //just after the new line after the shebang!
    val initCommand =
      s"""
      |if ! `java -version > /dev/null 2> /dev/null`; then unset JAVA_TOOL_OPTIONS; fi
      |mkdir -p $workDir ${jobPaths.callExecutionRoot.pathWithoutScheme} && chmod 777 $workDir
      |""".stripMargin
    commandScript = commandScript.patch(insertionPoint, initCommand, 0)
    commandScript
  }

  lazy val k8sJob: V1Job = {
    val args = List ("bash", "-c", reconfiguredScript)
    val taskName = jobDescriptor.key.call.fullyQualifiedName.toLowerCase().replace('.', '-').replace('_', '-')
    val jobName: String = {
      var j = jobDescriptor.workflowDescriptor.id.shortString + "-" + K8sAsyncBackendJobExecutionActor.getJobCount()
      if (taskName != "") {
        j += "-" + taskName
      }
      val idxLen = jobDescriptor.key.index match {
        case Some(idx) => 1 + idx.toString.length
        case _ => 0
      }
      if (j.length + idxLen > 63) {
        j = j.substring(0, 63 - idxLen)
      }
      if (j.endsWith("-")) {
        j = j.substring(0, j.length - 1)
      }
      j
    }
    val name: String = jobDescriptor.key.index match {
      case Some(idx) => jobName + "-" + idx.toString
      case None => jobName
    }
    val labels = Map(
      "jobgroup" -> jobName,
      "app" -> "cromwell",
      "workflowid" -> jobDescriptor.workflowDescriptor.id.shortString,
      "taskidx" -> jobDescriptor.key.index.getOrElse(0).toString,
      "name" -> name
    ).asJava

    val resources = Map (
      "cpu" -> new Quantity(runtimeAttributes.cpu.toString),
      "memory" -> new Quantity({
        val mem = runtimeAttributes.memory.bytes
        if (mem >= 1024 * 1024 * 1024) {
          s"%.1fGi".format(mem.toFloat / 1024 / 1024 / 1024)
        } else if (mem >= 1024 * 1024) {
          s"%.1fMi".format(mem.toFloat / 1024 / 1024)
        } else {
          mem.toString
        }
      }),
    )

    var k8sJob_0 = new V1JobBuilder()
      .withNewMetadata()
      .withName(name)
      .withNamespace(configuration.namespace)
      .withLabels(labels)
      .endMetadata()
      .withNewSpec()
      .withNewTemplate()
      .withNewMetadata()
      .withAnnotations(Map("cluster-autoscaler.kubernetes.io/safe-to-evict" -> "false").asJava)
      .withName(name)
      .withLabels(labels)
      .endMetadata()
      .withNewSpec()

    if (configuration.imageSecretKey.nonEmpty)
      k8sJob_0 = k8sJob_0.withImagePullSecrets(configuration.imageSecretKey.map(new V1LocalObjectReferenceBuilder().withName(_).build()).asJava)
    if (configuration.nodeSelector.nonEmpty)
      k8sJob_0 = k8sJob_0.withNodeSelector(configuration.nodeSelector.asJava)
    if (configuration.k8sServiceAccountName != "" && configuration.k8sServiceAccountName != "default")
      k8sJob_0 = k8sJob_0.withServiceAccount(configuration.k8sServiceAccountName)

    k8sJob_0 = k8sJob_0
      .addNewContainer()
      .withName(name)
      .withImage(runtimeAttributes.dockerImage)
      .withImagePullPolicy(_root_.io.kubernetes.client.openapi.models.V1Container.ImagePullPolicyEnum.ALWAYS)
      .withNewResources()
      .withRequests(resources.asJava)
      .withLimits(resources.asJava)
      .endResources()
      .withCommand(args.asJava)
      .withVolumeMounts(
        new V1VolumeMountBuilder()
          .withName(configuration.pvcName)
          .withMountPath(K8sConfiguration.workingDir)
          .build()
      )
      .withEnv(
        new V1EnvVarBuilder().
          withName("JAVA_TOOL_OPTIONS")
          .withValue("-XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap")
          .build()
      )
      .endContainer()
      .withVolumes(
        new V1VolumeBuilder()
          .withName(configuration.pvcName)
          .withNewPersistentVolumeClaim()
          .withClaimName(configuration.pvcName)
          .endPersistentVolumeClaim()
          .build()
      )
      .withRestartPolicy(_root_.io.kubernetes.client.openapi.models.V1PodSpec.RestartPolicyEnum.NEVER)
      .withSchedulerName(configuration.schedulerName)

    if(configuration.tolerations.nonEmpty)
      k8sJob_0 = k8sJob_0.withTolerations(new V1TolerationBuilder()
            .withKey(configuration.tolerations("key"))
            .withOperator(OperatorEnum.fromValue(configuration.tolerations("operator")))
            .withValue(configuration.tolerations("value"))
            .withEffect(EffectEnum.fromValue(configuration.tolerations("effect")))
            .build())

    k8sJob_0
      .endSpec()
      .endTemplate()
      .withBackoffLimit(5)
      .endSpec()
      .build()
  }

  override def tryAbort(job: StandardAsyncJob): Unit = {
    val api = new CoreV1Api(client)
    val pods = api.listNamespacedPod(k8sJob.getMetadata.getNamespace, null, null, null, null, "name=" + k8sJob.getMetadata.getName, null, null, null, null, null)
    if (pods == null || pods.getItems == null || pods.getItems.size() == 0)
      return
    val pod = pods.getItems.get(0)

    val batchApi = new BatchV1Api(client)
    try {
      batchApi.deleteNamespacedJobWithHttpInfo(k8sJob.getMetadata.getName, k8sJob.getMetadata.getNamespace, null, null, null, null, null, new V1DeleteOptionsBuilder().build())
    } catch {
      case _: com.google.gson.JsonSyntaxException => /* ignore */
    }
    try {
      api.deleteNamespacedPodWithHttpInfo(pod.getMetadata.getName, pod.getMetadata.getNamespace, null, null, null, null, null, null)
    } catch {
      case _: com.google.gson.JsonSyntaxException => /* ignore */
    }
    Log.info(s"Attempted CancelJob operation in k8s for Job ID ${job.jobId}.")
  }

  override def requestsAbortAndDiesImmediately: Boolean = false

  /**
    * Turns WomFiles into relative paths.  These paths are relative to the working disk.
    *
    * relativeLocalizationPath("foo/bar.txt") -> "foo/bar.txt"
    * relativeLocalizationPath("s3://some/bucket/foo.txt") -> "some/bucket/foo.txt"
    */
  override protected def relativeLocalizationPath(file: WomFile): WomFile = {
    file.mapFile(value =>
      getPath(value) match {
        case Success(path) =>
          path match {
            case localPath: DefaultPath => localPath.pathAsString
            case _ => path.pathWithoutScheme
          }
        case _ => value
      }
    )
  }

  private def relativePathAndVolume(path: String, mntPath: Path): Path = {
    val absolutePath = DefaultPathBuilder.get(path) match {
      case p if !p.isAbsolute =>
          commandDirectory.resolve(path)
      case p => p
    }

    if (absolutePath.startsWith(mntPath)) {
      mntPath.relativize(absolutePath)
    } else {
      throw new Exception(s"Absolute path $path doesn't appear to be under any mount points: $mntPath")
    }
  }

  private def makeSafeK8sJobReferenceName(referenceName: String) = {
    if (referenceName.length <= 127) referenceName else referenceName.md5Sum
  }

  private[k8s] def generateK8sJobOutputs(jobDescriptor: BackendJobDescriptor): Set[K8sJobFileOutput] = {
    import cats.syntax.validated._
    def evaluateFiles(output: OutputDefinition): List[WomFile] = {
      Try(
        output.expression.evaluateFiles(jobDescriptor.localInputs, NoIoFunctionSet, output.womType).map(_.toList map { _.file })
      ).getOrElse(List.empty[WomFile].validNel)
        .getOrElse(List.empty)
    }

    val womFileOutputs = jobDescriptor.taskCall.callable.outputs.flatMap(evaluateFiles) map relativeLocalizationPath

    val outputs: Seq[K8sJobFileOutput] = womFileOutputs.distinct flatMap {
      _.flattenFiles flatMap {
        case unlistedDirectory: WomUnlistedDirectory => generateUnlistedDirectoryOutputs(unlistedDirectory)
        case singleFile: WomSingleFile => generateK8sJobSingleFileOutputs(singleFile)
        case globFile: WomGlobFile => generateK8sJobGlobFileOutputs(globFile)
      }
    }

    val additionalGlobOutput = jobDescriptor.taskCall.callable.additionalGlob.toList.flatMap(generateK8sJobGlobFileOutputs).toSet

    outputs.toSet ++ additionalGlobOutput
  }

  private def generateUnlistedDirectoryOutputs(womFile: WomUnlistedDirectory): List[K8sJobFileOutput] = {
    val directoryPath = womFile.value.ensureSlashed
    val directoryListFile = womFile.value.ensureUnslashed + ".list"
    val dirDestinationPath = callRootPath.resolve(directoryPath).pathAsString
    val listDestinationPath = callRootPath.resolve(directoryListFile).pathAsString

    relativePathAndVolume(womFile.value, workingDiskMount)

    // We need both the collection directory and the collection list:
    List(
      // The collection directory:
      K8sJobFileOutput(
        makeSafeK8sJobReferenceName(directoryListFile),
        listDestinationPath,
        DefaultPathBuilder.get(directoryListFile)
      ),
      // The collection list file:
      K8sJobFileOutput(
        makeSafeK8sJobReferenceName(directoryPath),
        dirDestinationPath,
        DefaultPathBuilder.get(directoryPath + "*")
      )
    )
  }

  private def generateK8sJobSingleFileOutputs(womFile: WomSingleFile): List[K8sJobFileOutput] = {
    val destination = callRootPath.resolve(womFile.value.stripPrefix("/")).pathAsString
    val relpath = relativePathAndVolume(womFile.value, workingDiskMount)
    val output = K8sJobFileOutput(makeSafeK8sJobReferenceName(womFile.value), destination, relpath)
    List(output)
  }

  private def generateK8sJobGlobFileOutputs(womFile: WomGlobFile): List[K8sJobFileOutput] = {
    val globName = GlobFunctions.globName(womFile.value)
    val globDirectory = globName + "/"
    val globListFile = globName + ".list"
    val globDirectoryDestinationPath = callRootPath.resolve(globDirectory).pathAsString
    val globListFileDestinationPath = callRootPath.resolve(globListFile).pathAsString

    relativePathAndVolume(womFile.value, workingDiskMount)

    // We need both the glob directory and the glob list:
    List(
      // The glob directory:
      K8sJobFileOutput(makeSafeK8sJobReferenceName(globDirectory), globDirectoryDestinationPath, DefaultPathBuilder.get(globDirectory + "*")),
      // The glob list file:
      K8sJobFileOutput(makeSafeK8sJobReferenceName(globListFile), globListFileDestinationPath, DefaultPathBuilder.get(globListFile))
    )
  }

  override lazy val temporaryDirectory: String = {
    val dstDir = runtimeEnvironment.tempPath.replaceFirst(localMountPoint.pathAsString, workDir)
    configurationDescriptor.backendConfig.getOrElse(
      path = "temporary-directory",
      default = s"""$$(mkdir -p "${dstDir}" && echo "${dstDir}")""")
  }

  override lazy val scriptEpilogue: String = configurationDescriptor.backendConfig.as[Option[String]]("script-epilogue").getOrElse("")

  override def globScript(globFile: WomGlobFile): String = {
    val parentDirectory = globParentDirectory(globFile)
    val globDir = GlobFunctions.globName(globFile.value)
    val globDirectory = parentDirectory./(globDir)
    val globList = parentDirectory./(s"$globDir.list")
    val absoluteGlobValue = commandDirectory.resolve(globFile.value).pathAsString
    val globLinkCommand: String = configurationDescriptor.backendConfig.getAs[String]("glob-link-command")
      .map("( " + _ + " )")
      .getOrElse(s"( cp ${absoluteGlobValue} ${globDirectory.pathAsString} 2> /dev/null )")

    s"""|mkdir $globDirectory
        |$globLinkCommand
        |ls -1 $globDirectory > $globList
        |""".stripMargin
  }

  override def globParentDirectory(womGlobFile: WomGlobFile): Path = commandDirectory

  override def getTerminalEvents(runStatus: V1JobStatus): Seq[ExecutionEvent] = {
    if (runStatus.getCompletionTime != null) {
      val api = new CoreV1Api(client)
      val pods = api.listNamespacedPod(k8sJob.getMetadata.getNamespace, null, null, null, null, "name=" + k8sJob.getMetadata.getName, null, null, null, null, null)
      if (pods == null || pods.getItems == null || pods.getItems.size() == 0)
        return Seq.empty
      val pod = pods.getItems.get(0)
      if (configuration.fileSystem == "local") {
        try {
          val yamlPath = commandDirectory.resolve("pod.log")
          if (runStatus.getFailed != null) {
            Files.createDirectories(Paths.get(yamlPath.getParent.pathAsString))
          }
          Files.write(Paths.get(yamlPath.pathAsString), pod.toString.getBytes())
        } catch {
          case e: Exception => log.warning("Failed to retrieve logs and job deletion (ignored): " + e.toString)
        }
      }
      try {
        batchApi.deleteNamespacedJobWithHttpInfo(k8sJob.getMetadata.getName, k8sJob.getMetadata.getNamespace, null, null, null, null, null, null)
      } catch {
        case _: com.google.gson.JsonSyntaxException => /* ignore */
      }
      try {
        api.deleteNamespacedPodWithHttpInfo(pod.getMetadata.getName, pod.getMetadata.getNamespace, null, null, null, null, null, null)
      } catch {
        case _: com.google.gson.JsonSyntaxException => /* ignore */
        case e: _root_.io.kubernetes.client.openapi.ApiException => log.warning("failed: delete pod: " + e.toString)
      }
    }
    Seq.empty
  }

  override def isTerminal(runStatus: V1JobStatus): Boolean = runStatus.getCompletionTime != null

  // Primary entry point for cromwell to actually run something
  override def executeAsync(): Future[ExecutionHandle] = {
    for {
      job <- Future(Try{
        val batchApi = new BatchV1Api(client)
        batchApi.createNamespacedJob(k8sJob.getMetadata.getNamespace, k8sJob, null, null, null, null)
      } match {
        case Success(v) => v
        case Failure(e) => throw new Exception(s"header: ${e.getMessage}")
      })
    } yield PendingExecutionHandle(jobDescriptor, StandardAsyncJob(job.getMetadata.getName), Option(job), previousState = None)
  }

  override def recoverAsync(jobId: StandardAsyncJob): Future[ExecutionHandle] =  reconnectAsync(jobId)

  override def reconnectAsync(jobId: StandardAsyncJob): Future[ExecutionHandle] =
    Future.successful(PendingExecutionHandle(jobDescriptor, jobId, Option(k8sJob), previousState = None))

  override def reconnectToAbortAsync(jobId: StandardAsyncJob): Future[ExecutionHandle] = {
    tryAbort(jobId)
    reconnectAsync(jobId)
  }

  var batchApi = new BatchV1Api(client)
  // This is called by Cromwell after initial execution (see executeAsync above)
  override def pollStatusAsync(handle: PendingExecutionHandle[StandardAsyncJob, V1Job, V1JobStatus]): Future[V1JobStatus] = {
    val job = handle.runInfo match {
      case Some(actualJob) => actualJob
      case None =>
        throw new RuntimeException(
          s"pollStatusAsync called but job not available. This should not happen. Job Id ${handle.pendingJob.jobId}"
        )
    }

    var j: V1JobStatus = null
    try {
      j = batchApi.readNamespacedJobStatus(job.getMetadata.getName, job.getMetadata.getNamespace, null).getStatus
    } catch {
      case e: _root_.io.kubernetes.client.openapi.ApiException =>
        log.info(s"Failed (Retry): readNamespacedJobStatus, name=${job.getMetadata.getName}, namespace=${job.getMetadata.getNamespace}, err=${e.getResponseBody}")
        batchApi = new BatchV1Api(client)
        j = batchApi.readNamespacedJobStatus(job.getMetadata.getName, job.getMetadata.getNamespace, null).getStatus
    }
    Future(j)
  }

  override def pollBackOff: SimpleExponentialBackoff = SimpleExponentialBackoff(1.second, 5.minutes, 1.1)

  override def executeOrRecoverBackOff: SimpleExponentialBackoff = SimpleExponentialBackoff(
    initialInterval = 3 seconds, maxInterval = 20 seconds, multiplier = 1.1)

  def hostAbsoluteFilePath(jobPaths: JobPaths, pathString: String): Path = {

    val pathBuilders:List[PathBuilder]  = List(DefaultPathBuilder)
    val path = PathFactory.buildPath(pathString, pathBuilders)
    if (!path.isAbsolute)
      jobPaths.callExecutionRoot.resolve(path).toAbsolutePath
    else if(jobPaths.isInExecution(path.pathAsString))
      jobPaths.hostPathFromContainerPath(path.pathAsString)
    else
      jobPaths.hostPathFromContainerInputs(path.pathAsString)
  }
  private[k8s] def womFileToPath(outputs: Set[K8sJobFileOutput])(womFile: WomFile): WomFile = {
    womFile mapFile { path =>
      outputs collectFirst {
        case output if output.name == makeSafeK8sJobReferenceName(path) => output.s3key
      } getOrElse path
    }
  }
  override def mapOutputWomFile(womFile: WomFile): WomFile = {
    val wfile = {
      val hostPath = hostAbsoluteFilePath(jobPaths, womFile.valueString)
      if (!hostPath.exists) throw new FileNotFoundException(s"Could not process output, file not found: ${hostPath.pathAsString}")
      womFile mapFile { _ => hostPath.pathAsString }
    }
    val w = womFileToPath(generateK8sJobOutputs(jobDescriptor))(wfile)
    w
  }
}
final case class K8sJobFileOutput(name: String, s3key: String, local: Path)