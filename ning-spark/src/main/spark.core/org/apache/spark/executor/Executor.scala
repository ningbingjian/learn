/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.executor

import java.io.{File, NotSerializableException}
import java.lang.management.ManagementFactory
import java.net.URL
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.rpc.RpcTimeout
import org.apache.spark.scheduler.{DirectTaskResult, IndirectTaskResult, Task}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.{StorageLevel, TaskResultBlockId}
import org.apache.spark.util._

/**
 * Spark executor, backed by a threadpool to run tasks.
 *
 * This can be used with Mesos, YARN, and the standalone scheduler.
 * An internal RPC interface (at the moment Akka) is used for communication with the driver,
 * except in the case of Mesos fine-grained mode.
 */
/**
  * spark executor依赖线程池来运行任务
  * 可以通过Mesos,YARN,和standalone调度器调用
  * 利用RPC接口和Driver交互
  *
  * @param executorId
  * @param executorHostname
  * @param env
  * @param userClassPath
  * @param isLocal
  */
private[spark] class Executor(
    executorId: String,//executorId
    executorHostname: String,//executor运行主机名
    env: SparkEnv,//SParkEnv
    userClassPath: Seq[URL] = Nil,//类路径
    isLocal: Boolean = false)//是否本地运行
  extends Logging {

  logInfo(s"Starting executor ID $executorId on host $executorHostname")

  // Application dependencies (added through SparkContext) that we've fetched so far on this node.
  // Each map holds the master's timestamp for the version of that file or JAR we got.
  private val currentFiles: HashMap[String, Long] = new HashMap[String, Long]()//依赖文件
  private val currentJars: HashMap[String, Long] = new HashMap[String, Long]()//依赖的jar

  private val EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new Array[Byte](0))//空的缓冲字节流

  private val conf = env.conf

  // No ip or host:port - just hostname
  //检查主机名 不能是IP或者host:port得形式
  Utils.checkHost(executorHostname, "Expected executed slave to be a hostname")
  // must not have port specified.
  //主机名后不能带有端口
  assert (0 == Utils.parseHostPort(executorHostname)._2)

  // Make sure the local hostname we report matches the cluster scheduler's name for this host
  Utils.setCustomHostname(executorHostname)

  if (!isLocal) {//非本地模式运行
    // Setup an uncaught exception handler for non-local mode.
    // Make any thread terminations due to uncaught exceptions kill the entire
    // executor process to avoid surprising stalls.
    /**
      * 非本地模式，配置未捕获异常的处理器,让所有因为未捕获异常的线程终止，杀死额外的executor处理器避免意外抛出
      */
    Thread.setDefaultUncaughtExceptionHandler(SparkUncaughtExceptionHandler)
  }

  // Start worker thread pool
  //线程池
  private val threadPool = ThreadUtils.newDaemonCachedThreadPool("Executor task launch worker")
  //资源度量有关
  private val executorSource = new ExecutorSource(threadPool, executorId)

  if (!isLocal) {//非本地模式
    env.metricsSystem.registerSource(executorSource)
    //初始化blockManager
    env.blockManager.initialize(conf.getAppId)
  }

  // Whether to load classes in user jars before those in Spark jars
  //如果类相同 是否优先加载用户的类，默认不是
  private val userClassPathFirst = conf.getBoolean("spark.executor.userClassPathFirst", false)

  // Create our ClassLoader
  // do this after SparkEnv creation so can access the SecurityManager
  //url加载器,用来加载运行时需要的类
  private val urlClassLoader = createClassLoader()
  private val replClassLoader = addReplClassLoaderIfNeeded(urlClassLoader)

  // Set the classloader for serializer
  //设置序列化默认的类加载器
  env.serializer.setDefaultClassLoader(replClassLoader)

  // Akka's message frame size. If task result is bigger than this, we use the block manager
  // to send the result back.
  //akka框架的大小 字节【其实是akka和netty】默认是netty 默认是128M
  private val akkaFrameSize = AkkaUtils.maxFrameSizeBytes(conf)

  // Limit of bytes for total size of results (default is 1GB)
  //结果总大小限制 默认最多是1G
  private val maxResultSize = Utils.getMaxResultSize(conf)

  // Maintains the list of running tasks.
  //运行任务
  private val runningTasks = new ConcurrentHashMap[Long, TaskRunner]

  // Executor for the heartbeat task.
  //心跳 定时发送心跳给driver表示自己还活着
  private val heartbeater = ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-heartbeater")

  // must be initialized before running startDriverHeartbeat()
  private val heartbeatReceiverRef =
    RpcUtils.makeDriverRef(HeartbeatReceiver.ENDPOINT_NAME, conf, env.rpcEnv)//driver地址 ENDPOINT_NAME是代表当前的executor得endpoint名称

  startDriverHeartbeater()

  def launchTask(
      context: ExecutorBackend,
      taskId: Long,
      attemptNumber: Int,
      taskName: String,
      serializedTask: ByteBuffer): Unit = {
    val tr = new TaskRunner(context, taskId = taskId, attemptNumber = attemptNumber, taskName,
      serializedTask)
    runningTasks.put(taskId, tr)
    threadPool.execute(tr)
  }

  def killTask(taskId: Long, interruptThread: Boolean): Unit = {
    val tr = runningTasks.get(taskId)
    if (tr != null) {
      tr.kill(interruptThread)
    }
  }

  def stop(): Unit = {
    env.metricsSystem.report()
    heartbeater.shutdown()
    heartbeater.awaitTermination(10, TimeUnit.SECONDS)
    threadPool.shutdown()
    if (!isLocal) {
      env.stop()
    }
  }

  /** Returns the total amount of time this JVM process has spent in garbage collection. */
  //返回JVM垃圾收集运行时间
  private def computeTotalGcTime(): Long = {
    ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getCollectionTime).sum
  }
//任务运行 很重要
  class TaskRunner(
      execBackend: ExecutorBackend,
      val taskId: Long,
      val attemptNumber: Int,
      taskName: String,
      serializedTask: ByteBuffer)
    extends Runnable {

    /** Whether this task has been killed. */
    @volatile private var killed = false//任务是否已经被杀死

    /** How much the JVM process has spent in GC when the task starts to run. */
    @volatile var startGCTime: Long = _ //开始GC的时间

    /**
     * The task to run. This will be set in run() by deserializing the task binary coming
     * from the driver. Once it is set, it will never be changed.
     */
    @volatile var task: Task[Any] = _//具体的任务

    def kill(interruptThread: Boolean): Unit = {//杀死任务
      logInfo(s"Executor is trying to kill $taskName (TID $taskId)")
      killed = true
      if (task != null) {
        task.kill(interruptThread)
      }
    }

    override def run(): Unit = {
      //任务内存管理
      val taskMemoryManager = new TaskMemoryManager(env.memoryManager, taskId)
      //反序列化任务开始时间
      val deserializeStartTime = System.currentTimeMillis()
      //
      Thread.currentThread.setContextClassLoader(replClassLoader)
      //序列化实例
      val ser = env.closureSerializer.newInstance()
      logInfo(s"Running $taskName (TID $taskId)")

      //更新任务状态
      execBackend.statusUpdate(taskId, TaskState.RUNNING, EMPTY_BYTE_BUFFER)
      var taskStart: Long = 0
      //开始GC的时间
      startGCTime = computeTotalGcTime()

      try {
        //反序列化任务的依赖文件和jar文件和任务本身
        val (taskFiles, taskJars, taskBytes) = Task.deserializeWithDependencies(serializedTask)
        //更新文件并加载
        updateDependencies(taskFiles, taskJars)
        //反序列化任务本身
        task = ser.deserialize[Task[Any]](taskBytes, Thread.currentThread.getContextClassLoader)
        //任务得内存管理 包含UnifiedMemoryManager和StaticMemoryManager 默认是UnifiedMemoryManager
        task.setTaskMemoryManager(taskMemoryManager)

        // If this task has been killed before we deserialized it, let's quit now. Otherwise,
        // continue executing the task.
        if (killed) {//已经被杀死
          // Throw an exception rather than returning, because returning within a try{} block
          // causes a NonLocalReturnControl exception to be thrown. The NonLocalReturnControl
          // exception will be caught by the catch block, leading to an incorrect ExceptionFailure
          // for the task.
          throw new TaskKilledException
        }
        //epoch 还不太了解
        logDebug("Task " + taskId + "'s epoch is " + task.epoch)
        env.mapOutputTracker.updateEpoch(task.epoch)

        // Run the actual task and measure its runtime.
        //任务开始
        taskStart = System.currentTimeMillis()
        var threwException = true
        val (value, accumUpdates) = try {
          //调用具体的任务实现的run方法 包含ShuffleMapTask 和ResultTaks方法
          val res = task.run(
            taskAttemptId = taskId,
            attemptNumber = attemptNumber,
            metricsSystem = env.metricsSystem)
          threwException = false
          res//返回
        } finally {
          //清理分配的内存
          val freedMemory = taskMemoryManager.cleanUpAllAllocatedMemory()
          if (freedMemory > 0) {
            val errMsg = s"Managed memory leak detected; size = $freedMemory bytes, TID = $taskId"
            if (conf.getBoolean("spark.unsafe.exceptionOnMemoryLeak", false) && !threwException) {
              throw new SparkException(errMsg)
            } else {
              logError(errMsg)
            }
          }
        }
        //任务完成时间
        val taskFinish = System.currentTimeMillis()

        // If the task has been killed, let's fail it.
        if (task.killed) {//如果任务呗杀死 认为任务失败 抛出异常
          throw new TaskKilledException
        }
        //结果序列化的工具
        val resultSer = env.serializer.newInstance()
        val beforeSerialization = System.currentTimeMillis()
        //序列化结果
        val valueBytes = resultSer.serialize(value)
        val afterSerialization = System.currentTimeMillis()

        for (m <- task.metrics) {
          // Deserialization happens in two parts: first, we deserialize a Task object, which
          // includes the Partition. Second, Task.run() deserializes the RDD and function to be run.
          m.setExecutorDeserializeTime(
            (taskStart - deserializeStartTime) + task.executorDeserializeTime)
          // We need to subtract Task.run()'s deserialization time to avoid double-counting
          m.setExecutorRunTime((taskFinish - taskStart) - task.executorDeserializeTime)
          m.setJvmGCTime(computeTotalGcTime() - startGCTime)
          m.setResultSerializationTime(afterSerialization - beforeSerialization)
          m.updateAccumulators()
        }
        //封装任务运行结果
        val directResult = new DirectTaskResult(valueBytes, accumUpdates, task.metrics.orNull)
        //序列化任务运行结果
        val serializedDirectResult = ser.serialize(directResult)
        //任务运行结果大小
        val resultSize = serializedDirectResult.limit

        // directSend = sending directly back to the driver
        val serializedResult: ByteBuffer = {
          //结果大小大于maxResultSize,默认大于1G，不会直接返回结果给driver，而是driver需要的话根据task的blockId直接通过远程下载结果
          if (maxResultSize > 0 && resultSize > maxResultSize) {
            logWarning(s"Finished $taskName (TID $taskId). Result is larger than maxResultSize " +
              s"(${Utils.bytesToString(resultSize)} > ${Utils.bytesToString(maxResultSize)}), " +
              s"dropping it.")
            ser.serialize(new IndirectTaskResult[Any](TaskResultBlockId(taskId), resultSize))
          } else if (resultSize >= akkaFrameSize - AkkaUtils.reservedSizeBytes) {
            //如果结果大小大于akkaFrameSize减去reservedSizeBytes reservedSizeBytes默认200K  akkaFrameSize默认是128M
            //相当于128M-0.2M=127.8M 也就是如果结果介于127.8M到1G之间,那么结果直接通过内存和硬盘的方式存储,driver就可以直接通过优先读取内存，内存被清理之后也可以读取磁盘
            val blockId = TaskResultBlockId(taskId)
            env.blockManager.putBytes(
              blockId, serializedDirectResult, StorageLevel.MEMORY_AND_DISK_SER)
            logInfo(
              s"Finished $taskName (TID $taskId). $resultSize bytes result sent via BlockManager)")
            ser.serialize(new IndirectTaskResult[Any](blockId, resultSize))
          } else {
            //如果结果小于127.8M，那么直接返回给driver端
            logInfo(s"Finished $taskName (TID $taskId). $resultSize bytes result sent to driver")
            serializedDirectResult
          }
        }
        //向driver汇报任务的状态，并带上结果
        execBackend.statusUpdate(taskId, TaskState.FINISHED, serializedResult)

      } catch {
        case ffe: FetchFailedException =>
          val reason = ffe.toTaskEndReason
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

        case _: TaskKilledException | _: InterruptedException if task.killed =>
          logInfo(s"Executor killed $taskName (TID $taskId)")
          execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(TaskKilled))

        case cDE: CommitDeniedException =>
          val reason = cDE.toTaskEndReason
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

        case t: Throwable =>
          // Attempt to exit cleanly by informing the driver of our failure.
          // If anything goes wrong (or this was a fatal exception), we will delegate to
          // the default uncaught exception handler, which will terminate the Executor.
          logError(s"Exception in $taskName (TID $taskId)", t)

          val metrics: Option[TaskMetrics] = Option(task).flatMap { task =>
            task.metrics.map { m =>
              m.setExecutorRunTime(System.currentTimeMillis() - taskStart)
              m.setJvmGCTime(computeTotalGcTime() - startGCTime)
              m.updateAccumulators()
              m
            }
          }
          val serializedTaskEndReason = {
            try {
              ser.serialize(new ExceptionFailure(t, metrics))
            } catch {
              case _: NotSerializableException =>
                // t is not serializable so just send the stacktrace
                ser.serialize(new ExceptionFailure(t, metrics, false))
            }
          }
          execBackend.statusUpdate(taskId, TaskState.FAILED, serializedTaskEndReason)

          // Don't forcibly exit unless the exception was inherently fatal, to avoid
          // stopping other tasks unnecessarily.
          if (Utils.isFatalError(t)) {
            SparkUncaughtExceptionHandler.uncaughtException(t)
          }

      } finally {
        runningTasks.remove(taskId)
      }
    }
  }

  /**
   * Create a ClassLoader for use in tasks, adding any JARs specified by the user or any classes
   * created by the interpreter to the search path
   */
  private def createClassLoader(): MutableURLClassLoader = {
    // Bootstrap the list of jars with the user class path.
    val now = System.currentTimeMillis()
    userClassPath.foreach { url =>
      currentJars(url.getPath().split("/").last) = now
    }

    val currentLoader = Utils.getContextOrSparkClassLoader

    // For each of the jars in the jarSet, add them to the class loader.
    // We assume each of the files has already been fetched.
    val urls = userClassPath.toArray ++ currentJars.keySet.map { uri =>
      new File(uri.split("/").last).toURI.toURL
    }
    if (userClassPathFirst) {//类加载器
      new ChildFirstURLClassLoader(urls, currentLoader)
    } else {
      new MutableURLClassLoader(urls, currentLoader)
    }
  }

  /**
   * If the REPL is in use, add another ClassLoader that will read
   * new classes defined by the REPL as the user types code
   */
  private def addReplClassLoaderIfNeeded(parent: ClassLoader): ClassLoader = {
    val classUri = conf.get("spark.repl.class.uri", null)
    if (classUri != null) {
      logInfo("Using REPL class URI: " + classUri)
      try {
        val _userClassPathFirst: java.lang.Boolean = userClassPathFirst
        val klass = Utils.classForName("org.apache.spark.repl.ExecutorClassLoader")
          .asInstanceOf[Class[_ <: ClassLoader]]
        val constructor = klass.getConstructor(classOf[SparkConf], classOf[String],
          classOf[ClassLoader], classOf[Boolean])
        constructor.newInstance(conf, classUri, parent, _userClassPathFirst)
      } catch {
        case _: ClassNotFoundException =>
          logError("Could not find org.apache.spark.repl.ExecutorClassLoader on classpath!")
          System.exit(1)
          null
      }
    } else {
      parent
    }
  }

  /**
   * Download any missing dependencies if we receive a new set of files and JARs from the
   * SparkContext. Also adds any new JARs we fetched to the class loader.
   */
  private def updateDependencies(newFiles: HashMap[String, Long], newJars: HashMap[String, Long]) {
    lazy val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    synchronized {
      // Fetch missing dependencies
      for ((name, timestamp) <- newFiles if currentFiles.getOrElse(name, -1L) < timestamp) {
        logInfo("Fetching " + name + " with timestamp " + timestamp)
        // Fetch file with useCache mode, close cache for local mode.
        Utils.fetchFile(name, new File(SparkFiles.getRootDirectory()), conf,
          env.securityManager, hadoopConf, timestamp, useCache = !isLocal)
        currentFiles(name) = timestamp
      }
      for ((name, timestamp) <- newJars) {
        val localName = name.split("/").last
        val currentTimeStamp = currentJars.get(name)
          .orElse(currentJars.get(localName))
          .getOrElse(-1L)
        if (currentTimeStamp < timestamp) {
          logInfo("Fetching " + name + " with timestamp " + timestamp)
          // Fetch file with useCache mode, close cache for local mode.
          Utils.fetchFile(name, new File(SparkFiles.getRootDirectory()), conf,
            env.securityManager, hadoopConf, timestamp, useCache = !isLocal)
          currentJars(name) = timestamp
          // Add it to our class loader
          val url = new File(SparkFiles.getRootDirectory(), localName).toURI.toURL
          if (!urlClassLoader.getURLs().contains(url)) {
            logInfo("Adding " + url + " to class loader")
            urlClassLoader.addURL(url)
          }
        }
      }
    }
  }

  /** Reports heartbeat and metrics for active tasks to the driver. */
  //心跳和任务度量信息发送给driver
  private def reportHeartBeat(): Unit = {
    // list of (task id, metrics) to send back to the driver
    val tasksMetrics = new ArrayBuffer[(Long, TaskMetrics)]()
    val curGCTime = computeTotalGcTime()

    for (taskRunner <- runningTasks.values().asScala) {
      if (taskRunner.task != null) {
        taskRunner.task.metrics.foreach { metrics =>
          metrics.updateShuffleReadMetrics()
          metrics.updateInputMetrics()
          metrics.setJvmGCTime(curGCTime - taskRunner.startGCTime)
          metrics.updateAccumulators()

          if (isLocal) {
            // JobProgressListener will hold an reference of it during
            // onExecutorMetricsUpdate(), then JobProgressListener can not see
            // the changes of metrics any more, so make a deep copy of it
            val copiedMetrics = Utils.deserialize[TaskMetrics](Utils.serialize(metrics))
            tasksMetrics += ((taskRunner.taskId, copiedMetrics))
          } else {
            // It will be copied by serialization
            tasksMetrics += ((taskRunner.taskId, metrics))
          }
        }
      }
    }

    val message = Heartbeat(executorId, tasksMetrics.toArray, env.blockManager.blockManagerId)
    try {
      val response = heartbeatReceiverRef.askWithRetry[HeartbeatResponse](
          message, RpcTimeout(conf, "spark.executor.heartbeatInterval", "10s"))
      if (response.reregisterBlockManager) {
        logInfo("Told to re-register on heartbeat")
        env.blockManager.reregister()
      }
    } catch {
      case NonFatal(e) => logWarning("Issue communicating with driver in heartbeater", e)
    }
  }

  /**
   * Schedules a task to report heartbeat and partial metrics for active tasks to driver.
   */
  //每隔10秒发送一次心跳
  private def startDriverHeartbeater(): Unit = {
    val intervalMs = conf.getTimeAsMs("spark.executor.heartbeatInterval", "10s")

    // Wait a random interval so the heartbeats don't end up in sync
    //第一次返送延迟时间大于等于10秒
    val initialDelay = intervalMs + (math.random * intervalMs).asInstanceOf[Int]

    val heartbeatTask = new Runnable() {//发送心跳的方法
      override def run(): Unit = Utils.logUncaughtExceptions(reportHeartBeat())
    }
    //定时调度
    heartbeater.scheduleAtFixedRate(heartbeatTask, initialDelay, intervalMs, TimeUnit.MILLISECONDS)
  }
}
