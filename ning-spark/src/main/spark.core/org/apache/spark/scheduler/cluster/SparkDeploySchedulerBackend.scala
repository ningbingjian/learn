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

package org.apache.spark.scheduler.cluster

import java.util.concurrent.Semaphore

import org.apache.spark.rpc.RpcAddress
import org.apache.spark.{Logging, SparkConf, SparkContext, SparkEnv}
import org.apache.spark.deploy.{ApplicationDescription, Command}
import org.apache.spark.deploy.client.{AppClient, AppClientListener}
import org.apache.spark.launcher.{LauncherBackend, SparkAppHandle}
import org.apache.spark.scheduler._
import org.apache.spark.util.Utils

private[spark] class SparkDeploySchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext,
    masters: Array[String])
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv)
  with AppClientListener
  with Logging {

  private var client: AppClient = null
  private var stopping = false
  private val launcherBackend = new LauncherBackend() {
    override protected def onStopRequest(): Unit = stop(SparkAppHandle.State.KILLED)
  }

  @volatile var shutdownCallback: SparkDeploySchedulerBackend => Unit = _
  @volatile private var appId: String = _

  private val registrationBarrier = new Semaphore(0)

  private val maxCores = conf.getOption("spark.cores.max").map(_.toInt)
  private val totalExpectedCores = maxCores.getOrElse(0)

  //SparkDeploySchedulerBackend的启动是由在TaskSchedulerImpl启动的第一步
  //TaskSchedulerImpl.start方法首先调用的就是该方法
  override def start() {
    //先调用父类,   父类启动了Driver
    super.start()
    //和在控制台运行有关,launcherBackend可以先忽略这个
    launcherBackend.connect()
    // The endpoint for executors to talk to us
    //driverUrl
    val driverUrl = rpcEnv.uriOf(SparkEnv.driverActorSystemName,
      RpcAddress(sc.conf.get("spark.driver.host"), sc.conf.get("spark.driver.port").toInt),
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME)
    //在worker启动executorBackend进程的时候接收的参数列表
    val args = Seq(
      "--driver-url", driverUrl,
      "--executor-id", "{{EXECUTOR_ID}}",
      "--hostname", "{{HOSTNAME}}",
      "--cores", "{{CORES}}",
      "--app-id", "{{APP_ID}}",
      "--worker-url", "{{WORKER_URL}}")
    //spark.executor.extraJavaOptions配置文件或者sparkConf配置 可以为空
    val extraJavaOpts = sc.conf.getOption("spark.executor.extraJavaOptions")
      .map(Utils.splitCommandString).getOrElse(Seq.empty)
    val classPathEntries = sc.conf.getOption("spark.executor.extraClassPath")
      .map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)
    val libraryPathEntries = sc.conf.getOption("spark.executor.extraLibraryPath")
      .map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)

    // When testing, expose the parent class path to the child. This is processed by
    // compute-classpath.{cmd,sh} and makes all needed jars available to child processes
    // when the assembly is built with the "*-provided" profiles enabled.
    val testingClassPath =
      if (sys.props.contains("spark.testing")) {
        sys.props("java.class.path").split(java.io.File.pathSeparator).toSeq
      } else {
        Nil
      }

    // Start executors with a few necessary configs for registering with the scheduler
    val sparkJavaOpts = Utils.sparkJavaOpts(conf, SparkConf.isExecutorStartupConf)
    val javaOpts = sparkJavaOpts ++ extraJavaOpts
    //启动进程的命令CoarseGrainedExecutorBackend
    val command = Command("org.apache.spark.executor.CoarseGrainedExecutorBackend",
      args, sc.executorEnvs, classPathEntries ++ testingClassPath, libraryPathEntries, javaOpts)
    //app的UI地址http://master:4040
    val appUIAddress = sc.ui.map(_.appUIAddress).getOrElse("")
    //每个executor的核数spark.executor.cores
    val coresPerExecutor = conf.getOption("spark.executor.cores").map(_.toInt)
    //应用的描述，很重要 代表整个应用的元数据信息
    //名称 最大核心数 executor内存,WORKER启动executor进程的命令  应用界面地址  时间通知目录  时间日志编码  每个executor的核心数量
    val appDesc = new ApplicationDescription(sc.appName, maxCores, sc.executorMemory,
      command, appUIAddress, sc.eventLogDir, sc.eventLogCodec, coresPerExecutor)
    //客户端
    client = new AppClient(sc.env.rpcEnv, masters, appDesc, this, conf)
    //启动应用客户端
    //初始化ClientEndpoint，这样client持有了一个ClientEndpoint，负责和外界的通信
    //在启动ClientEndpoint的时候向master注册app
    client.start()
    launcherBackend.setState(SparkAppHandle.State.SUBMITTED)
    //等待注册成功，会在这里阻塞知道调用registrationBarrier.release
    waitForRegistration()
    //设置运行状态
    launcherBackend.setState(SparkAppHandle.State.RUNNING)
  }

  override def stop(): Unit = synchronized {
    stop(SparkAppHandle.State.FINISHED)
  }

  override def connected(appId: String) {
    logInfo("Connected to Spark cluster with app ID " + appId)
    //设置appId appId是master负责生成的
    this.appId = appId
    //通知当前的注册线程可以继续往下了，解除注册设置的阻塞, registrationBarrier.release()
    notifyContext()
    //launcherBackend设置应用ID
    launcherBackend.setAppId(appId)
  }

  override def disconnected() {
    notifyContext()
    if (!stopping) {
      logWarning("Disconnected from Spark cluster! Waiting for reconnection...")
    }
  }

  override def dead(reason: String) {
    notifyContext()
    if (!stopping) {
      launcherBackend.setState(SparkAppHandle.State.KILLED)
      logError("Application has been killed. Reason: " + reason)
      try {
        scheduler.error(reason)
      } finally {
        // Ensure the application terminates, as we can no longer run jobs.
        sc.stop()
      }
    }
  }

  override def executorAdded(fullId: String, workerId: String, hostPort: String, cores: Int,
    memory: Int) {
    logInfo("Granted executor ID %s on hostPort %s with %d cores, %s RAM".format(
      fullId, hostPort, cores, Utils.megabytesToString(memory)))
  }

  override def executorRemoved(fullId: String, message: String, exitStatus: Option[Int]) {
    val reason: ExecutorLossReason = exitStatus match {
      case Some(code) => ExecutorExited(code, exitCausedByApp = true, message)
      case None => SlaveLost(message)
    }
    logInfo("Executor %s removed: %s".format(fullId, message))
    //移除executor 实际上是发送消息给driverEndpoint告诉移除executor的消息
    removeExecutor(fullId.split("/")(1), reason)
  }

  override def sufficientResourcesRegistered(): Boolean = {
    totalCoreCount.get() >= totalExpectedCores * minRegisteredRatio
  }

  override def applicationId(): String =
    Option(appId).getOrElse {
      logWarning("Application ID is not initialized yet.")
      super.applicationId
    }

  /**
   * Request executors from the Master by specifying the total number desired,
   * including existing pending and running executors.
   *
   * @return whether the request is acknowledged.
   */
  protected override def doRequestTotalExecutors(requestedTotal: Int): Boolean = {
    Option(client) match {
      case Some(c) => c.requestTotalExecutors(requestedTotal)
      case None =>
        logWarning("Attempted to request executors before driver fully initialized.")
        false
    }
  }

  /**
   * Kill the given list of executors through the Master.
   * @return whether the kill request is acknowledged.
   */
  protected override def doKillExecutors(executorIds: Seq[String]): Boolean = {
    Option(client) match {
      case Some(c) => c.killExecutors(executorIds)
      case None =>
        logWarning("Attempted to kill executors before driver fully initialized.")
        false
    }
  }

  private def waitForRegistration() = {
    registrationBarrier.acquire()
  }

  private def notifyContext() = {
    registrationBarrier.release()
  }

  private def stop(finalState: SparkAppHandle.State): Unit = synchronized {
    try {
      stopping = true

      super.stop()
      client.stop()

      val callback = shutdownCallback
      if (callback != null) {
        callback(this)
      }
    } finally {
      launcherBackend.setState(finalState)
      launcherBackend.close()
    }
  }

}
