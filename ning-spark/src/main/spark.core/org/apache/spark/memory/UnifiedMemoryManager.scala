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

package org.apache.spark.memory

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.storage.{BlockStatus, BlockId}

/**UnifiedMemoryManager已经成为spark默认的内存管理器
  *内存管理器,在执行内存和存储内存之间设置的软边界，这样两者可以互相共用[借用]内存来使用。
  *执行和存储的共享区域是通过配置spark.memory.fraction来配置的,默认是0.75.也就是 两者共用可用内存*0.75的内存大小
  * 边界的位置在内存空间中是通过spark.memory.storageFraction配置的也就是说存储区域得内存大小是0.75*0.5=0.375,也就是占用可用内存的0.375
  *存储可以借用足够多的内存，直到执行期间请求更多的内存为止。
  *当执行期间申请更多的内存的时候，如果内存不充足，缓存块将会从内存中被删除，直到借用的内存满足执行期间需要的内存请求。
  *类似，执行期间的内存也可以借用给存储功能的内存，尽管这样，执行内存是从来都不会被移除，因为实现这个功能太复杂。
  *这意味着如果执行期间使用了太多的内存，存储模块得不到足够的内存，那么缓存块就会失效，这样会导致新的块被迅速移除，移除是根据各自的存储等级来移除的。
  *
  *默认是需要保留300M的可用堆内存出来的，例如现在又1024M的内存，那么存储和执行任务可使用的内存是(1024-300)*0.75=543M
  * 默认对于申请的内存必须大于300*1.5=450M，也就是说启动一个executor，内存要求必须大于450M
  *
 * A [[MemoryManager]] that enforces a soft boundary between execution and storage such that
 * either side can borrow memory from the other.
 *
 * The region shared between execution and storage is a fraction of (the total heap space - 300MB)
 * configurable through `spark.memory.fraction` (default 0.75). The position of the boundary
 * within this space is further determined by `spark.memory.storageFraction` (default 0.5).
 * This means the size of the storage region is 0.75 * 0.5 = 0.375 of the heap space by default.
 *
 * Storage can borrow as much execution memory as is free until execution reclaims its space.
 * When this happens, cached blocks will be evicted from memory until sufficient borrowed
 * memory is released to satisfy the execution memory request.
 *
 * Similarly, execution can borrow as much storage memory as is free. However, execution
 * memory is *never* evicted by storage due to the complexities involved in implementing this.
 * The implication is that attempts to cache blocks may fail if execution has already eaten
 * up most of the storage space, in which case the new blocks will be evicted immediately
 * according to their respective storage levels.
 *
 * @param storageRegionSize Size of the storage region, in bytes.
 *                          This region is not statically reserved; execution can borrow from
 *                          it if necessary. Cached blocks can be evicted only if actual
 *                          storage memory usage exceeds this region.
 */
private[spark] class UnifiedMemoryManager private[memory] (
    conf: SparkConf,
    val maxMemory: Long,
    storageRegionSize: Long,
    numCores: Int)
  extends MemoryManager(
    conf,
    numCores,
    storageRegionSize,
    maxMemory - storageRegionSize) {

  // We always maintain this invariant:
  assert(onHeapExecutionMemoryPool.poolSize + storageMemoryPool.poolSize == maxMemory)

  override def maxStorageMemory: Long = synchronized {
    maxMemory - onHeapExecutionMemoryPool.memoryUsed
  }

  /**
   * Try to acquire up to `numBytes` of execution memory for the current task and return the
   * number of bytes obtained, or 0 if none can be allocated.
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
   */
  override private[memory] def acquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Long = synchronized {
    assert(onHeapExecutionMemoryPool.poolSize + storageMemoryPool.poolSize == maxMemory)
    assert(numBytes >= 0)
    memoryMode match {
      case MemoryMode.ON_HEAP =>

        /**
         * Grow the execution pool by evicting cached blocks, thereby shrinking the storage pool.
         *
         * When acquiring memory for a task, the execution pool may need to make multiple
         * attempts. Each attempt must be able to evict storage in case another task jumps in
         * and caches a large block between the attempts. This is called once per attempt.
         */
        def maybeGrowExecutionPool(extraMemoryNeeded: Long): Unit = {
          if (extraMemoryNeeded > 0) {
            // There is not enough free memory in the execution pool, so try to reclaim memory from
            // storage. We can reclaim any free memory from the storage pool. If the storage pool
            // has grown to become larger than `storageRegionSize`, we can evict blocks and reclaim
            // the memory that storage has borrowed from execution.
            val memoryReclaimableFromStorage =
              math.max(storageMemoryPool.memoryFree, storageMemoryPool.poolSize - storageRegionSize)
            if (memoryReclaimableFromStorage > 0) {
              // Only reclaim as much space as is necessary and available:
              val spaceReclaimed = storageMemoryPool.shrinkPoolToFreeSpace(
                math.min(extraMemoryNeeded, memoryReclaimableFromStorage))
              onHeapExecutionMemoryPool.incrementPoolSize(spaceReclaimed)
            }
          }
        }

        /**
         * The size the execution pool would have after evicting storage memory.
         *
         * The execution memory pool divides this quantity among the active tasks evenly to cap
         * the execution memory allocation for each task. It is important to keep this greater
         * than the execution pool size, which doesn't take into account potential memory that
         * could be freed by evicting storage. Otherwise we may hit SPARK-12155.
         *
         * Additionally, this quantity should be kept below `maxMemory` to arbitrate fairness
         * in execution memory allocation across tasks, Otherwise, a task may occupy more than
         * its fair share of execution memory, mistakenly thinking that other tasks can acquire
         * the portion of storage memory that cannot be evicted.
         */
        def computeMaxExecutionPoolSize(): Long = {
          maxMemory - math.min(storageMemoryUsed, storageRegionSize)
        }

        onHeapExecutionMemoryPool.acquireMemory(
          numBytes, taskAttemptId, maybeGrowExecutionPool, computeMaxExecutionPoolSize)

      case MemoryMode.OFF_HEAP =>
        // For now, we only support on-heap caching of data, so we do not need to interact with
        // the storage pool when allocating off-heap memory. This will change in the future, though.
        offHeapExecutionMemoryPool.acquireMemory(numBytes, taskAttemptId)
    }
  }

  override def acquireStorageMemory(
      blockId: BlockId,
      numBytes: Long,//申请的内存大小
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Boolean = synchronized {
    assert(onHeapExecutionMemoryPool.poolSize + storageMemoryPool.poolSize == maxMemory)
    assert(numBytes >= 0)
    if (numBytes > maxStorageMemory) { //请求大小不能大于规定的存储内存最大值
      // Fail fast if the block simply won't fit
      logInfo(s"Will not store $blockId as the required space ($numBytes bytes) exceeds our " +
        s"memory limit ($maxStorageMemory bytes)")
      return false
    }
    if (numBytes > storageMemoryPool.memoryFree) {//请求内存大小大于目前剩余内存 需要向执行期间内存借用内存
      // There is not enough free memory in the storage pool, so try to borrow free memory from
      // the execution pool.
      val memoryBorrowedFromExecution = Math.min(onHeapExecutionMemoryPool.memoryFree, numBytes) //计算借用内存大小
      onHeapExecutionMemoryPool.decrementPoolSize(memoryBorrowedFromExecution)//执行内存削减内存
      storageMemoryPool.incrementPoolSize(memoryBorrowedFromExecution)//存储内存增加
    }
    storageMemoryPool.acquireMemory(blockId, numBytes, evictedBlocks) //申请内存
  }

  override def acquireUnrollMemory(
      blockId: BlockId,
      numBytes: Long,
      evictedBlocks: mutable.Buffer[(BlockId, BlockStatus)]): Boolean = synchronized {
    acquireStorageMemory(blockId, numBytes, evictedBlocks)
  }
}

object UnifiedMemoryManager {

  // Set aside a fixed amount of memory for non-storage, non-execution purposes.
  // This serves a function similar to `spark.memory.fraction`, but guarantees that we reserve
  // sufficient memory for the system even for small heaps. E.g. if we have a 1GB JVM, then
  // the memory used for execution and storage will be (1024 - 300) * 0.75 = 543MB by default.
  private val RESERVED_SYSTEM_MEMORY_BYTES = 300 * 1024 * 1024

  def apply(conf: SparkConf, numCores: Int): UnifiedMemoryManager = {
    val maxMemory = getMaxMemory(conf)
    new UnifiedMemoryManager(
      conf,
      maxMemory = maxMemory,
      storageRegionSize =
        (maxMemory * conf.getDouble("spark.memory.storageFraction", 0.5)).toLong,
      numCores = numCores)
  }

  /**
   * Return the total amount of memory shared between execution and storage, in bytes.
   */
  private def getMaxMemory(conf: SparkConf): Long = {
    //系统内存 堆内存大小
    val systemMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)
    //保留内存 默认300M 也就是默认必须保留300M内存，剩下的内存才是执行期间和存储使用的内存
    val reservedMemory = conf.getLong("spark.testing.reservedMemory",
      if (conf.contains("spark.testing")) 0 else RESERVED_SYSTEM_MEMORY_BYTES)
    //对内存得最小要求:300*1.5=450M
    val minSystemMemory = reservedMemory * 1.5
    if (systemMemory < minSystemMemory) {
      throw new IllegalArgumentException(s"System memory $systemMemory must " +
        s"be at least $minSystemMemory. Please use a larger heap size.")
    }
    //可使用得内存
    val usableMemory = systemMemory - reservedMemory
    //内存占比 spark.memory.fraction参数决定，默认是0.75
    val memoryFraction = conf.getDouble("spark.memory.fraction", 0.75)
    //执行期间和存储可使用的最大内存
    (usableMemory * memoryFraction).toLong
    //也就是说假如申请了1024M得内存，那么只有(1024M-300M)*0.75=543MB
  }
}
