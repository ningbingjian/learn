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

package org.apache.spark.util.collection

import org.apache.spark.memory.{MemoryMode, TaskMemoryManager}
import org.apache.spark.{Logging, SparkEnv}

/**
 * Spills contents of an in-memory collection to disk when the memory threshold
 * has been exceeded.
 */
private[spark] trait Spillable[C] extends Logging {
  /**
   * Spills the current in-memory collection to disk, and releases the memory.
   *
   * @param collection collection to spill to disk
   */
  protected def spill(collection: C): Unit

  // Number of elements read from input since last spill
  protected def elementsRead: Long = _elementsRead

  // Called by subclasses every time a record is read
  // It's used for checking spilling frequency
  protected def addElementsRead(): Unit = { _elementsRead += 1 }

  // Memory manager that can be used to acquire/release memory
  protected[this] def taskMemoryManager: TaskMemoryManager

  // Initial threshold for the size of a collection before we start tracking its memory usage
  // For testing only
  //在开始追踪之内存使用之前的初始内存阀值，默认是5M
  private[this] val initialMemoryThreshold: Long =
    SparkEnv.get.conf.getLong("spark.shuffle.spill.initialMemoryThreshold", 5 * 1024 * 1024)

  // Force this collection to spill when there are this many elements in memory
  // For testing only
  private[this] val numElementsForceSpillThreshold: Long =
    SparkEnv.get.conf.getLong("spark.shuffle.spill.numElementsForceSpillThreshold", Long.MaxValue)

  // Threshold for this collection's size in bytes before we start tracking its memory usage
  // To avoid a large number of small spills, initialize this to a value orders of magnitude > 0
  //追踪内存使用之前的集合大小的阀值，为了避免大量的小数据溢出，初始值大于0
  private[this] var myMemoryThreshold = initialMemoryThreshold

  // Number of elements read from input since last spill
  private[this] var _elementsRead = 0L

  // Number of bytes spilled in total
  private[this] var _memoryBytesSpilled = 0L

  // Number of spills
  private[this] var _spillCount = 0

  /**
   * Spills the current in-memory collection to disk if needed. Attempts to acquire more
   * memory before spilling.
   *如果有必要将当前内存集合溢出到磁盘，溢出之前尝试申请更多的内存
   * @param collection collection to spill to disk
   * @param currentMemory estimated size of the collection in bytes
   * @return true if `collection` was spilled to disk; false otherwise
   */
  protected def maybeSpill(collection: C, currentMemory: Long): Boolean = {
    var shouldSpill = false //初始值 不溢出
    if (elementsRead % 32 == 0 && currentMemory >= myMemoryThreshold) {//如果记录读取数整除32,并且当前内存大于内存阀值 默认初始是5GB
      // Claim up to double our current memory from the shuffle memory pool //申请更多的内存，申请大小是:当前内存的2倍-内存阀值
      val amountToRequest = 2 * currentMemory - myMemoryThreshold
      //申请内存使用taskMemoryManager taskMemoryManager也很重要
      val granted =
        taskMemoryManager.acquireExecutionMemory(amountToRequest, MemoryMode.ON_HEAP, null)
      myMemoryThreshold += granted //阀值改变
      // If we were granted too little memory to grow further (either tryToAcquire returned 0,
      // or we already had more memory than myMemoryThreshold), spill the current collection
      //溢出的条件判断  当前占用内存已经超过溢出内存的定义阀值了 也就是说原来的阀值加上新申请的内存还没有当前占用的内存多的情况
      shouldSpill = currentMemory >= myMemoryThreshold
    }
    //判断是否需要溢出的条件 ：1、当前内存大于溢出内存的定义阀值 2、记录读取数,默认是long的最大值  所以判断是否需要溢出的关键条件还是内存
    shouldSpill = shouldSpill || _elementsRead > numElementsForceSpillThreshold
    // Actually spill
    if (shouldSpill) {
      _spillCount += 1//记录溢出磁盘的次数
      logSpillage(currentMemory)//记录溢出
      spill(collection)//执行溢出到磁盘
      _elementsRead = 0 //记录读取重置
      _memoryBytesSpilled += currentMemory//记录当前溢出磁盘达到的内存大小
      releaseMemory()
    }
    shouldSpill
  }

  /**
   * @return number of bytes spilled in total
   */
  def memoryBytesSpilled: Long = _memoryBytesSpilled

  /**
   * Release our memory back to the execution pool so that other tasks can grab it.
   */
  def releaseMemory(): Unit = {
    // The amount we requested does not include the initial memory tracking threshold
    taskMemoryManager.releaseExecutionMemory(
      myMemoryThreshold - initialMemoryThreshold, MemoryMode.ON_HEAP, null)
    myMemoryThreshold = initialMemoryThreshold
  }

  /**
   * Prints a standard log message detailing spillage.
   *
   * @param size number of bytes spilled
   */
  @inline private def logSpillage(size: Long) {
    val threadId = Thread.currentThread().getId
    logInfo("Thread %d spilling in-memory map of %s to disk (%d time%s so far)"
      .format(threadId, org.apache.spark.util.Utils.bytesToString(size),
        _spillCount, if (_spillCount > 1) "s" else ""))
  }
}
