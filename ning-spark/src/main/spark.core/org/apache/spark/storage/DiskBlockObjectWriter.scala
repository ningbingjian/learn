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

package org.apache.spark.storage

import java.io.{BufferedOutputStream, FileOutputStream, File, OutputStream}
import java.nio.channels.FileChannel

import org.apache.spark.Logging
import org.apache.spark.serializer.{SerializerInstance, SerializationStream}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.util.Utils

/**
 * A class for writing JVM objects directly to a file on disk. This class allows data to be appended
 * to an existing block and can guarantee atomicity in the case of faults as it allows the caller to
 * revert partial writes.
 *
 * This class does not support concurrent writes. Also, once the writer has been opened it cannot be
 * reopened again.
 */
/**
  * 写出对象到磁盘文件，允许追加到存在的数据块，可以保证在发生错误的情况下允许调用者恢复部分写入
  * @param file
  * @param serializerInstance
  * @param bufferSize
  * @param compressStream
  * @param syncWrites
  * @param writeMetrics
  * @param blockId
  */
private[spark] class DiskBlockObjectWriter(
    val file: File,
    serializerInstance: SerializerInstance,
    bufferSize: Int,
    compressStream: OutputStream => OutputStream,
    syncWrites: Boolean,
    // These write metrics concurrently shared with other active DiskBlockObjectWriters who
    // are themselves performing writes. All updates must be relative.
    writeMetrics: ShuffleWriteMetrics,
    val blockId: BlockId = null)
  extends OutputStream
  with Logging {

  /** The file channel, used for repositioning / truncating the file. */
  //用于重新定位和截断文件
  private var channel: FileChannel = null
  private var bs: OutputStream = null
  private var fos: FileOutputStream = null
  private var ts: TimeTrackingOutputStream = null
  private var objOut: SerializationStream = null
  private var initialized = false
  private var hasBeenClosed = false
  private var commitAndCloseHasBeenCalled = false

  /**
   * Cursors used to represent positions in the file. 用来表示文件位置的游标
   *
   * xxxxxxxx|--------|---       |
   *         ^        ^          ^
   *         |        |        finalPosition
   *         |      reportedPosition
   *       initialPosition
   *
   * initialPosition: Offset in the file where we start writing. Immutable. 开始写入文件时的偏移量
   * reportedPosition: Position at the time of the last update to the write metrics. 文件最后一次修改的时间的偏移量
   * finalPosition: Offset where we stopped writing. Set on closeAndCommit() then never changed. 停止入文件的偏移量。在closeAndCommit方法设置后就不会再被修改
   * -----: Current writes to the underlying file. 当前写入底层文件
   * xxxxx: Existing contents of the file.  存在的内容
    *
    *
    *
   */
  private val initialPosition = file.length()
  private var finalPosition: Long = -1
  private var reportedPosition = initialPosition

  /**
   * Keep track of number of records written and also use this to periodically
   * output bytes written since the latter is expensive to do for each record.
   */
  private var numRecordsWritten = 0

  def open(): DiskBlockObjectWriter = {
    if (hasBeenClosed) {//如果已经关闭了文件
      throw new IllegalStateException("Writer already closed. Cannot be reopened.")
    }
    fos = new FileOutputStream(file, true)//文件流 支持追加 true
    ts = new TimeTrackingOutputStream(writeMetrics, fos)//记录时间而已
    channel = fos.getChannel()//文件管道用来写入读取
    bs = compressStream(new BufferedOutputStream(ts, bufferSize))//封装压缩流
    objOut = serializerInstance.serializeStream(bs)//序列化流 封装了压缩流
    initialized = true
    this
  }

  override def close() {
    if (initialized) {
      Utils.tryWithSafeFinally {
        if (syncWrites) {
          // Force outstanding writes to disk and track how long it takes
          objOut.flush()
          val start = System.nanoTime()
          fos.getFD.sync()
          writeMetrics.incShuffleWriteTime(System.nanoTime() - start)
        }
      } {
        objOut.close()
      }

      channel = null
      bs = null
      fos = null
      ts = null
      objOut = null
      initialized = false
      hasBeenClosed = true
    }
  }

  def isOpen: Boolean = objOut != null

  /**
   * Flush the partial writes and commit them as a single atomic block.
   */
  def commitAndClose(): Unit = {
    if (initialized) {
      // NOTE: Because Kryo doesn't flush the underlying stream we explicitly flush both the
      //       serializer stream and the lower level stream.
      //因为Kryo框架不刷新底层数据流，必须显示手动的刷新序列化流和底层流
      objOut.flush()//刷新写出
      bs.flush()//刷新写出
      close()//关闭
      finalPosition = file.length()//记录最后操作的位置
      // In certain compression codecs, more bytes are written after close() is called
      writeMetrics.incShuffleBytesWritten(finalPosition - reportedPosition)
    } else {
      finalPosition = file.length()
    }
    commitAndCloseHasBeenCalled = true
  }


  /**
   * Reverts writes that haven't been flushed yet. Callers should invoke this function
   * when there are runtime exceptions. This method will not throw, though it may be
   * unsuccessful in truncating written data.
   *
   * @return the file that this DiskBlockObjectWriter wrote to.
   */
  def revertPartialWritesAndClose(): File = {
    // Discard current writes. We do this by flushing the outstanding writes and then
    // truncating the file to its initial position.
    try {
      if (initialized) {
        writeMetrics.decShuffleBytesWritten(reportedPosition - initialPosition)
        writeMetrics.decShuffleRecordsWritten(numRecordsWritten)
        objOut.flush()
        bs.flush()
        close()
      }

      val truncateStream = new FileOutputStream(file, true)
      try {
        truncateStream.getChannel.truncate(initialPosition)
        file
      } finally {
        truncateStream.close()
      }
    } catch {
      case e: Exception =>
        logError("Uncaught exception while reverting partial writes to file " + file, e)
        file
    }
  }

  /**
   * Writes a key-value pair.
   */
  def write(key: Any, value: Any) {
    if (!initialized) {
      open()
    }

    objOut.writeKey(key)
    objOut.writeValue(value)
    recordWritten()
  }

  override def write(b: Int): Unit = throw new UnsupportedOperationException()

  override def write(kvBytes: Array[Byte], offs: Int, len: Int): Unit = {
    if (!initialized) {//如果没有初始化
      open()
    }

    bs.write(kvBytes, offs, len)
  }

  /**
   * Notify the writer that a record worth of bytes has been written with OutputStream#write.
   */
  def recordWritten(): Unit = {
    numRecordsWritten += 1
    writeMetrics.incShuffleRecordsWritten(1)

    if (numRecordsWritten % 32 == 0) {
      updateBytesWritten()
    }
  }

  /**
   * Returns the file segment of committed data that this Writer has written.
   * This is only valid after commitAndClose() has been called.
   */
  def fileSegment(): FileSegment = {
    if (!commitAndCloseHasBeenCalled) {
      throw new IllegalStateException(
        "fileSegment() is only valid after commitAndClose() has been called")
    }
    //返回文件片段
    new FileSegment(file, initialPosition, finalPosition - initialPosition)
  }

  /**
   * Report the number of bytes written in this writer's shuffle write metrics.
   * Note that this is only valid before the underlying streams are closed.
   */
  private def updateBytesWritten() {
    val pos = channel.position()
    writeMetrics.incShuffleBytesWritten(pos - reportedPosition)
    reportedPosition = pos
  }

  // For testing
  private[spark] override def flush() {
    objOut.flush()
    bs.flush()
  }
}
