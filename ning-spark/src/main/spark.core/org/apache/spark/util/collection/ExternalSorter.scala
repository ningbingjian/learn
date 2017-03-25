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

import java.io._
import java.util.Comparator

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

import com.google.common.io.ByteStreams

import org.apache.spark._
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.serializer._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.storage.{BlockId, DiskBlockObjectWriter}

/**
 * Sorts and potentially merges a number of key-value pairs of type (K, V) to produce key-combiner
 * pairs of type (K, C). Uses a Partitioner to first group the keys into partitions, and then
 * optionally sorts keys within each partition using a custom Comparator. Can output a single
 * partitioned file with a different byte range for each partition, suitable for shuffle fetches.
 *
 * If combining is disabled, the type C must equal V -- we'll cast the objects at the end.
 *
 * Note: Although ExternalSorter is a fairly generic sorter, some of its configuration is tied
 * to its use in sort-based shuffle (for example, its block compression is controlled by
 * `spark.shuffle.compress`).  We may need to revisit this if ExternalSorter is used in other
 * non-shuffle contexts where we might want to use different configuration settings.
 *
 * @param aggregator optional Aggregator with combine functions to use for merging data
 * @param partitioner optional Partitioner; if given, sort by partition ID and then key
 * @param ordering optional Ordering to sort keys within each partition; should be a total ordering
 * @param serializer serializer to use when spilling to disk
 *
 * Note that if an Ordering is given, we'll always sort using it, so only provide it if you really
 * want the output keys to be sorted. In a map task without map-side combine for example, you
 * probably want to pass None as the ordering to avoid extra sorting. On the other hand, if you do
 * want to do combining, having an Ordering is more efficient than not having it.
 *
 * Users interact with this class in the following way:
 *
 * 1. Instantiate an ExternalSorter.
 *
 * 2. Call insertAll() with a set of records.
 *
 * 3. Request an iterator() back to traverse sorted/aggregated records.
 *     - or -
 *    Invoke writePartitionedFile() to create a file containing sorted/aggregated outputs
 *    that can be used in Spark's sort shuffle.
 *
 * At a high level, this class works internally as follows:
 *
 *  - We repeatedly fill up buffers of in-memory data, using either a PartitionedAppendOnlyMap if
 *    we want to combine by key, or a PartitionedPairBuffer if we don't.
 *    Inside these buffers, we sort elements by partition ID and then possibly also by key.
 *    To avoid calling the partitioner multiple times with each key, we store the partition ID
 *    alongside each record.
 *
 *  - When each buffer reaches our memory limit, we spill it to a file. This file is sorted first
 *    by partition ID and possibly second by key or by hash code of the key, if we want to do
 *    aggregation. For each file, we track how many objects were in each partition in memory, so we
 *    don't have to write out the partition ID for every element.
 *
 *  - When the user requests an iterator or file output, the spilled files are merged, along with
 *    any remaining in-memory data, using the same sort order defined above (unless both sorting
 *    and aggregation are disabled). If we need to aggregate by key, we either use a total ordering
 *    from the ordering parameter, or read the keys with the same hash code and compare them with
 *    each other for equality to merge values.
 *
 *  - Users are expected to call stop() at the end to delete all the intermediate files.
 */
private[spark] class ExternalSorter[K, V, C](
    context: TaskContext,
    aggregator: Option[Aggregator[K, V, C]] = None,
    partitioner: Option[Partitioner] = None,
    ordering: Option[Ordering[K]] = None,
    serializer: Option[Serializer] = None)
  extends Logging
  with Spillable[WritablePartitionedPairCollection[K, C]] {

  override protected[this] def taskMemoryManager: TaskMemoryManager = context.taskMemoryManager()

  private val conf = SparkEnv.get.conf

  private val numPartitions = partitioner.map(_.numPartitions).getOrElse(1)
  private val shouldPartition = numPartitions > 1
  private def getPartition(key: K): Int = {
    if (shouldPartition) partitioner.get.getPartition(key) else 0
  }

  private val blockManager = SparkEnv.get.blockManager
  private val diskBlockManager = blockManager.diskBlockManager
  private val ser = Serializer.getSerializer(serializer)
  private val serInstance = ser.newInstance()

  // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
  private val fileBufferSize = conf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024

  // Size of object batches when reading/writing from serializers.
  //
  // Objects are written in batches, with each batch using its own serialization stream. This
  // cuts down on the size of reference-tracking maps constructed when deserializing a stream.
  //
  // NOTE: Setting this too low can cause excessive copying when serializing, since some serializers
  // grow internal data structures by growing + copying every time the number of objects doubles.
  //读取和写出的批处理对象大小 对象是批处理写出的，每一个批处理都使用自己的序列化流。这个可以减少在反序列化流时构造map的空间要求减少
  //注意：设置太小在序列化的时候会引起过多的复制操作，因为某些序列化框架在对象翻倍的时候会通过增加或者拷贝来增加内部数据结构的空间
  private val serializerBatchSize = conf.getLong("spark.shuffle.spill.batchSize", 10000)

  // Data structures to store in-memory objects before we spill. Depending on whether we have an
  // Aggregator set, we either put objects into an AppendOnlyMap where we combine them, or we
  // store them in an array buffer.
  //保存记录  key value对 key 是key对应的分区的id,value是当前记录的key value值
  private var map = new PartitionedAppendOnlyMap[K, C]
  private var buffer = new PartitionedPairBuffer[K, C]

  // Total spilling statistics
  private var _diskBytesSpilled = 0L
  def diskBytesSpilled: Long = _diskBytesSpilled

  // Peak size of the in-memory data structure observed so far, in bytes
  private var _peakMemoryUsedBytes: Long = 0L
  def peakMemoryUsedBytes: Long = _peakMemoryUsedBytes

  // A comparator for keys K that orders them within a partition to allow aggregation or sorting.
  // Can be a partial ordering by hash code if a total ordering is not provided through by the
  // user. (A partial ordering means that equal keys have comparator.compare(k, k) = 0, but some
  // non-equal keys also have this, so we need to do a later pass to find truly equal keys).
  // Note that we ignore this if no aggregator and no ordering are given.
  //按照key得hash值排序
  private val keyComparator: Comparator[K] = ordering.getOrElse(new Comparator[K] {
    override def compare(a: K, b: K): Int = {
      val h1 = if (a == null) 0 else a.hashCode()
      val h2 = if (b == null) 0 else b.hashCode()
      if (h1 < h2) -1 else if (h1 == h2) 0 else 1
    }
  })

  private def comparator: Option[Comparator[K]] = {
    if (ordering.isDefined || aggregator.isDefined) {
      Some(keyComparator)
    } else {
      None
    }
  }

  // Information about a spilled file. Includes sizes in bytes of "batches" written by the
  // serializer as we periodically reset its stream, as well as number of elements in each
  // partition, used to efficiently keep track of partitions when merging.
  private[this] case class SpilledFile(
    file: File,
    blockId: BlockId,
    serializerBatchSizes: Array[Long],
    elementsPerPartition: Array[Long])

  private val spills = new ArrayBuffer[SpilledFile]

  /**
   * Number of files this sorter has spilled so far.
   * Exposed for testing.
   */
  private[spark] def numSpills: Int = spills.size
//记住：records得数据结构 K是rdd计算结果对应的key对应的分区ID,V是RDD计算结果的key value对
  def insertAll(records: Iterator[Product2[K, V]]): Unit = {
    // TODO: stop combining if we find that the reduction factor isn't high
    val shouldCombine = aggregator.isDefined

    if (shouldCombine) {//map端是否聚合 是的话先聚合再更新
      // Combine values in-memory first using our AppendOnlyMap
      val mergeValue = aggregator.get.mergeValue
      val createCombiner = aggregator.get.createCombiner
      var kv: Product2[K, V] = null
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
      }
      while (records.hasNext) {
        addElementsRead()//读取记录累加
        kv = records.next()//下一条记录
        map.changeValue((getPartition(kv._1), kv._1), update)//更新记录
        maybeSpillCollection(usingMap = true)//判断是否达到了spill出磁盘的条件
      }
    } else {
      // Stick values into our buffer
      while (records.hasNext) {
        addElementsRead()//读取记录累加
        val kv = records.next()//下一条记录
        buffer.insert(getPartition(kv._1), kv._1, kv._2.asInstanceOf[C])//插入记录
        maybeSpillCollection(usingMap = false)
      }
    }
  }

  /**
   * Spill the current in-memory collection to disk if needed.
   *usingMap用来做map端聚合的
   * @param usingMap whether we're using a map or buffer as our current in-memory collection
   */
  private def maybeSpillCollection(usingMap: Boolean): Unit = {
    var estimatedSize = 0L
    if (usingMap) {//估计map的占用空间
      estimatedSize = map.estimateSize()
      if (maybeSpill(map, estimatedSize)) {//spill到磁盘得判断
        map = new PartitionedAppendOnlyMap[K, C]//如果spill到磁盘 那么map重新new 一个实例
      }
    } else {
      estimatedSize = buffer.estimateSize()//估计占用空间
      if (maybeSpill(buffer, estimatedSize)) {//spill到磁盘得判断
        buffer = new PartitionedPairBuffer[K, C]//重新分配空间
      }
    }

    if (estimatedSize > _peakMemoryUsedBytes) {//目前为止内存数据结构占用大小的峰值
      _peakMemoryUsedBytes = estimatedSize
    }
  }

  /**
   * Spill our in-memory collection to a sorted file that we can merge later.
   * We add this file into `spilledFiles` to find it later.
   *溢出内存集合到一个排序文件中，之后可以合并
    * 添加这个文件到spillFiles，以便之后能找到
   * @param collection whichever collection we're using (map or buffer)
   */
  override protected[this] def spill(collection: WritablePartitionedPairCollection[K, C]): Unit = {
    // Because these files may be read during shuffle, their compression must be controlled by
    // spark.shuffle.compress instead of spark.shuffle.spill.compress, so we need to use
    // createTempShuffleBlock here; see SPARK-3426 for more context.
    //因为文件是 shuffle阶段读取的，他们的压缩方式由参数spark.shuffle.compress来控制，代替原来的参数spark.shuffle.spill.compress
    //所以这里必须使用createTempShuffleBlock方法
    val (blockId, file) = diskBlockManager.createTempShuffleBlock()//临时shuffle得block

    // These variables are reset after each flush 下面的变量会在flush方法之后重置
    var objectsWritten: Long = 0//写出对象数量
    var spillMetrics: ShuffleWriteMetrics = null//溢出度量
    var writer: DiskBlockObjectWriter = null//写出器=
    def openWriter(): Unit = {//打开writer
      assert (writer == null && spillMetrics == null)
      spillMetrics = new ShuffleWriteMetrics
      writer = blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, spillMetrics)
    }
    openWriter()

    // List of batch sizes (bytes) in the order they are written to disk
    val batchSizes = new ArrayBuffer[Long]//批量大小

    // How many elements we have in each partition
    val elementsPerPartition = new Array[Long](numPartitions)//每个分区的元素个数

    // Flush the disk writer's contents to disk, and update relevant variables.
    // The writer is closed at the end of this process, and cannot be reused.
    def flush(): Unit = {//刷新输出
      val w = writer
      writer = null
      w.commitAndClose()//提交并且关闭 这个方法关键
      _diskBytesSpilled += spillMetrics.shuffleBytesWritten//_diskBytesSpilled记录shuffle输出的大小
      batchSizes.append(spillMetrics.shuffleBytesWritten)
      spillMetrics = null
      objectsWritten = 0
    }

    var success = false
    try {
      //迭代器是经过排序地 排序的规则是按(分区ID,key)排序 ,返回的是key  value对
      val it = collection.destructiveSortedWritablePartitionedIterator(comparator)//经过排序的集合,按key排序 ,相同的key自然会分配到相同的分区
      while (it.hasNext) {//
        val partitionId = it.nextPartition()//分区ID
        require(partitionId >= 0 && partitionId < numPartitions,
          s"partition Id: ${partitionId} should be in the range [0, ${numPartitions})")
        it.writeNext(writer)//写出
        elementsPerPartition(partitionId) += 1//每个分区写出的数据量
        objectsWritten += 1//写出对象加1
        //serializerBatchSize是spill的记录数，参数spark.shuffle.spill.batchSize  10000
        if (objectsWritten == serializerBatchSize) {
          flush()//刷新
          openWriter()//打开新的writer
        }
      }
      if (objectsWritten > 0) {
        flush()
      } else if (writer != null) {
        val w = writer
        writer = null
        //还原并且关闭
        w.revertPartialWritesAndClose()
      }
      success = true//运行到这里表示成功
    } finally {
      if (!success) {//不成功必须做对应处理
        // This code path only happens if an exception was thrown above before we set success;
        // close our stuff and let the exception be thrown further
        if (writer != null) {//还原并且关闭
          writer.revertPartialWritesAndClose()
        }
        if (file.exists()) {//删除临时block文件
          if (!file.delete()) {
            logWarning(s"Error deleting ${file}")
          }
        }
      }
    }
  //溢出文件保存
    spills.append(SpilledFile(file, blockId, batchSizes.toArray, elementsPerPartition))
  }

  /**
   * Merge a sequence of sorted files, giving an iterator over partitions and then over elements
   * inside each partition. This can be used to either write out a new file or return data to
   * the user.
   *
   * Returns an iterator over all the data written to this object, grouped by partition. For each
   * partition we then have an iterator over its contents, and these are expected to be accessed
   * in order (you can't "skip ahead" to one partition without reading the previous one).
   * Guaranteed to return a key-value pair for each partition, in order of partition ID.
   */
  //合并溢出文件 spills是溢出文件 inMemory是内存数据 spills文件是key-value对 返回迭代器得类型是(key,value)
  private def merge(spills: Seq[SpilledFile], inMemory: Iterator[((Int, K), C)])
      : Iterator[(Int, Iterator[Product2[K, C]])] = {
    val readers = spills.map(new SpillReader(_))
    //readers 是个数组，每个reader表示一个文件的读取
    val inMemBuffered = inMemory.buffered//按分区处理 例如先处理0 1 2 3分区
    (0 until numPartitions).iterator.map { p =>
      val inMemIterator = new IteratorForPartition(p, inMemBuffered)//针对分区的内存迭代器
      //所有的reader每次读取一个分区
      val iterators = readers.map(_.readNextPartition()) ++ Seq(inMemIterator)//内存分区数据和磁盘文件分区数据合并到一起进行迭代
      if (aggregator.isDefined) {//是否需要聚合
        // Perform partial aggregation across partitions
        (p, mergeWithAggregation(
          iterators, aggregator.get.mergeCombiners, keyComparator, ordering.isDefined))//同一个分区的磁盘溢出文件和内存数据进行聚合
      } else if (ordering.isDefined) {//是否需要排序
        // No aggregator given, but we have an ordering (e.g. used by reduce tasks in sortByKey);k
        // sort the elements without trying to merge them
        (p, mergeSort(iterators, ordering.get))
      } else {//既不需要聚合也不需要排序
        (p, iterators.iterator.flatten)
      }
    }
  }

  /**
   * Merge-sort a sequence of (K, C) iterators using a given a comparator for the keys.
   */
  //根据给定的key的比较器进行比较排序,返回迭代器
  //对k-v迭代器列表进行归并排序 ，排序规则是comparator[K]
  private def mergeSort(iterators: Seq[Iterator[Product2[K, C]]], comparator: Comparator[K])
      : Iterator[Product2[K, C]] =
  {
    //将每一个元素转换为buffered iterator
    val bufferedIters = iterators.filter(_.hasNext).map(_.buffered)//转换成buffer迭代器
    type Iter = BufferedIterator[Product2[K, C]]
    //优先级队列
    val heap = new mutable.PriorityQueue[Iter]()(new Ordering[Iter] {
      // Use the reverse of comparator.compare because PriorityQueue dequeues the max
      override def compare(x: Iter, y: Iter): Int = -comparator.compare(x.head._1, y.head._1)
    })
    //插入所有元素
    heap.enqueue(bufferedIters: _*)  // Will contain only the iterators with hasNext = true
    new Iterator[Product2[K, C]] {
      override def hasNext: Boolean = !heap.isEmpty//不为空表示还有元素

      override def next(): Product2[K, C] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val firstBuf = heap.dequeue()//返回最高优先级的元素
        val firstPair = firstBuf.next()//下一个
        if (firstBuf.hasNext) {
          heap.enqueue(firstBuf)//如果还有元素 在插入队列
        }
        firstPair
      }
    }
  }

  /**
   * Merge a sequence of (K, C) iterators by aggregating values for each key, assuming that each
   * iterator is sorted by key with a given comparator. If the comparator is not a total ordering
   * (e.g. when we sort objects by hash code and different keys may compare as equal although
   * they're not), we still merge them by doing equality tests for all keys that compare as equal.
   */
  private def mergeWithAggregation(
      iterators: Seq[Iterator[Product2[K, C]]],//所有的迭代器
      mergeCombiners: (C, C) => C,//合并
      comparator: Comparator[K],//比较器
      totalOrder: Boolean)//全局排序
      : Iterator[Product2[K, C]] =
  {
    if (!totalOrder) {//没有全局排序 仅仅是分区内排序
      // We only have a partial ordering, e.g. comparing the keys by hash code, which means that
      // multiple distinct keys might be treated as equal by the ordering. To deal with this, we
      // need to read all keys considered equal by the ordering at once and compare them.
      new Iterator[Iterator[Product2[K, C]]] {//构造一个返回迭代器返回 iterators里的Iterator的元素都是经过排序的
        val sorted = mergeSort(iterators, comparator).buffered//合并排序

        // Buffers reused across elements to decrease memory allocation
        val keys = new ArrayBuffer[K]//所有key
        val combiners = new ArrayBuffer[C]//合并结果保存

        override def hasNext: Boolean = sorted.hasNext//判断是否有下一个

        override def next(): Iterator[Product2[K, C]] = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
          keys.clear()
          combiners.clear()
          val firstPair = sorted.next()
          keys += firstPair._1
          combiners += firstPair._2
          val key = firstPair._1//先排序，在聚合 只在分区内排序 当前遍历的都是同一个分区的数据
          while (sorted.hasNext && comparator.compare(sorted.head._1, key) == 0) {
            val pair = sorted.next()
            var i = 0
            var foundKey = false
            while (i < keys.size && !foundKey) {
              if (keys(i) == pair._1) {
                combiners(i) = mergeCombiners(combiners(i), pair._2)
                foundKey = true
              }
              i += 1
            }
            if (!foundKey) {
              keys += pair._1
              combiners += pair._2
            }
          }

          // Note that we return an iterator of elements since we could've had many keys marked
          // equal by the partial order; we flatten this below to get a flat iterator of (K, C).
          keys.iterator.zip(combiners.iterator)//合并两个iterator
        }
      }.flatMap(i => i)
    } else {//全局排序
      // We have a total ordering, so the objects with the same key are sequential.
      new Iterator[Product2[K, C]] {
        val sorted = mergeSort(iterators, comparator).buffered

        override def hasNext: Boolean = sorted.hasNext

        override def next(): Product2[K, C] = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
          val elem = sorted.next()
          val k = elem._1
          var c = elem._2
          while (sorted.hasNext && sorted.head._1 == k) {
            val pair = sorted.next()
            c = mergeCombiners(c, pair._2)
          }
          (k, c)
        }
      }
    }
  }

  /**
   * An internal class for reading a spilled file partition by partition. Expects all the
   * partitions to be requested in order.
   */
  private[this] class SpillReader(spill: SpilledFile) {
    // Serializer batch offsets; size will be batchSize.length + 1
    //所有溢出文件的总大小 batchOffsets保存了所有溢出文件每一次批处理之后，偏移的位置
    //例如:serializerBatchSizes=[2,4,8] batchOffsets=[0,2,6,14]
    val batchOffsets = spill.serializerBatchSizes.scanLeft(0L)(_ + _)

    // Track which partition and which batch stream we're in. These will be the indices of
    // the next element we will read. We'll also store the last partition read so that
    // readNextPartition() can figure out what partition that was from.
    var partitionId = 0
    var indexInPartition = 0L
    var batchId = 0
    var indexInBatch = 0
    var lastPartitionId = 0

    skipToNextPartition()

    // Intermediate file and deserializer streams that read from exactly one batch
    // This guards against pre-fetching and other arbitrary behavior of higher level streams
    var fileStream: FileInputStream = null
    var deserializeStream = nextBatchStream()  // Also sets fileStream

    var nextItem: (K, C) = null
    var finished = false

    /** Construct a stream that only reads from the next batch */
    def nextBatchStream(): DeserializationStream = {
      // Note that batchOffsets.length = numBatches + 1 since we did a scan above; check whether
      // we're still in a valid batch.
      //注意：batchOffsets得长度等于numBatches + 1，因此在扫描数据之前，检查是否是有效的batchId
      if (batchId < batchOffsets.length - 1) {
        if (deserializeStream != null) {
          deserializeStream.close()
          fileStream.close()
          deserializeStream = null
          fileStream = null
        }
        //读取本次读取的开始位置
        val start = batchOffsets(batchId)
        fileStream = new FileInputStream(spill.file)
        //从开始位置读取
        fileStream.getChannel.position(start)
        batchId += 1

        val end = batchOffsets(batchId)//结束位置

        assert(end >= start, "start = " + start + ", end = " + end +
          ", batchOffsets = " + batchOffsets.mkString("[", ", ", "]"))
        //反序列化流
        val bufferedStream = new BufferedInputStream(ByteStreams.limit(fileStream, end - start))
        val compressedStream = blockManager.wrapForCompression(spill.blockId, bufferedStream)
        serInstance.deserializeStream(compressedStream)
      } else {
        // No more batches left
        cleanup()
        null
      }
    }

    /**
     * Update partitionId if we have reached the end of our current partition, possibly skipping
     * empty partitions on the way.
     */
    /**
      * 如果已经读到当前分区的结尾，更新分区ID
      */
    private def skipToNextPartition() {
      while (partitionId < numPartitions &&
          indexInPartition == spill.elementsPerPartition(partitionId)) {
        partitionId += 1
        indexInPartition = 0L
      }
    }

    /**
     * Return the next (K, C) pair from the deserialization stream and update partitionId,
     * indexInPartition, indexInBatch and such to match its location.
     *
     * If the current batch is drained, construct a stream for the next batch and read from it.
     * If no more pairs are left, return null.
     */
    //按分区读取每一个分区的数据
    private def readNextItem(): (K, C) = {
      if (finished || deserializeStream == null) {//已经读完数据
        return null
      }
      val k = deserializeStream.readKey().asInstanceOf[K] //key类型
      val c = deserializeStream.readValue().asInstanceOf[C]//value类型
      lastPartitionId = partitionId//读取从0分区开始 记住当前数据读取的分区ID
      // Start reading the next batch if we're done with this one
      indexInBatch += 1//批量读取计数
      if (indexInBatch == serializerBatchSize) {//达到读取批处理大小
        indexInBatch = 0//重设计数器
        deserializeStream = nextBatchStream()//重新读取下一个批处理流
      }
      // Update the partition location of the element we're reading
      indexInPartition += 1//更新读取数据在分区位置
      skipToNextPartition()//判断是否读取完当前分区的数据，然后决定是否需要跳到下一个分区
      // If we've finished reading the last partition, remember that we're done
      if (partitionId == numPartitions) {//最后一个分区
        finished = true//完成
        if (deserializeStream != null) {
          deserializeStream.close()
        }
      }
      (k, c)
    }

    var nextPartitionToRead = 0
    //读取溢出文件中的分区数据，读取下一个分区数据，返回的是迭代器
    def readNextPartition(): Iterator[Product2[K, C]] = new Iterator[Product2[K, C]] {
      val myPartition = nextPartitionToRead//分区ID 默认从0开始
      nextPartitionToRead += 1//下一个分区

      override def hasNext: Boolean = {//是否有下一个元素
        if (nextItem == null) {//等于空
          nextItem = readNextItem()//读取下一个符合分区的数据元素 重要 需要和写出对比阅读代码
          if (nextItem == null) {//没有下一个元素表示分区读取完毕
            return false
          }
        }
        //lastPartitionId必须大于等于nextPartitionToRead
        assert(lastPartitionId >= myPartition)
        // Check that we're still in the right partition; note that readNextItem will have returned
        // null at EOF above so we would've returned false there
        //当前分区的数据是否读取完毕时根据最后一个元素从哪个分区读取和myPartition作比较来决定的
        //lastPartitionId记录了当前读取到的元素所在的分区ID
        //myPartition记录了当前应该正在读取的分区数据 如果两者不相等，说明已经读取到了下一个分区的数据，那么当前分区的数据已经读取完毕
        lastPartitionId == myPartition
      }

      override def next(): Product2[K, C] = {
        if (!hasNext) {//判断是否有下一个元素
          throw new NoSuchElementException
        }
        val item = nextItem//赋值给下一个元素
        nextItem = null
        item
      }
    }

    // Clean up our open streams and put us in a state where we can't read any more data
    def cleanup() {
      batchId = batchOffsets.length  // Prevent reading any other batch
      val ds = deserializeStream
      deserializeStream = null
      fileStream = null
      ds.close()
      // NOTE: We don't do file.delete() here because that is done in ExternalSorter.stop().
      // This should also be fixed in ExternalAppendOnlyMap.
    }
  }

  /**
   * Return an iterator over all the data written to this object, grouped by partition and
   * aggregated by the requested aggregator. For each partition we then have an iterator over its
   * contents, and these are expected to be accessed in order (you can't "skip ahead" to one
   * partition without reading the previous one). Guaranteed to return a key-value pair for each
   * partition, in order of partition ID.
   *
   * For now, we just merge all the spilled files in once pass, but this can be modified to
   * support hierarchical merging.
   * Exposed for testing.
   */
  def partitionedIterator: Iterator[(Int, Iterator[Product2[K, C]])] = {
    val usingMap = aggregator.isDefined
    val collection: WritablePartitionedPairCollection[K, C] = if (usingMap) map else buffer
    if (spills.isEmpty) {
      // Special case: if we have only in-memory data, we don't need to merge streams, and perhaps
      // we don't even need to sort by anything other than partition ID
      if (!ordering.isDefined) {//是否需要排序输出
        // The user hasn't requested sorted keys, so only sort by partition ID, not key
        groupByPartition(collection.partitionedDestructiveSortedIterator(None))
      } else {
        // We do need to sort by both partition ID and key 需要排序
        groupByPartition(collection.partitionedDestructiveSortedIterator(Some(keyComparator)))
      }
    } else {
      // Merge spilled and in-memory data
      //合并溢出文件和内存数据
      merge(spills, collection.partitionedDestructiveSortedIterator(comparator))
    }
  }

  /**
   * Return an iterator over all the data written to this object, aggregated by our aggregator.
   */
  def iterator: Iterator[Product2[K, C]] = partitionedIterator.flatMap(pair => pair._2)

  /**
   * Write all the data added into this ExternalSorter into a file in the disk store. This is
   * called by the SortShuffleWriter.
   *
   * @param blockId block ID to write to. The index file will be blockId.name + ".index".
   * @return array of lengths, in bytes, of each partition of the file (used by map output tracker)
   */
  def writePartitionedFile(
      blockId: BlockId,
      outputFile: File): Array[Long] = {

    // Track location of each range in the output file
    val lengths = new Array[Long](numPartitions)

    if (spills.isEmpty) {//如果没有溢出磁盘
      // Case where we only have in-memory data
      val collection = if (aggregator.isDefined) map else buffer //根据是否需要在map端聚合来决定使用map还是buffer数据结构

      val it = collection.destructiveSortedWritablePartitionedIterator(comparator)
      //经过排序的迭代器 同一个分区的数据都是在数据都会聚集到一起的
      //例如:底层都是用数组存储数据key和value相邻,分区的数据相邻例如 0-12得位置是分区1得数据 13到24位置是分区2的所有数据 以此类推
      while (it.hasNext) {
        //不同得分区写出的文件不同
        val writer = blockManager.getDiskWriter(blockId, outputFile, serInstance, fileBufferSize,
          context.taskMetrics.shuffleWriteMetrics.get)
        val partitionId = it.nextPartition()
        while (it.hasNext && it.nextPartition() == partitionId) {//同一个分区的数据写出到同一个文件得同一段
          it.writeNext(writer)
        }
        writer.commitAndClose()//提交分区的所有数据
        val segment = writer.fileSegment()//返回分区文件分片
        lengths(partitionId) = segment.length//记录分区的长度
      }
    } else {//如果已经有溢出磁盘的文件 必须惊醒合并排序
      // We must perform merge-sort; get an iterator by partition and write everything directly.
      for ((id, elements) <- this.partitionedIterator) {//遍历分区迭代器  当前内存的数据 在这里合并磁盘和内存的数据，并且聚合或者排序或者不聚合也不排序输出
        if (elements.hasNext) {//如果有下一个
          val writer = blockManager.getDiskWriter(blockId, outputFile, serInstance, fileBufferSize,
            context.taskMetrics.shuffleWriteMetrics.get)//记录度量信息
          for (elem <- elements) {//往外写出key value
            writer.write(elem._1, elem._2)//往外写出
          }
          writer.commitAndClose()
          val segment = writer.fileSegment()
          lengths(id) = segment.length
        }
      }
    }

    context.taskMetrics().incMemoryBytesSpilled(memoryBytesSpilled)
    context.taskMetrics().incDiskBytesSpilled(diskBytesSpilled)
    context.internalMetricsToAccumulators(
      InternalAccumulator.PEAK_EXECUTION_MEMORY).add(peakMemoryUsedBytes)

    lengths
  }

  def stop(): Unit = {
    map = null // So that the memory can be garbage-collected
    buffer = null // So that the memory can be garbage-collected
    spills.foreach(s => s.file.delete())
    spills.clear()
    releaseMemory()
  }

  /**
   * Given a stream of ((partition, key), combiner) pairs *assumed to be sorted by partition ID*,
   * group together the pairs for each partition into a sub-iterator.
   *
   * @param data an iterator of elements, assumed to already be sorted by partition ID
   */
  private def groupByPartition(data: Iterator[((Int, K), C)])
      : Iterator[(Int, Iterator[Product2[K, C]])] =
  {
    val buffered = data.buffered
    (0 until numPartitions).iterator.map(p => (p, new IteratorForPartition(p, buffered)))
  }

  /**
   * An iterator that reads only the elements for a given partition ID from an underlying buffered
   * stream, assuming this partition is the next one to be read. Used to make it easier to return
   * partitioned iterators from our in-memory collection.
   */
  private[this] class IteratorForPartition(partitionId: Int, data: BufferedIterator[((Int, K), C)])
    extends Iterator[Product2[K, C]]
  {
    override def hasNext: Boolean = data.hasNext && data.head._1._1 == partitionId

    override def next(): Product2[K, C] = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val elem = data.next()
      (elem._1._2, elem._2)
    }
  }
}
