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

import java.util.Comparator

import org.apache.spark.util.collection.WritablePartitionedPairCollection._

/**
 * Implementation of WritablePartitionedPairCollection that wraps a map in which the keys are tuples
 * of (partition ID, K)
  * key是(分区ID，具体的key值)
 */
private[spark] class PartitionedAppendOnlyMap[K, V]
  extends SizeTrackingAppendOnlyMap[(Int, K), V] with WritablePartitionedPairCollection[K, V] {

  def partitionedDestructiveSortedIterator(keyComparator: Option[Comparator[K]])
    : Iterator[((Int, K), V)] = {
    //keyComparator是对RDD计算记录的key得比较器 partitionKeyComparator组合了(分区ID,RDD计算结果的key)得比较器
    //partitionComparator 只对分区比较
    val comparator = keyComparator.map(partitionKeyComparator).getOrElse(partitionComparator)
    destructiveSortedIterator(comparator)
  }
//插入的时候修改key为(分区ID,key)
  def insert(partition: Int, key: K, value: V): Unit = {
    update((partition, key), value)
  }
}
