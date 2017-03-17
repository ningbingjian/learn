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

import java.util.{Arrays, Comparator}

import com.google.common.hash.Hashing

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * A simple open hash table optimized for the append-only use case, where keys
 * are never removed, but the value for each key may be changed.
 *
 * This implementation uses quadratic probing with a power-of-2 hash table
 * size, which is guaranteed to explore all spaces for each key (see
 * http://en.wikipedia.org/wiki/Quadratic_probing).
 *
 * The map can support up to `375809638 (0.7 * 2 ^ 29)` elements.
 *
 * TODO: Cache the hash values of each key? java.util.HashMap does that.
 */
@DeveloperApi
class AppendOnlyMap[K, V](initialCapacity: Int = 64)
  extends Iterable[(K, V)] with Serializable {

  import AppendOnlyMap._

  require(initialCapacity <= MAXIMUM_CAPACITY,
    s"Can't make capacity bigger than ${MAXIMUM_CAPACITY} elements")
  require(initialCapacity >= 1, "Invalid initial capacity")

  private val LOAD_FACTOR = 0.7

  private var capacity = nextPowerOf2(initialCapacity)
  private var mask = capacity - 1
  private var curSize = 0
  private var growThreshold = (LOAD_FACTOR * capacity).toInt

  // Holds keys and values in the same array for memory locality; specifically, the order of
  // elements is key0, value0, key1, value1, key2, value2, etc.
  //重要 data的数据存储方式是:例如key是a value是1 那么data(0)就是a data(1)就是1 value连续存放，所以数组的元素个数总是偶数的
  private var data = new Array[AnyRef](2 * capacity)

  // Treat the null key differently so we can use nulls in "data" to represent empty items.
  //
  private var haveNullValue = false
  private var nullValue: V = null.asInstanceOf[V]//记录空值得key对应的值

  // Triggered by destructiveSortedIterator; the underlying data array may no longer be used
  private var destroyed = false
  private val destructionMessage = "Map state is invalid from destructive sorting!"

  /** Get the value for a given key */
  def apply(key: K): V = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) {
      return nullValue
    }
    var pos = rehash(k.hashCode) & mask
    var i = 1
    while (true) {
      val curKey = data(2 * pos)
      if (k.eq(curKey) || k.equals(curKey)) {
        return data(2 * pos + 1).asInstanceOf[V]
      } else if (curKey.eq(null)) {
        return null.asInstanceOf[V]
      } else {
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
    null.asInstanceOf[V]
  }

  /** Set the value for a key */
  def update(key: K, value: V): Unit = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) {//key是空的
      if (!haveNullValue) {//还没有空值
        incrementSize()//增加空间
      }
      nullValue = value//空值设置为value
      haveNullValue = true//设置tue 已经存在空的key==null值
      return
    }
    var pos = rehash(key.hashCode) & mask//根据key的hash值来寻找位置
    var i = 1//
    while (true) {
      val curKey = data(2 * pos)//位置默认在2*pos的位置
      if (curKey.eq(null)) {//如果当前是个空值证明还没有存在这个key
        data(2 * pos) = k//key
        data(2 * pos + 1) = value.asInstanceOf[AnyRef]//vlaue
        incrementSize()  // Since we added a new key //容量加1
        return
      } else if (k.eq(curKey) || k.equals(curKey)) {//如果不是空的,但是和当前的key一样 修改value
        data(2 * pos + 1) = value.asInstanceOf[AnyRef]//修改value
        return
      } else {//如果不是空的,但是和当前的key不一样 修改value，往下移动一个位置继续判断
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
  }

  /**
   * Set the value for key to updateFunc(hadValue, oldValue), where oldValue will be the old value
   * for key, if any, or null otherwise. Returns the newly updated value.
   */
  def changeValue(key: K, updateFunc: (Boolean, V) => V): V = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) {
      if (!haveNullValue) {
        incrementSize()
      }
      nullValue = updateFunc(haveNullValue, nullValue)
      haveNullValue = true
      return nullValue
    }
    var pos = rehash(k.hashCode) & mask
    var i = 1
    while (true) {
      val curKey = data(2 * pos)
      if (k.eq(curKey) || k.equals(curKey)) {
        val newValue = updateFunc(true, data(2 * pos + 1).asInstanceOf[V])
        data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
        return newValue
      } else if (curKey.eq(null)) {
        val newValue = updateFunc(false, null.asInstanceOf[V])
        data(2 * pos) = k
        data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
        incrementSize()
        return newValue
      } else {
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
    null.asInstanceOf[V] // Never reached but needed to keep compiler happy
  }

  /** Iterator method from Iterable */
  override def iterator: Iterator[(K, V)] = {
    assert(!destroyed, destructionMessage)
    new Iterator[(K, V)] {
      var pos = -1

      /** Get the next value we should return from next(), or null if we're finished iterating */
      def nextValue(): (K, V) = {
        if (pos == -1) {    // Treat position -1 as looking at the null value
          if (haveNullValue) {
            return (null.asInstanceOf[K], nullValue)
          }
          pos += 1
        }
        while (pos < capacity) {
          if (!data(2 * pos).eq(null)) {
            return (data(2 * pos).asInstanceOf[K], data(2 * pos + 1).asInstanceOf[V])
          }
          pos += 1
        }
        null
      }

      override def hasNext: Boolean = nextValue() != null

      override def next(): (K, V) = {
        val value = nextValue()
        if (value == null) {
          throw new NoSuchElementException("End of iterator")
        }
        pos += 1
        value
      }
    }
  }

  override def size: Int = curSize

  /** Increase table size by 1, rehashing if necessary */
  private def incrementSize() {
    curSize += 1
    if (curSize > growThreshold) {
      growTable()
    }
  }

  /**
   * Re-hash a value to deal better with hash functions that don't differ in the lower bits.
   */
  private def rehash(h: Int): Int = Hashing.murmur3_32().hashInt(h).asInt()

  /** Double the table's size and re-hash everything */
  protected def growTable() {
    // capacity < MAXIMUM_CAPACITY (2 ^ 29) so capacity * 2 won't overflow
    val newCapacity = capacity * 2
    require(newCapacity <= MAXIMUM_CAPACITY, s"Can't contain more than ${growThreshold} elements")
    val newData = new Array[AnyRef](2 * newCapacity)
    val newMask = newCapacity - 1
    // Insert all our old values into the new array. Note that because our old keys are
    // unique, there's no need to check for equality here when we insert.
    var oldPos = 0
    while (oldPos < capacity) {
      if (!data(2 * oldPos).eq(null)) {
        val key = data(2 * oldPos)
        val value = data(2 * oldPos + 1)
        var newPos = rehash(key.hashCode) & newMask
        var i = 1
        var keepGoing = true
        while (keepGoing) {
          val curKey = newData(2 * newPos)
          if (curKey.eq(null)) {
            newData(2 * newPos) = key
            newData(2 * newPos + 1) = value
            keepGoing = false
          } else {
            val delta = i
            newPos = (newPos + delta) & newMask
            i += 1
          }
        }
      }
      oldPos += 1
    }
    data = newData
    capacity = newCapacity
    mask = newMask
    growThreshold = (LOAD_FACTOR * newCapacity).toInt
  }

  private def nextPowerOf2(n: Int): Int = {
    val highBit = Integer.highestOneBit(n)
    if (highBit == n) n else highBit << 1
  }

  /**
   * Return an iterator of the map in sorted order. This provides a way to sort the map without
   * using additional memory, at the expense of destroying the validity of the map.
    * 基于分区ID的迭代器
    * 返回map得迭代器,迭代器是经过排序的，按key排序.排序的过程没有使用额外的内存，代价就是原来的map数据就失效了
   */
  def destructiveSortedIterator(keyComparator: Comparator[K]): Iterator[(K, V)] = {
    destroyed = true //是否摧毁了原来的数据
    // Pack KV pairs into the front of the underlying array
    var keyIndex, newIndex = 0
    while (keyIndex < capacity) {//keyIndex的索引小于容量
      if (data(2 * keyIndex) != null) {//2 * keyIndex不为空
        data(2 * newIndex) = data(2 * keyIndex)//2 * keyIndex赋值给2 * newIndex)
        data(2 * newIndex + 1) = data(2 * keyIndex + 1)//2 * keyIndex + 1 赋值给2 * newIndex + 1
        newIndex += 1//newIndex + 1
      }
      keyIndex += 1////keyIndex + 1
    }
    //目的是为了把数组的元素都提到数组前面来 注：由于data的存储格式之前没有了解 所以对这段代码误解了

    //判断游标的位置
    assert(curSize == newIndex + (if (haveNullValue) 1 else 0))
    //将数组进行排序
    new Sorter(new KVArraySortDataFormat[K, AnyRef]).sort(data, 0, newIndex, keyComparator)
  //构造迭代器返回
    new Iterator[(K, V)] {
      var i = 0
      var nullValueReady = haveNullValue //判断是存在空值
      def hasNext: Boolean = (i < newIndex || nullValueReady) //是否有下一个元素的判断条件:i<newIndex或则nullValueReady碰到了空值
      def next(): (K, V) = {
        if (nullValueReady) {//key为空的处理方式 nullValue记录了key为空得value
          nullValueReady = false
          (null.asInstanceOf[K], nullValue)
        } else {//返回key value
          val item = (data(2 * i).asInstanceOf[K], data(2 * i + 1).asInstanceOf[V])
          i += 1
          item
        }
      }
    }
  }

  /**
   * Return whether the next insert will cause the map to grow
   */
  def atGrowThreshold: Boolean = curSize == growThreshold
}

private object AppendOnlyMap {
  val MAXIMUM_CAPACITY = (1 << 29)
}
