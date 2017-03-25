package ning.spark.suite

import java.io.{ObjectOutputStream, IOException}
import java.util
import java.util.HashMap
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import org.apache.spark
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDDSuiteUtils.{CustomRdd}
import org.apache.spark.rdd._
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.ExternalAppendOnlyMap

import scala.collection.immutable.HashMap
import scala.collection.immutable.HashMap
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.reflect.ClassTag
import scala.util.control.NonFatal


/**
  * Created by ning on 2016/9/18.
  */
class RDDSuite extends SparkFunSuite with SharedSparkContext{
  test("basic operation"){
    //makeRDD创建RDD
    val nums = sc.makeRDD(Array(1, 2, 3, 4),2)
    //获取分区数量
    assert(nums.getNumPartitions === 2)
    //Return an array that contains all of the elements in this RDD.
    //返回RDD包含的所有数据的数组
    assert(nums.collect.toList === List(1,2,3,4))

    val dups = sc.makeRDD(Array(1, 1, 2, 2, 3, 3, 4, 4), 2)
    //去重计数
    assert(dups.distinct().count() === 4)
    //去重计数
    assert(dups.distinct().count === 4)

    assert(dups.distinct().collect === dups.distinct().collect)
    //指定计算分片
    assert(dups.distinct(2).collect === dups.distinct().collect)

    //累加
    assert(nums.reduce(_ + _) === 10)

    //初始值是0  累加
    assert(10 === nums.fold(0)(_+_))
    //
    assert(nums.map(_.toString).collect().toList === List("1", "2", "3", "4"))

    //过滤大于2的数值
    assert(nums.filter(_ > 2).collect().toList === List(3, 4))
    //抹平打散  只打散到第一层
    assert(nums.flatMap(x => 1 to x).collect().toList === List(1, 1, 2, 1, 2, 3, 1, 2, 3, 4))
    //合并追加
    assert(nums.union(nums).collect().toList === List(1, 2, 3, 4, 1, 2, 3, 4))
    //glom将分区的数据作为数组返回
    assert(nums.glom().map(_.toList).collect().toList === List(List(1, 2), List(3, 4)))
    //
    assert(nums.collect({ case i if i >= 3 => i.toString }).collect().toList === List("3", "4"))
    //添加key,和原来的数据组成key-value
    assert(nums.keyBy(_.toString).collect().toList === List(("1", 1), ("2", 2), ("3", 3), ("4", 4)))
    //判断空
    assert(!nums.isEmpty())
    //最大值
    assert(nums.max() === 4)
    //最小值
    assert(nums.min() === 1)
    //单独操作每一个分区的内容,输入参数是分区数据组成的迭代器,输出也是一个迭代器，每一个分区内的数据求和
    val partitionSums = nums.mapPartitions(iter => Iterator(iter.reduceLeft(_ + _)))
    //
    assert(partitionSums.collect().toList === List(3, 7))
    //带分区id的求和操作
    val partitionSumsWithSplit = nums.mapPartitionsWithIndex {
      case(split, iter) => Iterator((split, iter.reduceLeft(_ + _)))
    }
    assert(partitionSumsWithSplit.collect().toList === List((0, 3), (1, 7)))

    //如果没有数据就不会产生job RDD为空的时候会抛出异常 还真是奇怪，为什么要抛出异常呢  返回空不可以？
    nums.filter(_ > 5).reduce(_ + _)

   /* intercept[UnsupportedOperationException] {
      nums.filter(_ > 5).reduce(_ + _)
    }*/
  }
  test("serialization") {
    class EmptyRDD[T: ClassTag](sc: SparkContext) extends RDD[T](sc, Nil) {

      override def getPartitions: Array[Partition] = Array.empty

      override def compute(split: Partition, context: TaskContext): Iterator[T] = {
        throw new UnsupportedOperationException("empty RDD")
      }
    }
    val empty = new EmptyRDD(sc)
  }
  test("union"){
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    //合并单个RDD
    assert(sc.union(nums).collect().toList === List(1,2,3,4))
    //合并多个RDD
    assert(sc.union(nums,nums).collect().toList == List(1,2,3,4,1,2,3,4))
    //合并RDD集合
    assert(sc.union(Seq(nums)).collect.toList === List(1,2,3,4))
    //合并RDD集合
    assert(sc.union(Seq(nums,nums)) ===  List(1,2,3,4,1,2,3,4))

  }

  test("如果至少一个RDD没有指定使用分区partitioner,那么sc.union返回的是UnionRDD"){
    val rddWithPartitioner = sc.parallelize(Seq(1->true)).partitionBy(new HashPartitioner(1))
    val rddWithNoPartitioner  = sc.parallelize(Seq(2->true))
    val unionRdd = sc.union(rddWithPartitioner,rddWithNoPartitioner)
    assert(unionRdd.isInstanceOf[UnionRDD[_]])
  }
  test("如果所有的RDD都指定了分区函数,那么sc.union返回的是PartitionAwareUnionRDD"){
    val rddWithPartitioner = sc.parallelize(Seq(1->true)).partitionBy(new HashPartitioner(1))
    val unionRdd = sc.union(rddWithPartitioner,rddWithPartitioner)
    //assert(unionRdd.isInstanceOf[PartitionerAwareUnionRDD[_]])
  }

  test("unionRdd序列化后的数据应该很小 【应该理解为平均分布到各个分区,合并后的分区数量是累加的】"){
    val largeVariable = new Array[Byte](1000 * 1000)
    val rdd1 = sc.parallelize(1 to 10, 2).map(i => largeVariable.length)
    val rdd2 = sc.parallelize(1 to 10, 3)

    val ser = SparkEnv.get.closureSerializer.newInstance()
    val union = rdd1.union(rdd2)
    // The UnionRDD itself should be large, but each individual partition should be small.
    assert(ser.serialize(union).limit() > 2000)
    assert(union.partitions.size === 5)
    assert(ser.serialize(union.partitions.head).limit() < 2000)
  }
  test("聚合操作 aggregate"){
    val pairs = sc.makeRDD(Array(("a", 1), ("b", 2), ("a", 2), ("c", 5), ("a", 3)))
    type StringMap = scala.collection.mutable.HashMap[String,Int]
    val emptyMap = new StringMap{
      override  def default(key:String):Int = 0
    }
    /**
      * mergeElements变量，是一个函数的引用变量
      *(StringMap,(String,Int)) => StringMap 是一个函数的定义,传入2个参数的类型 ：StringMap和(String,Int),返回参数是StringMap
      * 这是代码体
      * (map,pair) =>{
      * map(pair._1) += pair._2
      * map
      * }
      *
      *
      * mergeMap变量，是一个函数的引用变量
      *(StringMap,StringMap) => StringMap 是函数的结构定义,表示需要输入两个参数,参数类型是StringMap,返回类型是StringMap
      * 函数的实现代码体是
      * (map1,map2) =>{
      *
      * for((k,v) <- map1){
      * map2(k) += v
      * }
      * map2
      * }
      */
    val  mergeElements :(StringMap,(String,Int)) => StringMap = (map,pair) => {
      val v = map.getOrElse(pair._1,0)
      map(pair._1) = v + pair._2
      map
    }
    val mergeMap :(StringMap,StringMap) => StringMap = (map1,map2) =>{
      for((k,v) <- map1){
        val v2 = map2.getOrElse(k,0)
        map2(k) = v2 + v
      }
      map2
    }
    //聚合操作 第一个参数是初始值,第二个参数描述怎么处理每一个分区的数据,第二个参数描述怎么处理分区之间的函数聚合
    //aggregate定义如下:
    //U是泛型
    //zeroValue是初始值，类型是U
    //seqOp: (U, T) => U  第二个参数是一个函数描述,需要两个输入参数 (U,T) U是初始值定义的类型,T是RDD代表的数据的类型,最后返回值也是U。
    //combOp: (U, U) => U,第三个参数是一个函数描述,需要两个输入参数,类型都是U，返回也是U
    //def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U
    val result = pairs.aggregate(emptyMap)(mergeElements, mergeMap)

    pairs.reduceByKey(_+_).saveAsTextFile("")
    println(result.toSet)
   }
  test("自定义RDD函数"){
    import org.apache.spark.rdd.RDDSuiteUtils.CustomRddFunc._
    val rdd = sc.makeRDD(Seq((1,"u1"),(2,"u2")))
    val resutlRdd = rdd.map(e=>(Student(e._1,e._2))).addAge(1)
    println(resutlRdd.collect().toList)
  }
  test("自定义RDD"){
    val rdd = sc.makeRDD(Seq((1,"u1"),(1,"u2")))
    val srcRdd = rdd.map(e=>(Student(e._1,e._2)))
    val stuRDD = new CustomRdd(100,srcRdd)
    println(stuRDD.collect().toList)
  }
  test("模拟reduceByKey"){
   class PairRdd[K, V](data:Seq[(K,V)])(implicit kt: ClassTag[K], vt: ClassTag[V]){
     def combineByKey[C](createCombiner:V =>C,mergeValue:(C,V) =>C)(implicit ct: ClassTag[C]):Iterator[(K,C)]={
       val map = scala.collection.mutable.HashMap[K,C]()
       val iterator = data.iterator
       var curEntry: Product2[K, V] = null
       while(iterator.hasNext){
         curEntry = iterator.next()
         if(map.get(curEntry._1) == None){
           map(curEntry._1) = createCombiner(curEntry._2)
         }else{
           map(curEntry._1) = mergeValue( map(curEntry._1),curEntry._2)
         }
       }
       map.toIterator
     }
     def reduceByKey(func: (V, V) => V):Iterator[(K,V)] ={
      combineByKey((v: V) => v,func)
     }
   }
    val data = Seq(("a",1),("b",1),("a",2))
    val p = new PairRdd[String,Int](data)
    val result = p.reduceByKey(_+_)
    println(result.toList)
  }
  test("test cogroup"){
    val data1 = Seq(("a",1),("b",1),("a",2))
    val data2 = Seq(("a",3),("b",4),("d",5))
    val rdd1 = sc.makeRDD(data1)
    val rdd2 = sc.makeRDD(data2)
    val rdd = rdd1.cogroup(rdd2)
    println(rdd.collect().toList)
  }
  test("test default parititioner"){
    val data1 = Seq(("a",1),("b",1),("a",2))
    val rdd1 = sc.makeRDD(data1)
    println(rdd1.partitioner)
  }
  test("test 1"){
    val data1 = Seq(("a",1),("b",1),("a",2))
    val rdd1 = sc.makeRDD(data1)
    val rdd2 = rdd1.reduceByKey(_+_)
    println(rdd1.partitioner.getOrElse(null))
    println(rdd2.partitioner.getOrElse(null))
  }


  test("RDD相减"){
    val data1 = Seq(("a",1),("b",1),("a",2),("c",3))
    val data2 = Seq(("a",1),("b",1),("d",5))
    val rdd1 = sc.makeRDD(data1,3).map((_,null))
    val rdd2 = sc.makeRDD(data2,2).map((_,null))
    println(rdd1.subtract(rdd2).collect().toList)
    val rdd4 = new SubstractRdd[(String,Int),Null,Null](rdd1,rdd2,rdd1.partitioner.getOrElse(new HashPartitioner(rdd1.partitions.length)))
    assert(rdd4.collect().toList === rdd1.subtract(rdd2).collect().toList)
  }
  test("RDD"){
    val rdd1 = sc.textFile("D:\\test\\111111111\\*")
      .flatMap(line => line.split(" " +
        ""))
      .map((_,1))
      //.reduceByKey(_+_)
      val rdd2 = rdd1.combineByKey((a:Int)=>a,(a:Int,b:Int) => a + b,(a:Int,b:Int) =>a + b,new HashPartitioner(rdd1.getNumPartitions),false)
      .map((_,1))
      .reduceByKey(_+_)
      .map((_,1))
      .reduceByKey(_+_)
      .collect()
      .toList
  /*  sc.setCheckpointDir("D:/tmp/1")
    val rdd = sc.textFile("D:\\test\\111111111\\111111111.log")
      .flatMap(line => line.split(" +"))
    rdd.checkpoint()
    rdd.map((_,1))
      .reduceByKey(_+_).collect*/

  }
}
