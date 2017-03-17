package ning.spark.suite

import java.io.{ObjectOutputStream, IOException}

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.control.NonFatal

/**
  * Created by ning on 2016/9/26.
  */
class CoGroupPartition(idx: Int, val narrowDeps: Array[Option[NarrowCoGroupSplitDep]])
  extends Partition with Serializable {
  override val index: Int = idx
  override def hashCode(): Int = idx
}
case class NarrowCoGroupSplitDep( @transient rdd: RDD[_],
                                  @transient splitIndex: Int,
                                  var split: Partition
                                ) extends Serializable {
  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = {
    // Update the reference to parent split at the time of task serialization
    try{
      split = rdd.partitions(splitIndex)
      oos.defaultWriteObject()
    }catch{
      case e: IOException => throw e
      case NonFatal(t) => throw new IOException(t)
    }

  }
}
class SubstractRdd[K:ClassTag,V:ClassTag,W:ClassTag](
                                                      @transient var rdd1: RDD[_ <: Product2[K,V]],
                                                      @transient var rdd2: RDD[_ <: Product2[K,W]],
                                                      part:Partitioner
                                                    )extends RDD[(K,V)](rdd1.context,Nil){
  private var serializer: Option[Serializer] = None
  def setSerializer(serializer: Serializer): SubstractRdd[K,V,W] ={
    this.serializer = Option(serializer)
    this
  }

  override def getDependencies: Seq[Dependency[_]] = {
    def rddDependcy[T1:ClassTag,T2:ClassTag](rdd:RDD[_ <: Product2[T1,T2]]): Dependency[_] ={
      if(rdd.partitioner ==Some(part)){
        new OneToOneDependency(rdd)
      }else{
        new ShuffleDependency[T1,T2,Any](rdd,part,serializer)
      }
    }
    Seq(rddDependcy[K,V](rdd1),rddDependcy[K,W](rdd2))
  }
  override val partitioner = Some(part)



  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](part.numPartitions)
    for(i <- 0 until array.length){
      array(i) = new CoGroupPartition(
        i,
        Seq(rdd1,rdd2).zipWithIndex.map({case (rdd,j) => {
          dependencies(j) match {
            case s:ShuffleDependency[_,_,_] => None
            case _ =>
              Some(new NarrowCoGroupSplitDep(rdd,j,rdd.partitions(j)))
          }
        }
        }).toArray
      )
    }
    array
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
  }
  override def compute(p: Partition, context: TaskContext): Iterator[(K, V)] = {
    import java.util.{HashMap => JHashMap}
    val partition = p.asInstanceOf[CoGroupPartition]
    val map = new JHashMap[K, ArrayBuffer[V]]
    def getSeq(k:K) :ArrayBuffer[V] = {
      val seq = map.get(k)
      if(seq != null){
        seq
      }else {
        val seq = new ArrayBuffer[V]()
        map.put(k,seq)
        seq
      }
    }
    def integrate(depNum:Int,op:Product2[K,V] => Unit) = {
      dependencies(depNum) match {
        case oneToOneDependency:OneToOneDependency[_] =>
          val dependencyPartition = partition.narrowDeps(depNum).get.split
          oneToOneDependency.rdd.iterator(dependencyPartition,context).asInstanceOf[Iterator[Product2[K,V]]].foreach(op)
        case suffleDependence:ShuffleDependency[_,_,_] =>
          val iter = SparkEnv.get.shuffleManager.getReader(suffleDependence.shuffleHandle,partition.index,partition.index+1,context)
            .read()
          iter.foreach(op)

      }
    }
    integrate(0,t => getSeq(t._1) += t._2)
    integrate(1,t => map.remove(t._1))
    import scala.collection.JavaConverters._
    map.asScala.iterator.map(t => {
      t._2.iterator.map(
        (t._1,_)
      )
    }).flatten
  }
}
