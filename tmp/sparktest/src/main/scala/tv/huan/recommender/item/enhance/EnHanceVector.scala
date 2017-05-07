package tv.huan.recommender.item.enhance
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vectors, Vector => MLV}
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import org.apache.mahout.math.RandomAccessSparseVector
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.{CosineSimilarity, PearsonCorrelationSimilarity}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
/**
  * Created by zhaoshufen on 2017/5/1.
  */
object EnHanceVector extends Serializable{
  implicit def toEnhance(vector:SparseVector) = new EnHanceSparseVector(vector)
  def main(args: Array[String]): Unit = {
    val a1 = Array(1,2,3)
    val a2 = Array(4.0,5.0,6.0)
    println(a1.zip(a2).mkString("-"))
  }
}
object EnHanceVectors extends Serializable{
  def topK(vector:MLV,k:Int): MLV ={
    vector match {
      case v:DenseVector =>
          throw new RuntimeException("top k in denseVector is not support now !!")
      case v:SparseVector =>
        val (indices,values) = {
          val zipped = v.indices zip v.values
          zipped.sortBy(_._1).take(k).unzip
        }
        Vectors.sparse(Integer.MAX_VALUE,indices,values)

    }
  }
  def add(v1:MLV,v2:MLV):MLV ={
    (v1,v2) match{
      case (v1:SparseVector,v2:SparseVector) =>
        val zip1 = v1.indices zip v1.values
        val map1 = zip1.toMap

        val zip2 = v2.indices zip v2.values
        val map2 = zip2.toMap

        val resultMap = mutable.Map(map2.toSeq:_*)

        for((k,v) <- map1){
          val add = map2.getOrElse(k,0.0) + v
          resultMap(k) = add
        }
        val indices = ArrayBuffer[Int]()
        val values = ArrayBuffer[Double]()
        for((k,v) <- resultMap if v != 0.0){
          indices.append(k)
          values.append(v)
        }
        Vectors.sparse(indices.size,indices.toArray,values.toArray)
    }


  }
  def merge(vectors:MLV*): MLV = {
    val vector0 = vectors(0)
    vector0 match {
      case vector:DenseVector =>
        val len = vectors.map(_.size).sum
        val res = Vectors.zeros(len).asInstanceOf[DenseVector]
        val kvs = for{
          v <- vectors
          i <- 0 until v.size
        } yield (i,v(i))
        for(kv <- kvs){
          if(kv._2 != 0.0) {
            res.values(kv._1) = kv._2
          }
        }
        res
      case vector:SparseVector =>
        val (indicates,values) = vectors.flatMap{
          case v:SparseVector =>
            v.indices zip v.values
        }.sortBy(_._1).unzip
        Vectors.sparse(Integer.MAX_VALUE,indicates.toArray,values.toArray)
    }
  }
}
class EnHanceSparseVector(vector:SparseVector) extends Serializable{
  def normalize():MLV = {
    divide(Vectors.norm(vector,2))
  }
  def eachValue(f:(Double) => Double): Unit ={
    def change(arr:Array[Double]) = {
      for(i <- 0 until arr.length){
        arr(i) = f(arr(i))
      }
    }
    change(vector.values)
  }
  def divide(x:Double):MLV = {
    if(x == 1.0){
      return vector
    }
    eachValue(c => c / x)
    vector
  }
  def assign(f:(Double,Double )=>Double,y:Double): SparseVector ={
      for(i <- 0 until vector.values.length){
        vector.values(i) = f(vector.values(i),y)
      }
    vector
  }
  def assign(otherVector:SparseVector,f:(Double,Double )=>Double): SparseVector ={
    val zip1 = vector.indices zip vector.values
    val map1 = zip1.toMap

    val zip2 = otherVector.indices zip otherVector.values
    var map2 = zip2.toMap


    for((k,v) <- map1){
      val c = if(!map2.contains(k)) v else f(map2(k),v)
      map2 ++= Map(k -> c)
    }
    val indices = ArrayBuffer[Int]()
    val values = ArrayBuffer[Double]()
    for((k,v) <- map2){
      indices.append(k)
      values.append(v)
    }
    Vectors.sparse(indices.size,indices.toArray,values.toArray).asInstanceOf[SparseVector]
  }
}
