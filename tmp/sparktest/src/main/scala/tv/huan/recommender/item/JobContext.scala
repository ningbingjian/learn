package tv.huan.recommender.item

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scala.collection.mutable
/**
  * Created by zhaoshufen on 2017/5/1.
  */
class JobContext (val sc:SparkContext,val sqlContext:SQLContext){
  val param = mutable.Map[Any,Any]()
  def set(key:Any,value:Any)={
    param.put(key,value)
  }
  def getAs[V](key:Any):V={
    param(key).asInstanceOf[V]
  }
}

