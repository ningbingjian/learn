package tv.huan

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Hello world!
 *
 */
object App   {
  trait Key
  trait Value{
    def +(v:Value):Value
  }
  case class K1(id:Int) extends Key
  case class V1(id:Int) extends Value{
    def +(v:Value):V1 = {
      V1(id + v.asInstanceOf[V1].id)
    }
  }

  def main(args: Array[String]): Unit = {
    /*System.setProperty("hadoop.home","D:\\tool\\hadoop\\hadoop-2.7.1")*/
    val sparkConf = new SparkConf()
    /*sparkConf.setMaster("local[2]")*/
    sparkConf.setAppName("test")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.parallelize(Seq(
      (K1(2),V1(1)),
      (K1(2),V1(1)),
      (K1(1),V1(2)),
      (K1(1),V1(3)),
      (K1(1),V1(6)),
      (K1(1),V1(6))
    ))
    val rdd1 = rdd.asInstanceOf[RDD[(Key,Value)]]
    compute(rdd1)
  }
  def compute(rdd:RDD[(Key,Value)]): Unit ={
    val r = rdd.reduceByKey({
      case (v1:Value,v2:Value) => v1 + v2
    })
    r.collect().foreach(println)
  }
}
