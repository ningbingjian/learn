import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhaoshufen on 2017/3/12.
  * -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8888
  */
object T {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("T")
    val sc = new SparkContext()
    val rdd = sc.parallelize(Seq(("a",1),("a",2),("b",3)))
    rdd.reduceByKey(_ + _)
      .reduceByKey(_+_)
      .collect()
      .foreach(println)
  }
}
