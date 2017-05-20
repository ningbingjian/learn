package tv.huan

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhaoshufen on 2017/5/4.
  */
object App2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","D:\\tool\\hadoop\\hadoop-2.7.1")
    val conf = new SparkConf().setAppName("rdd_test02")
    conf.setMaster("local[2]")
    Logger.getRootLogger.setLevel(Level.ERROR)
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    val rdd1 = sc.parallelize(
      Seq(
        ("u1",1)
      )
    )
    val rdd2 = sc.parallelize(
      Seq(
        ("u1",11),
        ("",12),
        ("",13)
      )
    )
    rdd1.toDF("userid","level").registerTempTable("t1")
    rdd2.toDF("userid","age").registerTempTable("t2")
    val df3 = sqlContext.sql(
      """
        |select
        | t1.userid,
        | t1.level,
        | t2.age
        |from t1
        |join t2 on t1.userid = t2.userid
      """.stripMargin)
    df3.collect().foreach(println)
  }
}
