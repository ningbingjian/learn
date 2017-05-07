package tv.huan.recommender

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhaoshufen on 2017/4/30.
  */
object TestCF {
  def main(args: Array[String]) {

    //0 构建Spark对象
    val conf = new SparkConf().setAppName("ItemCF")
    System.setProperty("hadoop.home.dir","D:\\tool\\hadoop\\hadoop-2.7.1")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    //1 读取样本数据
    val data_path = this.getClass.getResource("/") +  "/home/huangmeiling/sample_itemcf2.txt"
    val data = sc.textFile(data_path)
    val userdata = data.map(_.split(",")).map(f => (ItemPref(f(0), f(1), f(2).toDouble))).cache()

    //2 建立模型
    val mysimil = new ItemSimilarity()
    val simil_rdd1 = mysimil.Similarity(userdata, "cooccurrence")
    val recommd = new RecommendedItem
    val recommd_rdd1 = recommd.Recommend(simil_rdd1, userdata, 30)

    //3 打印结果
    println(s"物品相似度矩阵: ${simil_rdd1.count()}")
    simil_rdd1.collect().foreach { ItemSimi =>
      println(ItemSimi.itemid1 + ", " + ItemSimi.itemid2 + ", " + ItemSimi.similar)
    }
    println(s"用戶推荐列表: ${recommd_rdd1.count()}")
    recommd_rdd1.collect().foreach { UserRecomm =>
      println(UserRecomm.userid + ", " + UserRecomm.itemid + ", " + UserRecomm.pref)
    }

  }
}
