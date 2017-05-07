package tv.huan.recommender.item

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{SparseVector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import Constatns._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import tv.huan.recommender.item.measures.{CosineSimilarity, PearsonCorrelationSimilarity}
import tv.huan.recommender.item.enhance.EnHanceVectors
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vectors, Vector => MLV}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import collection.immutable
import collection.mutable
import tv.huan.recommender.item.enhance.EnHanceVector._
/**
  * Created by zhaoshufen on 2017/5/1.
  */
object RecommenderJob {
  val similarity = new PearsonCorrelationSimilarity()
  case class VectorOrPref(vector:Option[MLV] = None,
                          userid:Option[Int] = None,
                          pref:Option[Double] = None)
  case class VectorAndPref(vector:Option[MLV] = None,
                           userids:Option[Seq[Int]] = None,
                           prefs:Option[Seq[Double]] = None)
  case class PrefAndSimilarity(itemid:Int,pref:Double,vector:MLV)
  def main(args: Array[String]): Unit = {
    //0 构建Spark对象
    val conf = new SparkConf().setAppName("ItemCF")
    System.setProperty("hadoop.home.dir","D:\\tool\\hadoop\\hadoop-2.7.1")
    conf.setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val jc = new JobContext(sc,sqlContext)
    new ReadItemRatingJob().run(jc)
    new PreparePreferenceMatrixJob().run(jc)
    new NormsAndTransposeJob().run(jc)
    new SimilarityJob().run(jc)
    new TopKSimilarityJob().run(jc)
    new PartialMultiplyJob().run(jc)
    new AggregateAndRecommendJob().run(jc)
  }

  /**
    * 评分RDD构建
    */
  class ReadItemRatingJob extends JobRunner{
    override def run(jc: JobContext) = {
      val sc = jc.sc
      //1 读取样本数据
      val data_path = this.getClass.getResource("/") +  "/home/huangmeiling/sample_itemcf2.txt"
      val data = sc.textFile(data_path)
      val rating = data.map(_.split(","))
        .map{
          case Array(userid,itemid,pref) =>
            ItemRating(userid.toInt,itemid.toInt,pref.toDouble)
        }.cache()
      jc.set(classOf[ReadItemRatingJob].getName,rating)
    }
  }

  /**
    * userVector,itemVector构建
    */
  class  PreparePreferenceMatrixJob extends JobRunner{
    override def run(jc: JobContext) = {
      val sc = jc.sc
      val ratingRdd = jc.getAs[RDD[ItemRating]](classOf[ReadItemRatingJob].getName)

      // 2 转换为物品向量
      val itemVector = ratingRdd.map{
        case ir:ItemRating => (ir.itemid,(ir.userid,ir.pref))
      }.groupByKey()
        .map{
          case (itemid,rating) => {
            (itemid,Vectors.sparse(Integer.MAX_VALUE,rating.toSeq))
          }
        }
      // 3 转换为用户向量
      val userVector = ratingRdd.map{
        case ir:ItemRating => (ir.userid,(ir.itemid,ir.pref))
      }.groupByKey()
        .map{
          case (userid,rating) => {
            (userid,Vectors.sparse(Integer.MAX_VALUE,rating.toSeq))
          }
        }
      itemVector.collect().foreach(println)
      jc.set(ITEM_VECTOR,itemVector)
      jc.set(USER_VECTOR,userVector)
    }
  }

  /**
    * 物品向量归一化操作
    */
  class NormsAndTransposeJob extends JobRunner{
    override def run(jc: JobContext) = {
      val itemVector = jc.getAs[RDD[(Int,MLV)]](ITEM_VECTOR)
      val norm = itemVector.flatMap{
        case (itemid,vector) =>
          val buf = ListBuffer[(Int,MLV)]()
          val normVector = similarity.normalize(vector).asInstanceOf[SparseVector]
          for(i <- 0 until normVector.indices.length){
            val userid = normVector.indices(i)
            val pref = normVector.values(i)
            val vector = Vectors.sparse(Integer.MAX_VALUE,Seq((itemid,pref))).asInstanceOf[SparseVector]
            buf append ((userid,vector))
          }
          buf.toSeq
      }.reduceByKey((v1,v2) =>{
         EnHanceVectors.merge(v1,v2)
       })
      jc.set(NORM_VECTOR,norm)
    }
  }
  class SimilarityJob extends JobRunner{
    def run(jc:JobContext)={
      val normVectorRdd = jc.getAs[RDD[(Int,MLV)]](NORM_VECTOR)
      val similarityVector = normVectorRdd.flatMap {
        case (userid,vector:SparseVector) =>
          val result = ListBuffer[(Int,MLV)]()
          for(n <- 0 until vector.indices.length){
            val lb = ListBuffer[(Int,Double)]()
            val occurrenceValueA = vector.values(n)
            val occurrenceIndexA = vector.indices(n)
            for(m <- n until vector.indices.length){
              val occurrenceValueB = vector.values(m)
              val occurrenceIndexB = vector.indices(m)
              if(occurrenceIndexA != occurrenceIndexB){//这里有待考证 移除自己和自己的相似度
                lb.append((occurrenceIndexB,similarity.aggregate(occurrenceValueA,occurrenceValueB)))
              }
            }
              val dots = Vectors.sparse(Integer.MAX_VALUE,lb.toSeq)
              result.append((occurrenceIndexA,dots))
          }
          result
      }.reduceByKey {
        case (v1:SparseVector,v2:SparseVector) =>
          EnHanceVectors.add(v1,v2)
      }
      similarityVector.collect().foreach(println)
      jc.set(SIMILARITY_VECTOR,similarityVector)
    }
  }

  class TopKSimilarityJob extends JobRunner{
    override def run(jc: JobContext) = {
      val simVectorRdd = jc.getAs[RDD[(Int,MLV)]](SIMILARITY_VECTOR)
      val topKSim = simVectorRdd.flatMap{
        case (itemid,v:SparseVector) => {
          val lb = ListBuffer[(Int,MLV)]()
          val topK = EnHanceVectors.topK(v,SIMILARITY_TOP_N)
          for(i <- 0 until v.indices.length){
            val vector = Vectors.sparse(Integer.MAX_VALUE,Array(itemid),Array(v.values(i)))
            lb append ((v.indices(i),vector))
          }
          lb append ((itemid,topK))
          lb
        }
      }.reduceByKey((v1,v2) => EnHanceVectors.merge(v1,v2))
      topKSim.collect().foreach(println)
      jc.set(TOPK_VECTOR,topKSim)
    }
  }
  class PartialMultiplyJob extends JobRunner{
    override def run(jc: JobContext) = {
      val userVector = jc.getAs[RDD[(Int,SparseVector)]](USER_VECTOR)
      val topkVector = jc.getAs[RDD[(Int,SparseVector)]](TOPK_VECTOR)
      val rdd1 = userVector.flatMap {
        case (userid,v:SparseVector) =>
          val lb = ListBuffer[(Int,VectorOrPref)]()
          for(i <- 0 until v.indices.length){
            val vectorOrPref = VectorOrPref(None,Some(userid),Some(v.values(i)))
            lb append ((v.indices(i),vectorOrPref))
          }
          lb
      }
      val rdd2 = topkVector.map {
        case (itemid,v:SparseVector) =>
          val vectorOrPref = VectorOrPref(Some(v),None,None)
          ((itemid,vectorOrPref))
      }

      val rdd3 = rdd1 union rdd2
      rdd3.collect().foreach(println)
      val vectorAndPrefRdd = rdd3.groupByKey().map {
        case (itemid,vectorOrPrefs:Iterable[VectorOrPref]) =>
          val userids = ListBuffer[Int]();
          val prefs = ListBuffer[Double]();
          var vector:MLV = null;
          for(vectorOrPref <- vectorOrPrefs){
            if(vectorOrPref.vector == None){
              userids append vectorOrPref.userid.get
              prefs append vectorOrPref.pref.get
            }else{
              vector = vectorOrPref.vector.get
            }
          }
          (itemid,VectorAndPref(Some(vector),Some(userids),Some(prefs)))
      }
      vectorAndPrefRdd.collect().foreach(println)
      vectorAndPrefRdd
      jc.set(VECTOR_AND_PREF,vectorAndPrefRdd)
    }
  }
  class AggregateAndRecommendJob extends JobRunner{
    override def run(jc: JobContext) = {
      val vectorAndPrefRdd = jc.getAs[RDD[(Int,VectorAndPref)]](VECTOR_AND_PREF)
      val prefAndSimilarity = vectorAndPrefRdd.flatMap{
        case (itemid,vectorAndPref:VectorAndPref) =>
          val userids = vectorAndPref.userids.get
          val prefs = vectorAndPref.prefs.get
          val simVector = vectorAndPref.vector.get
          val lb = ListBuffer[(Int,PrefAndSimilarity)]()
          for(i <- 0 until userids.length){
            val userid = userids(i)
            val pref = prefs(i)
            if(pref != Double.NaN){
              lb append ((userid,PrefAndSimilarity(itemid,pref,simVector)))
            }
          }
          lb
      }
      prefAndSimilarity.collect().foreach(println)
      val recommenderItems = prefAndSimilarity.groupByKey()
        .map{
          case (userid,prefAndSimilarities:Iterable[PrefAndSimilarity]) =>
            var numerators:SparseVector = null
            var denominators:SparseVector = null
            for(prefAndSimilarity <- prefAndSimilarities){
              val simVector = prefAndSimilarity.vector.asInstanceOf[SparseVector]
              val pref = prefAndSimilarity.pref
              if(denominators == null)
                denominators = simVector.copy
              else
                denominators = denominators.assign(simVector,(x,y) => Math.abs(x) + Math.abs(y))
              if (numerators == null)
                numerators = simVector.copy
              else{
                simVector.assign((x,y)=>x * y,pref)
                numerators = numerators.assign(simVector,(x,y) => x+y)
              }
            }
            if(numerators != null){
              val lb = ListBuffer[(Int,Double)]()
              val numeratorMap = numerators.indices.zip(numerators.values).toMap
              val denominatorMap = denominators.indices.zip(denominators.values).toMap
              for((k,v) <- numeratorMap){
                val predition = v/denominatorMap(k)
                lb append ((k,predition))
              }
              (userid,lb.sortBy(_._2).reverse)
            }
        }
      recommenderItems.collect().foreach(println)
    }
  }

}
