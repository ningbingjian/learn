package tv.huan.recommender.item.measures
import org.apache.spark.mllib.linalg.{SparseVector, Vectors, Vector => MLV}


/**
  * Created by zhaoshufen on 2017/5/1.
  */
abstract class VectorSimilarityMeasure extends Serializable{
  def normalize(vector:MLV):MLV
  def aggregate(nonZeroValueA: Double, nonZeroValueB: Double):Double
  def similarity(summedAggregations: Double, normA: Double, normB: Double, numberOfColumns: Int): Double
}
class CosineSimilarity extends VectorSimilarityMeasure{
  override def normalize(vector: MLV):MLV = {
    import tv.huan.recommender.item.enhance.EnHanceVector._
    vector match {
      case v:SparseVector => v.normalize()
      case _ => throw new UnsupportedOperationException("unsupport vector :" +vector.getClass)
    }
  }

  override def similarity(summedAggregations: Double,
                          normA: Double,
                          normB: Double,
                          numberOfColumns: Int):Double = summedAggregations
  override def aggregate(nonZeroValueA: Double, nonZeroValueB: Double) : Double = {
    nonZeroValueA * nonZeroValueB
  }
}
class PearsonCorrelationSimilarity extends CosineSimilarity{
  import tv.huan.recommender.item.enhance.EnHanceVector._
  override def normalize(vector: MLV):MLV = {
    vector match {
      case v:SparseVector =>
        val avg = Vectors.norm(vector,1) / vector.numNonzeros
        v.eachValue(v => v-avg)
        v.normalize()
    }

  }
}



