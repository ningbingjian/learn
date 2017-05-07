package tv.huan.recommender.item

import org.apache.spark.mllib.linalg.SparseVector

/**
  * Created by zhaoshufen on 2017/5/1.
  */
sealed trait ItemData extends Serializable
case class UserOrPref(userid:Int,
                      vector: Option[SparseVector] = None)
case class ItemRating(userid:Int,itemid:Int,pref:Double)

