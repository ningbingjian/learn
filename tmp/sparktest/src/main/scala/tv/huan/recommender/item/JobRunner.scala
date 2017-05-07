package tv.huan.recommender.item

/**
  * Created by zhaoshufen on 2017/5/1.
  */
abstract class JobRunner extends Serializable{
    def run(jc:JobContext)
}
