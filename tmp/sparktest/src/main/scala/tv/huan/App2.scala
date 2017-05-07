package tv.huan

/**
  * Created by zhaoshufen on 2017/5/4.
  */
object App2 {
  def main(args: Array[String]): Unit = {
    val t = Seq(("a",1)).toMap
    for((k,v) <- t){
      println(k + "-" + v)
    }
  }
}
