package ning.spark.suite

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext}
import org.scalatest.{Suite, BeforeAndAfterAll}

/**
  * Created by ning on 2016/9/18.
  */
trait SharedSparkContext  extends BeforeAndAfterAll{
  //self => 这句相当于给this起了一个别名为self
  //由Suite类型混入
  self:Suite =>

  @transient private var _sc: SparkContext = _

  def  sc:SparkContext = _sc

  var conf = new SparkConf(false)

  override def beforeAll(){
    _sc = new SparkContext("local[4]", "test", conf)
    super.beforeAll()
  }
  override def afterAll(){
    LocalSparkContext.stop(_sc)
    _sc = null
    super.afterAll()
  }

}
