package tv.huan

/**
  * Created by zhaoshufen on 2017/4/23.
  */
object PartialFuncTest {
  def main(args: Array[String]): Unit = {
    //定义一个偏函数 只能接收Int类型，并且只接收匹配1的情况,所谓的偏函数就是说只用了Int的部分值而已
    val p:PartialFunction[Int,String] = {
      case 1 => "one"
    }
    val p1:PartialFunction[Int,String] = {
      case 2 => "two"
    }
    val p2 = p orElse p1
    println(p2(2))
    //p(1)
    println(p(1)) // one
    println(p.apply(1)) // one
    println(p.applyOrElse[Int,String](1,{msg=>{"applyOrElse 1"}})) // one
    //println(p(2)) //MatchError

    //模拟spark的Endpoint接收消息的偏函数
    val nep = new NettyRpcEndpoint()
    nep.receive.applyOrElse[Any,Unit]("a",{ msg =>{
      println(s"unsupport msg : ${msg}")
    }})
    nep.receive.applyOrElse[Any,Unit]("b",{ msg =>{
      println(s"unsupport msg : ${msg} ")
    }})
    nep.receive.applyOrElse[Any,Unit](1,{ msg =>{
      println(s"unsupport msg : ${msg} ")
    }})

  }
  trait RpcEndpoint{
    def receive(): PartialFunction[Any, Unit]
  }
  class NettyRpcEndpoint extends RpcEndpoint{
    def receive(): PartialFunction[Any, Unit]={
      case "a" => println("NettyRpcEndpoint -->a")
      case "b" => println("NettyRpcEndpoint -->b")
    }
  }

}
