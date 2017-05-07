package tv.huan

import java.net.InetSocketAddress
import scala.collection.mutable
/**
  * Created by zhaoshufen on 2017/4/27.
  */
object App1 {
  def main(args: Array[String]): Unit = {
    println( Option(getClass.getResourceAsStream("foo")))
    trait Service1
    trait Service2
    def make1() = new Service1 {
      def getId = 123
    }
    def make2() = new Service2 {
      def getId = 456
    }
    val m1 = make1()
    println(m1.getId)


  }

  private def test1 = {
    val list = List(4, 3, 2)
    val default = 1
    val head = list match {
      case head :: _ => head
      case Nil => default
    }
    println(head)
    println(list.headOption.getOrElse(default))
    println(list.headOption getOrElse default)
    list.headOption.getOrElse(default)
    list.headOption getOrElse default
    println(Some(null).get)

    val host: Option[String] = Some("127.0.0.1")
    val port: Option[Int] = Some(1024)
    /*
    val addr: Option[InetSocketAddress] =
      host flatMap { h =>
        port map { p =>
          new InetSocketAddress(h, p)
        }
      }*/
    val addr: Option[InetSocketAddress] = for {
      h <- host
      p <- port
    } yield new InetSocketAddress(h, p)
    println(addr.get)
    val m = mutable.Map[Any, Any]()

    def getAs[K, V](key: K): V = {
      m(key).asInstanceOf[V]
    }

    m += "k1" -> 1
    m += "k2" -> 2
    println(m)
    val s = getAs[String, Integer]("k1")
    println(s)
    val f = (s: String) => s.toInt
    val g = (i: Int) => i * 2
    val h = g.compose(f)
  }
}
