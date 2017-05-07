package tv.huan

import com.google.common.base.Objects

/**
  * Created by zhaoshufen on 2017/4/22.
  */
class MyArgs (args: Array[String]){
  var host = "127.0.0.1"
  var port = 7077
  var webUiPort = 8080

  private def parse(args:List[String]): Unit = args match {
    case ("--ip" | "-i"):: value :: tail =>
      host = value
      parse(tail)
    case ("--host" | "-h") :: value :: tail =>
      host = value
      parse(tail)
    case ("--port" | "-p") :: IntParam(value) :: tail =>
      port = value
      parse(tail)
    case "--webui-port" ::IntParam(value) :: tail =>
      webUiPort = value
      parse(tail)
    case Nil => {}
    case _ =>  // scalastyle:off println
      System.err.println(
        "Usage: Master [options]\n" +
          "\n" +
          "Options:\n" +
          "  -i HOST, --ip HOST     Hostname to listen on (deprecated, please use --host or -h) \n" +
          "  -h HOST, --host HOST   Hostname to listen on\n" +
          "  -p PORT, --port PORT   Port to listen on (default: 7077)\n" +
          "  --webui-port PORT      Port for web UI (default: 8080)\n" +
          "  --properties-file FILE Path to a custom Spark properties file.\n" +
          "                         Default is conf/spark-defaults.conf.")
      // scalastyle:on println
      System.exit(1)
  }
  parse(args.toList)
  object IntParam{
    def unapply(str: String): Option[Int] = {
      try {
        Some(str.toInt)
      } catch {
        case e:NumberFormatException => None
      }
    }
  }

  override def toString: String = Objects.toStringHelper(this.getClass.getName)
    .add("host",host)
    .add("port",port)
    .add("webUiPort",webUiPort)
    .toString

}
object MyArgs{
  def main(args: Array[String]): Unit = {
    val args1 = Array(
      "--ip","192.168.1.12",
      "--host","192.168.20.12",
      "--port","7078",
      "--webui-port","8088"
    )
    val myArgs1 = new MyArgs(args1)
    println(myArgs1)
    val args2 = Array(
      "--ip","192.168.1.21",
      "--host","192.168.20.21",
      "--port","7021",
      "--webui-port","8021"
    )
    val myArgs2 = new MyArgs(args2)
    println(myArgs2)
    /**
      *
      *tv.huan.MyArgs{host=192.168.20.12, port=7078, webUiPort=8088}
      tv.huan.MyArgs{host=192.168.20.21, port=7021, webUiPort=8021}
      *
      */
  }
}
