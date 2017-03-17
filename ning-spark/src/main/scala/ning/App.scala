package ning

import java.nio.file.{Files, Paths}

import org.apache.commons.io.{FileUtils, FilenameUtils}
import java.io.File

import ning.chapter12.{BasicIntQueue,Doubling, Incrementing}
import org.apache.commons.lang.StringUtils

import scala.collection.JavaConversions._
/**
 * Hello world!
 *
 */
object App extends Application {
  class A extends   BasicIntQueue with Incrementing with Doubling{
      override def put(x: Int) {
        super.put(x)
      }
  }
  val a = new A
  a.put(2)
  println(a.get())

 // println( Seq("a","b","c").reduceLeft(_++_))
/*
  val dir = "D:/temp"
  val paths = Files.newDirectoryStream(Paths.get(dir),"*.1234")
  val sort = paths.map(path =>FilenameUtils.getBaseName(path.toString))
    .filter(StringUtils.isNumeric(_))
    .map(Integer.valueOf(_))
    .toList
    .sortWith(_ > _)
    .toList
  var max = sort(0)
  val newPics = Files.newDirectoryStream(Paths.get(dir),"*.jpg")
    .toList
    .sortWith(_.compareTo(_) > 0)
    .map(pic => {
      val file1 = new File(pic.toString)
      max = max + 1
      val file2 = file1.renameTo(new File(dir+"/"+max+".1234"))
    })
*/
 /* val arr = Array[Long](3,5,7)
  val result = arr.scanLeft(0L)(_+_).toList
  println(result)
  val a = new A
  val r1 = arr.map(_+a.nextPartition())
  println(r1.toList)
  class A{
    var nextPartitionToRead = 0
    def nextPartition():Int = {
      nextPartitionToRead += 1
      nextPartitionToRead
    }

  }*/

}
