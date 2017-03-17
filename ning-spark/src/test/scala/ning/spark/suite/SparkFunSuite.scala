package ning.spark.suite

import org.apache.spark.Logging
import org.scalatest.{Outcome, FunSuite}

/**
  * Created by ning on 2016/9/18.
  */
abstract class SparkFunSuite extends FunSuite with Logging{
  final protected override def withFixture(test: NoArgTest): Outcome = {
    val testName = test.text
    val suiteName = this.getClass.getName
    val shortSuiteName = suiteName.replaceAll("org.apache.spark", "o.a.s")
    try {
      logInfo(s"\n\n===== TEST OUTPUT FOR $shortSuiteName: '$testName' =====\n")
      test()
    } finally {
      logInfo(s"\n\n===== FINISHED $shortSuiteName: '$testName' =====\n")
    }
  }
}
