/**
 * Created by sachin on 12/10/15.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object SocketStreamingStateFull {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SocketStreamingStateFull")
      .set("spark.streaming.blockInterval", "100")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getRootLogger.setLevel(Level.WARN)

    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    val ssc = new StreamingContext(conf, Seconds(30))
    ssc.checkpoint("./checkpoint")

    val initialRDD = ssc.sparkContext.parallelize(List(("hello", 1), ("world", 1)))

    val lines = ssc.socketTextStream("localhost", 9998)

    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))

    pairs.print

    val stateDstream = pairs.updateStateByKey[Int](updateFunc)
    stateDstream.print()

    ssc.start()
    ssc.awaitTermination()


  }
}
