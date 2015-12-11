/**
 * Created by sachin on 8/22/15.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object KafkaStreamingStateFull {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("KafkaStreamingRepartition")
      .set("spark.streaming.blockInterval", "100")

    val zkQuorum = "localhost:2181";
    val group = "test";
    val topics = "test";
    val numThreads = "1";

    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    ssc.checkpoint("./checkpoint") //new line to added for checkpoint

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)



    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))

    pairs.print

    val stateDstream = pairs.updateStateByKey[Int](updateFunc) //new line to added to append the state

    stateDstream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}