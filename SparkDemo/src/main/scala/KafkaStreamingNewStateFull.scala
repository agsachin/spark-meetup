/**
 * Created by sachin on 8/22/15.
 */

import _root_.kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.State
import org.apache.spark.streaming.kafka._

object KafkaStreamingNewStateFull {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("KafkaStreamingRepartition")
      .set("spark.streaming.blockInterval", "100")

    val topics = "test";

    def trackStateFunc(batchTime: Time, key: String, value: Option[Int], state: State[Long]): Option[(String, Long)] = {
      val sum = value.getOrElse(0).toLong + state.getOption.getOrElse(0L)
      val output = (key, sum)
      state.update(sum)
      Some(output)
    }

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    ssc.checkpoint("./checkpoint") //new line to added for checkpoint

    val brokers = "localhost:9092"
    val topicsSet = topics.split(",").toSet

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      "auto.offset.reset"->"smallest")

    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)


    val initialRDD = ssc.sparkContext.parallelize(List(("dummy", 1L), ("source", 1L)))

    val words = lines.map(_._2).flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))

    //pairs.print

    val stateSpec = StateSpec.function(trackStateFunc _)
      .initialState(initialRDD)
      .numPartitions(2)
      .timeout(Seconds(60))

    val wordCountStateStream = pairs.mapWithState(stateSpec)
    wordCountStateStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}