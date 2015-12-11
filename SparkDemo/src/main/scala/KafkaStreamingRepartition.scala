/**
 * Created by sachin on 8/22/15.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object KafkaStreamingRepartition {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("KafkaStreamingRepartition")
      .set("spark.streaming.blockInterval", "100") //new line to add block interval

    val zkQuorum = "localhost:2181";
    val group = "test";
    val topics = "test";
    val numThreads = "1";


    def fun(rdd: RDD[String]): Unit = {
      print("partition count" + rdd.partitions.length + "\n")
    }


    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    val re_lines = lines.repartition(5) //new line to print partition

    re_lines.foreachRDD(x => fun(x))

    val words = re_lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))

    val wordCounts = pairs.reduceByKey(_ + _)

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}