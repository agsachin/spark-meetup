/**
 * Created by sachin on 12/10/15.
 */
package org.apache.spark.examples.streaming

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object DirectKafkaStreaming {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val brokers = "localhost:9092";
    val topics = "test1";

    val sparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("DirectKafkaStreaming")

    val ssc = new StreamingContext(sparkConf, Seconds(2))


    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    val lines = messages.map(_._2)
    val hashTags = lines.flatMap(status => status.split(" ").filter(_.startsWith("#")))
    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
