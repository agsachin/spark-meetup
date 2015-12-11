/**
 * Created by sachin on 12/10/15.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TwitterStreaming {
  def main(args: Array[String]) {


    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val consumerKey = "G6d9Tl0Hh0uA3RC4Sx0Cg81tk";
    val consumerSecret = "i5tjc4EuavhEuxO5vzDpgTb57wkyX66QmgkwZIZCOXW8LTG3C2";
    val accessToken = "145001241-8ldS6p1Ci6HDk0PV9mk3c5ldnDSHzNBe6HK3FS31";
    val accessTokenSecret = "NbciwyIuQr3NHOqrQXNGHuenWuB163hOLcDF6FAAbZNNv";

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("TwitterPopularTags")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val stream = TwitterUtils.createStream(ssc, None)

    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      Thread sleep 1500
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}