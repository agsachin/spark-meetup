/**
 * Created by sachin on 12/10/15.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object SocketStreamingTransform {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SocketStreamingTransform")
      .set("spark.streaming.blockInterval", "100")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val ssc = new StreamingContext(conf, Seconds(2))

    val joinFile = ssc.sparkContext.textFile("/Users/sachin/Documents/github/TestSpark/data.txt")
    val data = joinFile.map(line => line.split(",")).map(e => (e(0), e(1)))
    data.collect.foreach(println)

    print(data.collect().foreach(x => println(x)))

    val lines = ssc.socketTextStream("localhost", 9998)

    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))

    val wordCounts = pairs.reduceByKey(_ + _)

    wordCounts.print()

    val cleanedDStream = wordCounts.transform(rdd => {
      rdd.join(data)
    })

    cleanedDStream.print

    ssc.start()
    ssc.awaitTermination()


  }
}
