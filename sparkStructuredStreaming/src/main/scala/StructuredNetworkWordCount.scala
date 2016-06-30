package scala

import org.apache.spark.sql.SparkSession


object StructuredNetworkWordCount {
  def main(args: Array[String]) {
//    if (args.length < 2) {
//      System.err.println("Usage: StructuredNetworkWordCount <hostname> <port>")
//      System.exit(1)
//    }

//    val host = args(0)
//    val port = args(1).toInt

    val host = "localhost"
    val port = 9999

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()
    // Create DataFrame representing the stream of input lines from connection to host:port
    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load().as[String]

    // Split the lines into words
    val words = lines.flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
