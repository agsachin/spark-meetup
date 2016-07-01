
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession,ForeachWriter, Row}
import java.io.PrintWriter

/**
  * Created by mbriggs on 01/07/16.
  */
object ForEachSink extends App {

  class MyForeach extends ForeachWriter[Row]{

    // instantiate resources only in open(), since this class
    // is serialized to the executors
    @transient var writer: PrintWriter = null

    // called on each partition for each 'trigger' interval the 'version
    override def open(partitionId: Long, version: Long): Boolean = {
      //writer = new PrintWriter(new File("/tmp/output.txt"))
      true
    }

    override def process(value: Row): Unit = {
      //writer.write(value.toString)
      println(value.toString)
    }

    override def close(errorOrNull: Throwable): Unit = {
      //writer.close()
    }
  }

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder.
    master("local[*]")
    .appName("test-structuredStreaming")
    .config("spark.sql.shuffle.partitions",20)
    .config("spark.sql.streaming.schemaInference","true")
    .getOrCreate()

  spark.conf.set("spark.sql.streaming.checkpointLocation",
    "/tmp/spark/ckpt" + System.currentTimeMillis())

  import spark.implicits._

  // stream json files from local disk folder
  // format - {"createdAt":"2015-01-01 12:15:09", "signal":15, "deviceId": 1}
  val events = spark.readStream.json("/tmp/spark/wdw")

  // format 'createAt' column's type to timestamp
  val timetypedEvents = events.selectExpr("cast(createdAt as timestamp) eventTime", "deviceId", "signal")

  // calculate Number of events by device in 20 second time windows, every 10 seconds
  val windowedCount = timetypedEvents.groupBy(window($"eventTime", "20 seconds", "10 seconds" ),
    $"deviceId")
    .count()

  // start the stream, writing complete aggregated results to console
  val query = windowedCount.writeStream
    .outputMode("complete")
    .foreach(new MyForeach)
    .start()


  query.awaitTermination()

}
