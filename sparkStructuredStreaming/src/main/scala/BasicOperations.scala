package scala

import java.io.{File, PrintWriter}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.concurrent.ConcurrentLinkedQueue

import org.apache.spark.sql.{ForeachWriter, SparkSession}

import scala.collection.mutable

/**
 * Created by sachin on 6/30/16.
 */
object BasicOperations {

  def main(args: Array[String]) {
    //    if (args.length < 2) {
    //      System.err.println("Usage: StructuredNetworkWordCount <hostname> <port>")
    //      System.exit(1)
    //    }

    //    val host = args(0)
    //    val port = args(1).toInt
//    Logger.getLogger("org").setLevel(Level.OFF)
//    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession
      .builder
      .master("local[*]")
      .config("spark.sql.streaming.schemaInference","true")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    val lines = spark.readStream.option("inferSchema","true").json("/Users/sachin/testSpark/inputJson")
    import org.apache.spark.sql.functions._
    import spark.implicits._
    lines.printSchema()

    val filtered = lines.filter($"signal" < 10)
    filtered.printSchema()

    import org.apache.spark.sql.functions.udf
    val String2Time = udf((time:String) => { val df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");new Timestamp(df1.parse(time).getTime)})
    val time = filtered.withColumn("newTime", String2Time($"time"))
    time.printSchema()

    // Number of events in every 1 minute time windows
    val windowedCount = time.groupBy(window(time.col("newTime"), "1 minute"))
      .count()


    // Average number of events for each device type in every 1 minute time windows
    val windowedAverageSignal = time.groupBy(
      filtered.col("dataType"),
      window(time.col("time"), "1 minute"))
      .avg("signal")

    val printWindow =  windowedAverageSignal.select($"window")

    val count =filtered.groupBy("dataType").count()

    val query = printWindow.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    val query2 = windowedAverageSignal.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    val test = windowedAverageSignal.repartition(2).select($"avg(signal)").as[Double]
    test.printSchema()
    test.writeStream
      .outputMode("complete")
      .foreach (new TestForeachWriter)
      .start()

    query.processAllAvailable()

    query2.processAllAvailable()


    // val dataType = lines.groupByKey(row => row.getAs("dataType"))
    // val averageSignal = dataType.agg(typed.avg(row=>row.getAs("signal")))
    // case class DeviceDataString(device: String, dataType: String, signal: Double, time: java.sql.Timestamp)
    //
    //
    //    val dsString = lines.map(row => DeviceDataString(row.split(",")(0),row.split(",")(1),row.split(",")(2).toDouble,row.split(",")(3)))
    //    val t1= new DateTime("2010-06-30T01:20")
    //    val c1 = DeviceData("device","a",1.0,DateTime.parse("2010-06-30T01:20"))
    //    val c2 = DeviceData("device","b",1.0,DateTime.parse("2010-06-30T01:20"))
    //    val ds1 = Seq(1, 2, 3).toDS()
    //    val ds = Seq(DeviceData("device","a",1.0,)).toDS()
    // import org.apache.spark.sql.functions.udf
    // val String2Time = udf((time:String) => DateTime.parse(time))

    // Running average signal for each device type
    //import org.apache.spark.sql.expressions.scalalang.typed._
    //ds.groupByKey(_.Type).agg(typed.avg(_.signal))    //
  }
}

class MyForeach extends ForeachWriter[String]{
  var writer = new PrintWriter(new File("/tmp/test.txt"))

  override def open(partitionId: Long, version: Long): Boolean = {
    writer = new PrintWriter(new File("/tmp/output.txt"))
    true
  }

  override def process(value: String): Unit = {
    writer.write(value.toString)
  }

  override def close(errorOrNull: Throwable): Unit = {
    writer.close()
  }
}

class MyConsoleForeach extends ForeachWriter[String]{

  override def open(partitionId: Long, version: Long): Boolean = {
    println(partitionId+"::"+version)
    true
  }

  override def process(value: String): Unit = {
    println(value)
  }

  override def close(errorOrNull: Throwable): Unit = {
    println(errorOrNull.getMessage)
  }
}
object ForeachSinkSuite {

  trait Event

  case class Open(partition: Long, version: Long) extends Event

  case class Process[T](value: T) extends Event

  case class Close(error: Option[Throwable]) extends Event

  private val _allEvents = new ConcurrentLinkedQueue[Seq[Event]]()

  def addEvents(events: Seq[Event]): Unit = {
    _allEvents.add(events)
  }

  def allEvents(): Seq[Seq[Event]] = {
    _allEvents.toArray(new Array[Seq[Event]](_allEvents.size()))
  }

  def clear(): Unit = {
    _allEvents.clear()
  }
}

/** A [[ForeachWriter]] that writes collected events to ForeachSinkSuite */
class TestForeachWriter extends ForeachWriter[Double] {
  ForeachSinkSuite.clear()

  private val events = mutable.ArrayBuffer[ForeachSinkSuite.Event]()

  override def open(partitionId: Long, version: Long): Boolean = {
    events += ForeachSinkSuite.Open(partition = partitionId, version = version)
    true
  }

  override def process(value: Double): Unit = {
    events += ForeachSinkSuite.Process(value)
  }

  override def close(errorOrNull: Throwable): Unit = {
    events += ForeachSinkSuite.Close(error = Option(errorOrNull))
    ForeachSinkSuite.addEvents(events)
    events.foreach(println)
  }
}