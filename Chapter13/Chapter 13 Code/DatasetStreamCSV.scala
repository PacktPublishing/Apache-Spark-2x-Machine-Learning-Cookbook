package spark.ml.cookbook.chapter13

import java.util.concurrent.TimeUnit

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.SparkConf

case class StockPrice(date: String, open: Double, high: Double, low: Double, close: Double, volume: Integer, adjclose: Double)

object DatasetStreamCSV {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // setup SparkSession to use for interactions with Spark
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Dataset Stream")
      .config("spark.sql.warehouse.dir", ".")

      .getOrCreate()

    import spark.implicits._
        val s = spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load("../data/sparkml2/chapter13/GE.csv")

    s.printSchema()
    s.show()
    val conf  = new SparkConf()
    val streamDataset = spark.readStream
      .schema(s.schema)
      .option("sep", ",")
      .option("header", "true")
      .csv("../data/sparkml2/chapter13/ge").as[StockPrice]


    streamDataset.printSchema()

   val ge = streamDataset.filter("close > 100.00")

    val query = ge.writeStream
      .outputMode("append")
      .trigger(ProcessingTime(1, TimeUnit.SECONDS))
      .format("console")

    query.start().awaitTermination()

  }
}
