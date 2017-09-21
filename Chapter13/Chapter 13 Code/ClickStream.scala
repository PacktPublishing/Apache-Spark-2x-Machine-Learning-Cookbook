package spark.ml.cookbook.chapter13

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.Queue

object ClickStream {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Streaming App")
      .config("spark.sql.warehouse.dir", ".")
      .config("spark.executor.memory", "2g")
      .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

    Logger.getRootLogger.setLevel(Level.WARN)

    val rddQueue = new Queue[RDD[String]]()

    val inputStream = ssc.queueStream(rddQueue)

    val clicks = inputStream.map(data => ClickGenerator.parseClicks(data))
    val clickCounts = clicks.map(c => c.url).countByValue()

    clickCounts.print(12)

    ssc.start()


    for (i <- 1 to 10) {
      rddQueue += ssc.sparkContext.parallelize(ClickGenerator.generateClicks(100))
      Thread.sleep(1000)
    }


    ssc.stop()
  }
}
