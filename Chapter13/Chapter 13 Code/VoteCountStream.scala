package spark.ml.cookbook.chapter13

import java.util.concurrent.TimeUnit

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime

object VoteCountStream {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // setup SparkSession to use for interactions with Spark
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Test Stream")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    import spark.implicits._

    val stream = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // Generate vote count
    val villiansVote = stream.groupBy("value").count()

    // Start triggering the query that prints the running counts to the console
    val query = villiansVote.orderBy("count").writeStream
      .outputMode("complete")
      .format("console")
      .trigger(ProcessingTime.create(10, TimeUnit.SECONDS))
      .start()

    query.awaitTermination()
  }
}
