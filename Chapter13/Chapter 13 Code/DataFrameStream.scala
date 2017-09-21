package spark.ml.cookbook.chapter13

import java.util.concurrent.TimeUnit

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime

object DataFrameStream {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // setup SparkSession to use for interactions with Spark
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("DataFrame Stream")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    import spark.implicits._


    val df = spark.read
            .format("json")
            .option("inferSchema", "true")
            .load("../data/sparkml2/chapter13/person.json")
    df.printSchema()
    df.show()

    val stream = spark.readStream
          .schema(df.schema)
          .option("maxFilesPerTrigger", "1")
          .json("../data/sparkml2/chapter13/people")

    stream.printSchema()

    val people = stream.select("name", "age").where("age > 60")

    val query = people.writeStream
    .outputMode("append")
      .trigger(ProcessingTime(1, TimeUnit.SECONDS))
      .format("console")

    query.start().awaitTermination()
  }
}
