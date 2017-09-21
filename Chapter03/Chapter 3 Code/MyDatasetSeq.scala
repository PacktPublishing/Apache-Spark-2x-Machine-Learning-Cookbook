package spark.ml.cookbook.chapter3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MyDatasetSeq {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // setup SparkSession to use for interactions with Spark
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("mydatasetseq")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    import spark.implicits._

    val cars = spark.createDataset(MyDatasetData.carData)
    cars.show(false)

    cars.columns.foreach(println)
    println()

    println(cars.schema)

    cars.filter(cars("price") > 50000.00).show()

    spark.stop()
  }

}
