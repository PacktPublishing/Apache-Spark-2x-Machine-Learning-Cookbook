package  spark.ml.cookbook.chapter3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MyDatasetRDD {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // setup SparkSession to use for interactions with Spark
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("mydatasetrdd")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    val rdd = sc.makeRDD(MyDatasetData.carData)
    val cars = spark.createDataset(rdd)
    cars.show(false)

    cars.columns.foreach(println)
    println()
    println(cars.schema)

    cars.groupBy("make").count().show()

    val carRDD = cars.where("make = 'Tesla'").rdd
    carRDD.foreach(println)

    spark.stop()
  }
}
