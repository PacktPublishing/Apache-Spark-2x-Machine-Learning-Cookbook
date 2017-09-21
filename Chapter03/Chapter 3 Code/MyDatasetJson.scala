package spark.ml.cookbook.chapter3

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level


object MyDatasetJson {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // setup SparkSession to use for interactions with Spark
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("mydatasetjson")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    import spark.implicits._

    val cars = spark.read.json("../data/sparkml2/chapter3/cars.json").as[Car]
    cars.show()

    cars.columns.foreach(println)
    println()

    println(cars.schema)

    cars.select("make").distinct().show()

    cars.createOrReplaceTempView("cars")
    spark.sql("select make, model, kind from cars where kind = 'electric'").show()

    spark.stop()
  }
}
