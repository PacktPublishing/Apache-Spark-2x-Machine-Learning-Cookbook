
package spark.ml.cookbook.chapter11

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession




object MyCSV {
  def main(args: Array[String]) {



    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("MyCSV")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    val dataFile  = "../data/sparkml2/chapter11/ratings.csv"
    // 1. load the csv file as text file
    val file = spark.sparkContext.textFile(dataFile)
    val headerAndData = file.map(line => line.split(",").map(_.trim))
    val header = headerAndData.first
    val data = headerAndData.filter(_(0) != header(0))
    val maps = data.map(splits => header.zip(splits).toMap)
    val result = maps.take(10)
    result.foreach(println)

    // 2. load the csv file using format
    val df = spark.read.format("com.databricks.spark.csv").option("header", "true").load(dataFile)
    df.createOrReplaceTempView("ratings")
    val resDF = spark.sql("select * from ratings")

    resDF.show(10, false)


    spark.stop()
  }
}

