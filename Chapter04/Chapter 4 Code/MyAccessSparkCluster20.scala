
package spark.ml.cookbook.chapter4

import org.apache.spark.sql.SparkSession




object MyAccessSparkCluster20 {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .master("local[*]")   // if use cluster master("spark://master:7077")
      .appName("MyAccesSparkCluster20")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    val df = spark.read
          .option("header","True")
          .csv("../data/sparkml2/chapter4/mySampleCSV.csv")


    df.show()
    spark.stop()
  }
}

