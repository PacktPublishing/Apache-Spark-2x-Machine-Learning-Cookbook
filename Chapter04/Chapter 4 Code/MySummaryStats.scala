package spark.ml.cookbook.chapter4

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level


object MySummaryStats  {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // setup SparkSession to use for interactions with Spark
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Summary Statistics")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()
    val sc = spark.sparkContext

    // handcrafted data set for understanding of Stats Summary
    val rdd = sc.parallelize(
      Seq(
        Vectors.dense(0, 1, 0),
        Vectors.dense(1.0, 10.0, 100.0),
        Vectors.dense(3.0, 30.0, 300.0),
        Vectors.dense(5.0, 50.0, 500.0),
        Vectors.dense(7.0, 70.0, 700.0),
        Vectors.dense(9.0, 90.0, 900.0),
        Vectors.dense(11.0, 110.0, 1100.0)
      )
    )

    // Compute column summary statistics.
    val summary = Statistics.colStats(rdd)
    println("mean:" + summary.mean)
    println("variance:" +summary.variance)
    println("none zero" + summary.numNonzeros)
    println("min:" + summary.min)
    println("max:" + summary.max)
    println("count:" + summary.count)

    spark.stop()
  }
}