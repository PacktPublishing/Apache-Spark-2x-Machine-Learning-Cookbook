package spark.ml.cookbook.chapter1

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object MyFirstSpark20 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("myFirstSpark20")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    val x = Array(1.0,5.0,8.0,10.0,15.0,21.0,27.0,30.0,38.0,45.0,50.0,64.0)
    val y = Array(5.0,1.0,4.0,11.0,25.0,18.0,33.0,20.0,30.0,43.0,55.0,57.0)


    val xRDD = spark.sparkContext.parallelize(x)
    val yRDD = spark.sparkContext.parallelize(y)

    val zipedRDD = xRDD.zip(yRDD)
    zipedRDD.collect().foreach(println)

    val xSum = zipedRDD.map(_._1).sum()
    val ySum = zipedRDD.map(_._2).sum()
    val xySum= zipedRDD.map(c => c._1 * c._2).sum()
    val n= zipedRDD.count()

    println("RDD X Sum: " +xSum)
    println("RDD Y Sum: " +ySum)
    println("RDD X*Y Sum: "+xySum)
    println("Total count: "+n)
    spark.stop()

  }
}
