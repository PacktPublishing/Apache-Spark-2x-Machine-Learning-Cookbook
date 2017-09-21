package spark.ml.cookbook.chapter5

// myRegress01 - recipie0

import org.apache.spark.sql.SparkSession

import scala.math._

/**
 * Created by Siamak Amirghodsi on 2/10/2016.
 */

object MyRegress01_20 {

  def main(args: Array[String]): Unit = {

    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Create a configuration object and set parameters(spark mode, application name and a home directory
    // @Todo old 1.6
    //val conf = new SparkConf().setMaster("local[*]").setAppName("myRegress01").setSparkHome("C:\\spark-1.5.2-bin-hadoop2.6")

    // setup SparkSession to use for interactions with Spark
    // @Todo new 2.0
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("myRegress01_20")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    //import spark.implicits._

    // Create Spark Context so we can interact with the custer
    // Use the configuration object you defined above
    // @Todo old 1.6
    //val sc = new SparkContext(conf)
    //  @Todo new 2.0
    val sc = spark.sparkContext
    // Create a SQL Context to provide interface to DataFrame
    // @Todo old 1.6
    //val sqlContext = new SQLContext(sc)


    val x = Array(1.0,5.0,8.0,10.0,15.0,21.0,27.0,30.0,38.0,45.0,50.0,64.0)
    val y = Array(5.0,1.0,4.0,11.0,25.0,18.0,33.0,20.0,30.0,43.0,55.0,57.0)
    val xRDD = sc.parallelize(x)
    val yRDD = sc.parallelize(y)

    println("-------------------------------------------")
    val zipedRDD = xRDD.zip(yRDD)
    zipedRDD.collect().foreach(println)

    val xSum = zipedRDD.map(_._1).sum()
    val ySum = zipedRDD.map(_._2).sum()
    val xySum= zipedRDD.map(c => c._1 * c._2).sum()
    val n= zipedRDD.count()

    val xMean = zipedRDD.map(_._1).mean()
    val yMean = zipedRDD.map(_._2).mean()
    val xyMean = zipedRDD.map(c => c._1 * c._2).mean()

    val xSquaredMean = zipedRDD.map(_._1).map(x => x * x).mean()
    val ySquaredMean = zipedRDD.map(_._2).map(y => y * y).mean()

    println("xMean yMean xyMean", xMean, yMean, xyMean)
    val numerator = xMean * yMean  - xyMean
    val denominator = xMean * xMean - xSquaredMean
    val slope = numerator / denominator
    println("slope %f5".format(slope))
    val b_intercept = yMean - (slope*xMean)
    println("Intercept", b_intercept)
  }

}
