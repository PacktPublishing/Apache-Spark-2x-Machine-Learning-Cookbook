package spark.ml.cookbook.chapter8

import org.apache.spark.ml.clustering.BisectingKMeans
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object MyBisectingKMeans {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("MyBisectingKMeans")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    // https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass.html#glass
    val dataset = spark.read.format("libsvm").load("../data/sparkml2/chapter8/glass.scale")
    dataset.show(false)

    val splitData = dataset.randomSplit(Array(80.0, 20.0))
    val training = splitData(0)
    val testing = splitData(1)

    println(training.count())
    println(testing.count())

    val bkmeans = new BisectingKMeans()
      .setK(6)
      .setMaxIter(65)
      .setSeed(1)

    val bisectingModel = bkmeans.fit(training)
    println("Parameters:")
    println(bisectingModel.explainParams())

    val cost = bisectingModel.computeCost(training)
    println("Sum of Squared Errors = " + cost)

    println("Cluster Centers:")
    val centers = bisectingModel.clusterCenters
    centers.foreach(println)

    val predictions = bisectingModel.transform(testing)
    predictions.show(false)

    spark.stop()
  }
}
