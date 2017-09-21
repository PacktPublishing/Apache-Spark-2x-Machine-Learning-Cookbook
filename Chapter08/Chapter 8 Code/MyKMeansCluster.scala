
package spark.ml.cookbook.chapter8


import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.SparkSession


object MyKMeansCluster {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)

    // setup SparkSession to use for interactions with Spark
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("myKMeansCluster")
      .config("spark.sql.warehouse.dir",  ".")
      .getOrCreate()


    val trainingData = spark.read.format("libsvm").load("../data/sparkml2/chapter8/my_kmeans_data.txt")

    trainingData.show()


    // Trains a k-means model
    val kmeans = new KMeans()
      .setK(3)   // default value is 2
      .setFeaturesCol("features")
      .setMaxIter(10)   // default Max Iteration is 20
      .setPredictionCol("prediction")
      .setSeed(1L)
    val model = kmeans.fit(trainingData)

    model.summary.predictions.show()
    // the fit function will run the algo and do calculation, it is based on DataFrame created above.

    println("KMeans Cost:" +model.computeCost(trainingData))   //Within Set Sum of Squared Error (WSSSE)

    println("KMeans Cluster Centers: ")
    model.clusterCenters.foreach(println)
    spark.stop()
  }
}

