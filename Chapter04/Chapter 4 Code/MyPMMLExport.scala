
package spark.ml.cookbook.chapter4

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.clustering.KMeans



object MyPMMLExport {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .master("local[*]")   // if use cluster master("spark://master:7077")
      .appName("myPMMLExport")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    val data = spark.sparkContext.textFile("../data/sparkml2/chapter4/my_kmeans_data_sample.txt")

    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()


    val numClusters = 2
    val numIterations = 10
    val model = KMeans.train(parsedData, numClusters, numIterations)

    println("MyKMeans PMML Model:\n" + model.toPMML)

    model.toPMML("../data/sparkml2/chapter4/myKMeansSamplePMML.xml")


   // model.toPMML(spark.sparkContext, "../data/kmeans")
   // model.toPMML(System.out)
    spark.stop()
  }
}

