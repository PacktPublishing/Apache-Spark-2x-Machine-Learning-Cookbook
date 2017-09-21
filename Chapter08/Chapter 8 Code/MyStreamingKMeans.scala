package spark.ml.cookbook.chapter8

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MyStreamingKMeans {

  def main(args: Array[String]) {

    val trainingDir = "../data/sparkml2/chapter8/trainingDir"
    val testDir = "../data/sparkml2/chapter8/testDir"
    val batchDuration = 10
    val numClusters = 2
    val numDimensions = 3


    Logger.getLogger("org").setLevel(Level.ERROR)

    // setup SparkSession to use for interactions with Spark
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("myStreamingKMeans")
      .config("spark.sql.warehouse.dir",  ".")
      .getOrCreate()


    val ssc = new StreamingContext(spark.sparkContext, Seconds(batchDuration.toLong))

    val trainingData = ssc.textFileStream(trainingDir).map(Vectors.parse)
    val testData = ssc.textFileStream(testDir).map(LabeledPoint.parse)

    val model = new StreamingKMeans()
      .setK(numClusters)
      .setDecayFactor(1.0)
      .setRandomCenters(numDimensions, 0.0)

    model.trainOn(trainingData)
    model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println
