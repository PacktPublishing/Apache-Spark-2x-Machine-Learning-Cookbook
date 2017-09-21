
package spark.ml.cookbook.chapter10

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.sql.SparkSession



object MyGradientBoostingRegression {
  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("MyGradientBoostedTreesRegression")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    val rawData = spark.sparkContext.textFile("../data/sparkml2/chapter10/breast-cancer-wisconsin.data")
    val data = rawData.map(_.trim)
      .filter(text => !(text.isEmpty || text.startsWith("#") || text.indexOf("?") > -1))
      .map { line =>
        val values = line.split(',').map(_.toDouble)
        val slicedValues = values.slice(1, values.size)
        val featureVector = Vectors.dense(slicedValues.init)
        val label = values.last / 2 -1
        LabeledPoint(label, featureVector)

      }

    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))


    println("Training Data count:"+trainingData.count())
    println("Test Data Count:"+testData.count())

    val algo = "Regression"
    val numIterations = 3
    val maxDepth   = 5
    val maxBins  = 32
    val categoricalFeatureInfo = Map[Int,Int]()


    val boostingStrategy = BoostingStrategy.defaultParams(algo)

    boostingStrategy.setNumIterations(numIterations)

    boostingStrategy.treeStrategy.setMaxDepth(maxDepth)
    boostingStrategy.treeStrategy.setMaxBins(maxBins)

    boostingStrategy.treeStrategy.categoricalFeaturesInfo = categoricalFeatureInfo

    val model = GradientBoostedTrees.train(trainingData, boostingStrategy)


    val metrics = getMetrics(model, testData)

    println("Test Mean Squared Error = " + metrics.meanSquaredError)
    println("My regression GBT model:\n" + model.toDebugString)
    spark.stop()
  }
  def getMetrics(model: GradientBoostedTreesModel, data: RDD[LabeledPoint]): RegressionMetrics = {
    val predictionsAndLabels = data.map(example =>
      (model.predict(example.features), example.label)
    )
    new RegressionMetrics(predictionsAndLabels)
  }
}

