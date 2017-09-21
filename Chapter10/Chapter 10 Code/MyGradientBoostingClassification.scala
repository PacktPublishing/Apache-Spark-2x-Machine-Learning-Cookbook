
package spark.ml.cookbook.chapter10

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.sql.SparkSession


object MyGradientBoostingClassification {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("MyGradientBoostedTreesClassification")
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


    val algo = "Classification"
    val numIterations = 3
    val numClasses = 2
    val maxDepth   = 5
    val maxBins  = 32
    val categoricalFeatureInfo = Map[Int,Int]()


    val boostingStrategy = BoostingStrategy.defaultParams(algo)

    boostingStrategy.setNumIterations(numIterations)
    boostingStrategy.treeStrategy.setNumClasses(numClasses)
    boostingStrategy.treeStrategy.setMaxDepth(maxDepth)
    boostingStrategy.treeStrategy.setMaxBins(maxBins)
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = categoricalFeatureInfo

    evaluate(trainingData, testData, boostingStrategy)
    println("===================")

    spark.stop()
  }

  def evaluate(
                trainingData: RDD[LabeledPoint],
                testData: RDD[LabeledPoint],
                boostingStrategy : BoostingStrategy
                ) :Unit = {



    val model = GradientBoostedTrees.train(trainingData, boostingStrategy)

    val metrics = getMetrics(model, testData)
    println("Confusion Matrix :")
    println(metrics.confusionMatrix)
    println("Model Accuracy: "+metrics.precision)
    println("Model Error: "+ (1-metrics.precision))
//    (0 until boostingStrategy.treeStrategy.getNumClasses()).map(
//      category => (metrics.precision(category), metrics.recall(category))
//    ).foreach(println)
//    println("My Classification GBT model:\n" + model.toDebugString)
  }

  def getMetrics(model: GradientBoostedTreesModel, data: RDD[LabeledPoint]): MulticlassMetrics = {
    val predictionsAndLabels = data.map(example =>
      (model.predict(example.features), example.label)
    )
    new MulticlassMetrics(predictionsAndLabels)
  }
}



