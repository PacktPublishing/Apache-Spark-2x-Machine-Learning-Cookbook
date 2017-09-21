
package spark.ml.cookbook.chapter10

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.sql.SparkSession

object MyRandomForestClassification {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("MyRandomForestClassification")
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

    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 3 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
//    val impurity = "gini"
    val maxDepth = 4
    val maxBins = 32



    evaluate(trainingData, testData, numClasses,categoricalFeaturesInfo,numTrees,
      featureSubsetStrategy, "gini", maxDepth, maxBins)
    evaluate(trainingData, testData, numClasses,categoricalFeaturesInfo,numTrees,
      featureSubsetStrategy, "entropy", maxDepth, maxBins)
    println("=============")
    spark.stop()

  }

  def evaluate(
                trainingData: RDD[LabeledPoint],
                testData: RDD[LabeledPoint],
                numClasses: Int,
                categoricalFeaturesInfo: Map[Int,Int],

                numTrees: Int,
                featureSubsetStrategy: String,
                impurity: String,
                maxDepth: Int,
                maxBins:Int
                ) :Unit = {


    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
       numTrees, featureSubsetStrategy,impurity,  maxDepth, maxBins)
    val metrics = getMetrics(model, testData)
    println("Using Impurity :"+ impurity)
    println("Confusion Matrix :")
    println(metrics.confusionMatrix)
    println("Model Accuracy: "+metrics.precision)
    println("Model Error: "+ (1-metrics.precision))
//    (0 until numClasses).map(
//      category => (metrics.precision(category), metrics.recall(category))
//    ).foreach(println)
    println("My Random Forest Model:\n" + model.toDebugString)
  }

  def getMetrics(model: RandomForestModel, data: RDD[LabeledPoint]): MulticlassMetrics = {
    val predictionsAndLabels = data.map(example =>
      (model.predict(example.features), example.label)
    )
    new MulticlassMetrics(predictionsAndLabels)
  }
}

