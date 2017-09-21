
package spark.ml.cookbook.chapter10


import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MyDecisionTreeClassification {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("MyDecisionTreeClassification")
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
    println(rawData.count())
    println(data.count())


    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))


    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val maxDepth = 5
    val maxBins = 32

    evaluate(trainingData, testData, numClasses, categoricalFeaturesInfo,
              "gini", maxDepth, maxBins)

    evaluate(trainingData, testData, numClasses, categoricalFeaturesInfo,
              "entropy", maxDepth, maxBins)

    spark.stop()

  }
  def evaluate(
                 trainingData: RDD[LabeledPoint],
                 testData: RDD[LabeledPoint],
                 numClasses: Int,
                 categoricalFeaturesInfo: Map[Int,Int],

                 impurity: String,
                 maxDepth: Int,
                 maxBins:Int
                 ) :Unit = {


    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)
    val metrics = getMetrics(model, testData)
    println("Using Impurity :"+ impurity)
    println("Confusion Matrix :")
    println(metrics.confusionMatrix)
    println("Decision Tree Accuracy: "+metrics.precision)
    println("Decision Tree Error: "+ (1-metrics.precision))
    (0 until numClasses).map(
      category => (metrics.precision(category), metrics.recall(category))
    ).foreach(println)
  }

  def getMetrics(model: DecisionTreeModel, data: RDD[LabeledPoint]): MulticlassMetrics = {
    val predictionsAndLabels = data.map(example =>
      (model.predict(example.features), example.label)
    )
    new MulticlassMetrics(predictionsAndLabels)
  }


}

