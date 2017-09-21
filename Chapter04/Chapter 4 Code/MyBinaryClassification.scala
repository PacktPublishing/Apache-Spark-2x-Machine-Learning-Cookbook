package spark.ml.cookbook.chapter4

import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

object MyBinaryClassification {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("myBinaryClassification")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()


    // Load training data in LIBSVM format
    //https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary.html
    val data = MLUtils.loadLibSVMFile(spark.sparkContext, "../data/sparkml2/chapter4/myBinaryClassificationData.txt")

    // Split data into training (60%) and test (40%)
    val Array(training, test) = data.randomSplit(Array(0.6, 0.4), seed = 11L)
     training.cache()

    // Run training algorithm to build the model
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(2)
      .run(training)

    // Clear the prediction threshold so the model will return probabilities
    model.clearThreshold

    // Compute raw scores on the test set
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    // Instantiate metrics object
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)

    // Precision by threshold
    val precision = metrics.precisionByThreshold
    precision.foreach { case (t, p) =>
      println(s"Threshold: $t, Precision: $p")
    }

    // Recall by threshold
    val recall = metrics.recallByThreshold
    recall.foreach { case (t, r) =>
      println(s"Threshold: $t, Recall: $r")
    }

    val PRC = metrics.pr

    val f1Score = metrics.fMeasureByThreshold
    f1Score.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 1")
    }

    val beta = 0.5
    val fScore = metrics.fMeasureByThreshold(beta)
    f1Score.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 0.5")
    }

    val auPRC = metrics.areaUnderPR
    println("Area under precision-recall curve = " + auPRC)

    val thresholds = precision.map(_._1)

    val roc = metrics.roc

    val auROC = metrics.areaUnderROC
    println("Area under ROC = " + auROC)

    spark.stop()
  }
}

