package spark.ml.cookbook.chapter5

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object OnevsRest {

  def main(args: Array[String]): Unit = {

    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("MLP")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    val data = spark.read.format("libsvm")
      .load("../data/sparkml2/chapter5/iris.scale.txt")

    data.show(false)

    val Array(train, test) = data.randomSplit(Array(0.8, 0.2), seed = System.currentTimeMillis())

    // logistic regression classifier
    val lrc = new LogisticRegression()
      .setMaxIter(15)
      .setTol(1E-3)
      .setFitIntercept(true)

    val ovr = new OneVsRest().setClassifier(lrc)

    val ovrModel = ovr.fit(train)

    val predictions = ovrModel.transform(test)
    predictions.show(false)

    val eval = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")

    // compute the classification error on test data.
    val accuracy = eval.evaluate(predictions)
    println("Accuracy: " + eval.evaluate(predictions))
  }
}
