package spark.ml.cookbook.chapter5

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SparkSession

object MLP {

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

    // Split data
    val splitData = data.randomSplit(Array(0.8, 0.2), seed = System.currentTimeMillis())
    val train = splitData(0)
    val test = splitData(1)

    // specify layers for the neural network:
    // input layer of size 4 (features), and output of size 4 (classes)
    val layers = Array[Int](4, 5, 4)

    val mlp = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(110)
      .setSeed(System.currentTimeMillis())
      .setMaxIter(145)

    val mlpModel = mlp.fit(train)

    val result = mlpModel.transform(test)
    result.show(false)

    val predictions = result.select("prediction", "label")

    val eval = new MulticlassClassificationEvaluator().setMetricName("accuracy")
    println("Accuracy: " + eval.evaluate(predictions))
  }
}
