package spark.ml.cookbook.chapter4

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object MyPipeLine {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // setup SparkSession to use for interactions with Spark
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("My PipeLine")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    val trainset = spark.createDataFrame(Seq(
      (1L, 1, "spark rocks"),
      (2L, 0, "flink is the best"),
      (3L, 1, "Spark rules"),
      (4L, 0, "mapreduce forever"),
      (5L, 0, "Kafka is great")
    )).toDF("id", "label", "words")

    val tokenizer = new Tokenizer()
      .setInputCol("words")
      .setOutputCol("tokens")

    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")

    val lr = new LogisticRegression()
      .setMaxIter(15)
      .setRegParam(0.01)

    // three stage pipeline
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))

    // Fit the pipeline to training documents.
    val model = pipeline.fit(trainset)

    val testSet = spark.createDataFrame(Seq(
      (10L, 1, "use spark please"),
      (11L, 2, "Kafka")
    )).toDF("id", "label", "words")

    model.transform(testSet).select("probability","prediction").show(false)

    spark.stop()
  }
}