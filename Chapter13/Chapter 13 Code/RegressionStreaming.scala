package spark.ml.cookbook.chapter13

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.Queue


object RegressionStreaming {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Regression Streaming App")
      .config("spark.sql.warehouse.dir", ".")
      .config("spark.executor.memory", "2g")
      .getOrCreate()


    import spark.implicits._

    val ssc = new StreamingContext(spark.sparkContext, Seconds(2))

    Logger.getRootLogger.setLevel(Level.WARN)


    val rawDF = spark.read
      .format("com.databricks.spark.csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .load("../data/sparkml2/chapter13/winequality-white.csv")

    val rdd = rawDF.rdd.zipWithUniqueId()

    rdd.collect().foreach(println)

    val lookupQuality = rdd.map{ case (r: Row, id: Long)=> (id, r.getInt(11))}.collect().toMap
    lookupQuality.foreach(println)

    val d = rdd.map{ case (r: Row, id: Long)=> LabeledPoint(id,
      Vectors.dense(r.getDouble(0), r.getDouble(1), r.getDouble(2), r.getDouble(3), r.getDouble(4),
          r.getDouble(5), r.getDouble(6), r.getDouble(7), r.getDouble(8), r.getDouble(9), r.getDouble(10))
    )}

    rdd.collect().foreach(println)

    val trainQueue = new Queue[RDD[LabeledPoint]]()
    val testQueue = new Queue[RDD[LabeledPoint]]()

    val trainingStream = ssc.queueStream(trainQueue)
    val testStream = ssc.queueStream(testQueue)

    val numFeatures = 11
    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.zeros(numFeatures))
      .setNumIterations(25)
        .setStepSize(0.1)
      .setMiniBatchFraction(0.25)

    model.trainOn(trainingStream)
    val result = model.predictOnValues(testStream.map(lp => (lp.label, lp.features)))
    result.map{ case (id: Double, prediction: Double) =>  (id, prediction, lookupQuality(id.asInstanceOf[Long])) }.print()

    ssc.start()


    val Array(trainData, test) = d.randomSplit(Array(.80, .20))

    trainQueue +=  trainData
    Thread.sleep(4000)

    val testGroups = test.randomSplit(Array(.50, .50))
    testGroups.foreach(group => {
      testQueue += group
      //println("-" * 25)

      Thread.sleep(2000)
    })

    ssc.stop()
  }
}
