/**
  * Created by bhall on 6/6/16.
  */
package spark.ml.cookbook.chapter13

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.StreamingKMeans

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.Queue


object KMeansStreaming {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("KMean Streaming App")
      .config("spark.sql.warehouse.dir", ".")
      .config("spark.executor.memory", "2g")
      .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

    Logger.getRootLogger.setLevel(Level.WARN)

    val irisData = IrisData.readFromFile(spark.sparkContext)
    val lookup = IrisData.buildLabelLookup(irisData)

    val trainQueue = new Queue[RDD[LabeledPoint]]()
    val testQueue = new Queue[RDD[LabeledPoint]]()

    val trainingStream = ssc.queueStream(trainQueue)
    val testStream = ssc.queueStream(testQueue)

    val model = new StreamingKMeans().setK(3)
      .setDecayFactor(1.0)
      .setRandomCenters(4, 0.0)

    model.trainOn(trainingStream.map(lp => lp.features))
    val values = model.predictOnValues(testStream.map(lp => (lp.label, lp.features)))
    values.foreachRDD(n => n.foreach(v => {
      println(v._2, v._1, lookup(v._1.toLong))
    }))

    ssc.start()

    val irisLabelPoints = irisData.map(record => IrisData.toLabelPoints(record))
    val Array(trainData, test) = irisLabelPoints.randomSplit(Array(.80, .20))

    trainQueue +=  irisLabelPoints
    Thread.sleep(2000)

    val testGroups = test.randomSplit(Array(.25, .25, .25, .25))
    testGroups.foreach(group => {
        testQueue += group
        println("-" * 25)
        Thread.sleep(1000)
    })

    ssc.stop()
  }
}