
package spark.ml.cookbook.chapter4

import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.evaluation.MultilabelMetrics
import org.apache.spark.rdd.RDD


object MyMultiLabel {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("myMultilabel")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()


    val data: RDD[(Array[Double], Array[Double])] = spark.sparkContext.parallelize(
      Seq((Array(0.0, 1.0), Array(0.1, 2.0)),
        (Array(0.0, 2.0), Array(0.1, 1.0)),
        (Array.empty[Double], Array(0.0)),
        (Array(2.0), Array(2.0)),
        (Array(2.0, 0.0), Array(2.0, 0.0)),
        (Array(0.0, 1.0, 2.0), Array(0.0, 1.0)),
        (Array(1.0), Array(1.0, 2.0))), 2)

    val metrics = new MultilabelMetrics(data)

    // Summary stats
    println(s"Recall = ${metrics.recall}")
    println(s"Precision = ${metrics.precision}")
    println(s"F1 measure = ${metrics.f1Measure}")
    println(s"Accuracy = ${metrics.accuracy}")

    // Individual label stats
    metrics.labels.foreach(label =>
      println(s"Class $label precision = ${metrics.precision(label)}"))
    metrics.labels.foreach(label => println(s"Class $label recall = ${metrics.recall(label)}"))
    metrics.labels.foreach(label => println(s"Class $label F1-score = ${metrics.f1Measure(label)}"))

    // Micro stats
    println(s"Micro recall = ${metrics.microRecall}")
    println(s"Micro precision = ${metrics.microPrecision}")
    println(s"Micro F1 measure = ${metrics.microF1Measure}")

    // Hamming loss
    println(s"Hamming loss = ${metrics.hammingLoss}")

    // Subset accuracy
    println(s"Subset accuracy = ${metrics.subsetAccuracy}")
    // $example off$
    spark.stop()
  }
}

