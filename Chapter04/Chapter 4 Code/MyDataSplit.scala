package spark.ml.cookbook.chapter4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MyDataSplit {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // setup SparkSession to use for interactions with Spark
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Data Splitting")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    // http://archive.ics.uci.edu/ml/machine-learning-databases/00359/NewsAggregatorDataset.zip
    val data = spark.read.csv("../data/sparkml2/chapter4/newsCorpora.csv")

    val rowCount = data.count()
    println("Original RowCount=" + rowCount)

    val splitData = data.randomSplit(Array(0.8, 0.2))

    val trainingSet = splitData(0)
    val testSet = splitData(1)

    val trainingSetCount = trainingSet.count()
    val testSetCount = testSet.count()

    println("trainingSet RowCount=" + trainingSetCount)
    println("testSet RowCount=" + testSetCount)
    println("Combined RowCount=" + (trainingSetCount+testSetCount))

    spark.stop()
  }
}