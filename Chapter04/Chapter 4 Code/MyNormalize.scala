package spark.ml.cookbook.chapter4

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.log4j.{Level, Logger}

object MyNormalize  {

  def parseWine(str: String): (Int, Vector) = {
    val columns = str.split(",")
    // don't use the entire row of data
    (columns(0).toInt, Vectors.dense(columns(1).toFloat, columns(2).toFloat, columns(3).toFloat))
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // setup SparkSession to use for interactions with Spark
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("My Normalize")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    import spark.implicits._

    //http://archive.ics.uci.edu/ml/machine-learning-databases/wine/wine.data
    val data = spark.read.text("../data/sparkml2/chapter4/wine.data").as[String].map(parseWine)
    val df = data.toDF("id", "feature")

    df.printSchema()
    df.show(false)

    val scale = new MinMaxScaler()
      .setInputCol("feature")
      .setOutputCol("scaled")
      .setMax(1)
      .setMin(-1)

    scale.fit(df).transform(df).select("scaled").show(false)

    spark.stop()
  }
}