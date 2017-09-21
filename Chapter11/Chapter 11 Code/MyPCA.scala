
package spark.ml.cookbook.chapter11


import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession


object MyPCA {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("MyPCA")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    val dataFile  = "../data/sparkml2/chapter11/processed.cleveland.data"
    val rawdata = spark.sparkContext.textFile(dataFile).map(_.trim)
    println(rawdata.count())
    val data = rawdata.filter(text => !(text.isEmpty || text.indexOf("?") > -1))
      .map { line =>
      val values = line.split(',').map(_.toDouble)
      Vectors.dense(values)
    }

    println(data.count())
    data.take(2).foreach(println)
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(4)
      .fit(df)
    val pcaDF = pca.transform(df)
    val result = pcaDF.select("pcaFeatures")
    result.show(false)

    spark.stop()
  }
}

