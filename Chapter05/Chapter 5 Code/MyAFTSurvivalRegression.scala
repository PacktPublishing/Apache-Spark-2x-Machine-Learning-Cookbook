package spark.ml.cookbook.chapter5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.AFTSurvivalRegression
import org.apache.spark.sql.SparkSession

object MyAFTSurvivalRegression {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[4]")
      .appName("myAFTSurvivalRegression")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

//    ID,time,age,drug,censor,entdate,enddate
//    1,5,46,0,1,5/15/1990,10/14/1990
    val file = spark.sparkContext.textFile("../data/sparkml2/chapter5/hmohiv.csv")
    val headerAndData = file.map(line => line.split(",").map(_.trim))
    val header = headerAndData.first
    val rawData = headerAndData.filter(_(0) != header(0))
    val df = spark.createDataFrame(rawData
      .map { line =>
        val id = line(0).toDouble
        val time =line(1).toDouble
        val age = line(2).toDouble
        val censor = line(4).toDouble
        (id, censor,Vectors.dense(time,age))
      }).toDF("label", "censor", "features")
    df.show()



    val aft = new AFTSurvivalRegression()
      .setQuantileProbabilities(Array(0.3, 0.6))
      .setQuantilesCol("quantiles")

    val aftmodel = aft.fit(df)

    // Print the coefficients, intercept and scale parameter for AFT survival regression
    println(s"Coefficients: ${aftmodel.coefficients} ")
    println(s"Intercept: ${aftmodel.intercept}" )
    println(s"Scale: ${aftmodel.scale}")

    aftmodel.transform(df).show(false)

    spark.stop()
  }
}

