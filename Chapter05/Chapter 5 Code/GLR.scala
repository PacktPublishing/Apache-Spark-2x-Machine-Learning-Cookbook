package spark.ml.cookbook.chatper5

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.GeneralizedLinearRegression
import org.apache.spark.sql.SparkSession


object GLR {

  def main(args: Array[String]): Unit = {

    import org.apache.log4j.{Level, Logger}

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("GLR")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    import spark.implicits._

    val data = spark.read.textFile("../data/sparkml2/chapter5/housing8.csv").as[String]

    val regressionData = data.map { line =>
      val columns = line.split(',')

      LabeledPoint(columns(13).toDouble , Vectors.dense(columns(0).toDouble,columns(1).toDouble, columns(2).toDouble, columns(3).toDouble,columns(4).toDouble,
        columns(5).toDouble,columns(6).toDouble, columns(7).toDouble
      ))
    }

    regressionData.show(false)

    val glr = new GeneralizedLinearRegression()
      .setMaxIter(1000)
      .setRegParam(0.03)
      .setFamily("gaussian")
      .setLink("identity")

    val glrModel = glr.fit(regressionData)

    val summary = glrModel.summary

    summary.residuals().show()


    println("Residual Degree Of Freedom: " + summary.residualDegreeOfFreedom)
    println("Residual Degree Of Freedom Null: " + summary.residualDegreeOfFreedomNull)
    println("AIC: " + summary.aic)
    println("Dispersion: " + summary.dispersion)
    println("Null Deviance: " + summary.nullDeviance)
    println("Deviance: " +summary.deviance)

    println("p-values: " + summary.pValues.mkString(","))
    println("t-values: " + summary.tValues.mkString(","))
    println("Coefficient Standard Error: " +  summary.coefficientStandardErrors.mkString(","))

    spark.stop()
  }
}
