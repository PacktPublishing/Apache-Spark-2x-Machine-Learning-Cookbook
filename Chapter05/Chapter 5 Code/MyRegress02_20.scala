package spark.ml.cookbook.chapter5

//myRegress02 - recipie02

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Vector, Vectors}

/**
 * Created by Siamak Amirghodsi on 2/28/2016.
 */
object MyRegress02_20 {
  def main(args: Array[String]): Unit = {

    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("myRegress02")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    import spark.implicits._

    val data = spark.read.text("../data/sparkml2/chapter5/housing8.csv").as[String]

    val RegressionDataSet = data.map { line =>
      val columns = line.split(',')

      LabeledPoint(columns(13).toDouble , Vectors.dense(columns(0).toDouble,columns(1).toDouble, columns(2).toDouble, columns(3).toDouble,columns(4).toDouble,
        columns(5).toDouble,columns(6).toDouble, columns(7).toDouble
      ))
    }

    RegressionDataSet.show(false)

    // Model parameters
    val numIterations = 10

    val lr = new LinearRegression()
        .setMaxIter(numIterations)
        .setSolver("l-bfgs")

    val myModel = lr.fit(RegressionDataSet)

    val summary = myModel.summary
    println("training Mean Squared Error = " + summary.meanSquaredError)
    println("training Root Mean Squared Error = " + summary.rootMeanSquaredError)

  } // end of main
} // end of myRegress02
