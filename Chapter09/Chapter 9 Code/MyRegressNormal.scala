package spark.ml.cookbook.chapter9


import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

import org.apache.log4j.{Level, Logger}

/**
  * Created by Siamak Amirghodsi on 2/28/2016.
  */
object MyRegressNormal {
  def main(args: Array[String]): Unit = {



    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("myRegressNormal")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    import spark.implicits._

    val data = spark.read.text("../data/sparkml2/chapter9/housing8.csv").as[String]

    val RegressionDataSet = data.map { line =>
      val columns = line.split(',')

      LabeledPoint(columns(13).toDouble , Vectors.dense(columns(0).toDouble,columns(1).toDouble, columns(2).toDouble, columns(3).toDouble,columns(4).toDouble,
        columns(5).toDouble,columns(6).toDouble, columns(7).toDouble
      ))
    }

    RegressionDataSet.show(false)

    val lr = new LinearRegression()
      .setMaxIter(1000)
      // must be set to zero for "normal" or will generate following error message
      //  Error message -  "Only L2 regularization can be used when normal solver is used."
      .setElasticNetParam(0.0)
      .setRegParam(0.01)
      .setSolver("normal")

    val myModel = lr.fit(RegressionDataSet)

    val summary = myModel.summary
    println("training Mean Squared Error = " + summary.meanSquaredError)
    println("training Root Mean Squared Error = " + summary.rootMeanSquaredError)

  } // end of main
}


