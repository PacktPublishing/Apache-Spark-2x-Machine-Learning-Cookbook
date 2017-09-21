package spark.ml.cookbook.chapter6

// myRegress03 - recipe02 - Ridge regression - no 3

/**
 * Created by Siamak Amirghodsi on 3/12/2016.
 */

import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD, RidgeRegressionWithSGD}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.math._
//import org.apache.spark.mllib.regression.RidgeRegressionWithSGD

object MyRegress03 {

  def main(args: Array[String]): Unit = {



    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // @Todo 1.6
    // Create a configuration object and set parameters(spark mode, application name and a home directory
//    val conf = new SparkConf().setMaster("local[*]").setAppName("myRegress03").setSparkHome("C:\\spark-1.5.2-bin-hadoop2.6")
    // Create Spark Context so we can interact with the custer
    // Use the configuration object you defined above
//    val sc = new SparkContext(conf)
    // Create a SQL Context to provide interface to DataFrame
//    val sqlContext = new SQLContext(sc)

    // @Todo new 2.0
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("myRegress03")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    //  @Todo new 2.0
    val sc = spark.sparkContext

    //val data = sc.textFile("C:/mydocs/housing5.csv")
    val data = sc.textFile("../data/sparkml2/chapter6/housing8.csv")
    val RegressionDataSet = data.map { line =>
      val columns = line.split(',')

      LabeledPoint(columns(13).toDouble , Vectors.dense(columns(0).toDouble,columns(1).toDouble, columns(2).toDouble, columns(3).toDouble,columns(4).toDouble,
        columns(5).toDouble,columns(6).toDouble, columns(7).toDouble
      ))

    }

    printf("---------------------------------------------")
    RegressionDataSet.collect().foreach(println(_))
    printf("---------------------------------------------")


    // Ridge regression Model parameters
    val numIterations = 1000
    val stepsSGD = .001
    val regularizationParam = 1.13

    //val model = new LinearRegressionWithSGD().setIntercept(true) use of constructor


    val myRidgeModel = RidgeRegressionWithSGD.train(RegressionDataSet, numIterations,stepsSGD, regularizationParam)

    val predictedLabelValue = RegressionDataSet.map { lp => val predictedValue = myRidgeModel.predict(lp.features)
      (lp.label, predictedValue)
    }

    printf("****************************************************")
    println("Intercept set:",myRidgeModel.intercept)
    println("Model Weights:",myRidgeModel.weights)
    printf("****************************************************")

    predictedLabelValue.takeSample(false,20).foreach(println(_))

    val MSE = predictedLabelValue.map{ case(l, p) => math.pow((l - p), 2)}.reduce(_ + _) / predictedLabelValue.count
    val RMSE = math.sqrt(MSE)

    println("training Mean Squared Error = " + MSE)
    println("training Root Mean Squared Error = " + RMSE)


  } // end of main


}
