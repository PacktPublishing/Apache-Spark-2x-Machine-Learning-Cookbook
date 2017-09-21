package spark.ml.cookbook.chapter6

//myRegress02 - recipe02

import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import scala.math._
//import org.apache.spark.mllib.regression.RidgeRegressionWithSGD
/**
 * Created by Siamak Amirghodsi on 2/28/2016.
 */
object MyRegress02 {
  def main(args: Array[String]): Unit = {

    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Create a configuration object and set parameters(spark mode, application name and a home directory
    // @Todo old 1.6
    //val conf = new SparkConf().setMaster("local[*]").setAppName("myRegress02").setSparkHome("C:\\spark-1.5.2-bin-hadoop2.6")

    // @Todo new 2.0
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("myRegress02")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    //  @Todo new 2.0
    val sc = spark.sparkContext

    // Create Spark Context so we can interact with the custer
    // Use the configuration object you defined above
    // @Todo old 1.6
//    val sc = new SparkContext(conf)
//    // Create a SQL Context to provide interface to DataFrame
//    val sqlContext = new SQLContext(sc)

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


    // Model parameters
    val numIterations = 1000
    val stepsSGD = .001


    //val model = new LinearRegressionWithSGD().setIntercept(true) use of constructor


    val myModel = LinearRegressionWithSGD.train(RegressionDataSet, numIterations,stepsSGD)
    // Evaluate model on training examples and compute training error
    val predictedLabelValue = RegressionDataSet.map { lp => val predictedValue = myModel.predict(lp.features)
      (lp.label, predictedValue)
    }

    printf("****************************************************")
    println("Intercept set:",myModel.intercept)
    println("Model Weights:",myModel.weights)
    printf("****************************************************")

    predictedLabelValue.takeSample(false,20).foreach(println(_))

    val MSE = predictedLabelValue.map{ case(l, p) => math.pow((l - p), 2)}.reduce(_ + _) / predictedLabelValue.count
    val RMSE = math.sqrt(MSE)

    println("training Mean Squared Error = " + MSE)
    println("training Root Mean Squared Error = " + RMSE)


  } // end of main

} // end of myRegress02
