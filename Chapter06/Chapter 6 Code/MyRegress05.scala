package spark.ml.cookbook.chapter6

// myRegress05 - recipe04 - Logistic Regression - 3rd

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LassoWithSGD}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.log4j.Logger
import org.apache.log4j.Level
/**
 * Created by Siamak Amirghodsi on 3/12/2016.
 */
object MyRegress05 {

  def main(args: Array[String]): Unit = {



    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Create a configuration object and set parameters(spark mode, application name and a home directory
    //val conf = new SparkConf().setMaster("local[*]").setAppName("myRegress05").setSparkHome("C:\\spark-1.5.2-bin-hadoop2.6")
    // Create Spark Context so we can interact with the custer
    // Use the configuration object you defined above
    //val sc = new SparkContext(conf)
    // Create a SQL Context to provide interface to DataFrame
    //val sqlContext = new SQLContext(sc)


    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("myRegress05")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    val data = spark.sparkContext.textFile("../data/sparkml2/chapter6/admission1.csv")
    val RegressionDataSet = data.map { line =>
      val columns = line.split(',')
      LabeledPoint(columns(0).toDouble , Vectors.dense(columns(1).toDouble,columns(2).toDouble, columns(3).toDouble ))
    }

    printf("---------------------------------------------")
    RegressionDataSet.collect().foreach(println(_))
    printf("---------------------------------------------")



    // Lasso regression Model parameters


    val numIterations = 100
    val stepsSGD = .00001
    val regularizationParam = .05 //.001


    val myLogisticSGDModel = LogisticRegressionWithSGD.train(RegressionDataSet, numIterations,stepsSGD, regularizationParam)
    val predictedLabelValue1 = RegressionDataSet.map { lp => val predictedValue =  myLogisticSGDModel.predict(lp.features)
      (lp.label, predictedValue)
    }



    printf("****************************************************")
    println("Intercept set:", myLogisticSGDModel.intercept)
    println("Model Weights:", myLogisticSGDModel.weights)
    printf("****************************************************")

    predictedLabelValue1.takeSample(false,20).foreach(println(_))

    val MSE = predictedLabelValue1.map{ case(l, p) => math.pow((l - p), 2)}.reduce(_ + _) / predictedLabelValue1.count
    val RMSE = math.sqrt(MSE)

    println("training Mean Squared Error = " + MSE)
    println("training Root Mean Squared Error = " + RMSE)



    spark.stop()
  } // end of main

}
