package spark.ml.cookbook.chapter6

// myRegress06 - recipe05

/**
 * Created by Siamak Amirghodsi on 3/13/2016.
 */

// L_BGFS - myRegress06.scala

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object MyRegress06 {

  def main(args: Array[String]): Unit = {



    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Create a configuration object and set parameters(spark mode, application name and a home directory
//    val conf = new SparkConf().setMaster("local[*]").setAppName("myRegress06").setSparkHome("C:\\spark-1.5.2-bin-hadoop2.6")
    // Create Spark Context so we can interact with the custer
    // Use the configuration object you defined above
//    val sc = new SparkContext(conf)
    // Create a SQL Context to provide interface to DataFrame
//    val sqlContext = new SQLContext(sc)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("myRegress06")
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


    // LBFGS create new regression object
    val myLBFGSestimator = new LogisticRegressionWithLBFGS().setIntercept(false)

    // apply the data to model
    val model1 = myLBFGSestimator.run(RegressionDataSet)

    // predict a single applicant on the go
    val singlePredict1 = model1.predict(Vectors.dense(700,3.4, 1))
    println(singlePredict1)
    val singlePredict2 = model1.predict(Vectors.dense(150,3.4, 1))
    println(singlePredict2)

    // predict admission for a  group of new applicant
    val newApplicants=Seq(
      (Vectors.dense(380.0, 3.61, 3.0)),
      (Vectors.dense(660.0, 3.67, 3.0)),
      (Vectors.dense(800.0, 1.3, 1.0)),
      (Vectors.dense(640.0, 3.19, 4.0)),
      (Vectors.dense(520.0, 2.93, 1.0))
    )


    // go through the sequence one by one and predict
    val predictedLabelValue = newApplicants.map { lp => val predictedValue =  model1.predict(lp)
      ( predictedValue)
    }

    //print predicted outcome
    predictedLabelValue.foreach(println(_))


  } // end of main

}
