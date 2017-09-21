package spark.ml.cookbook.chapter6

/**
 * Created by Siamak Amirghodsi on 3/13/2016.
 */

// SVMwithSGD - mySVM07.scala

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MultilabelMetrics, binary}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level


object MySVM07 {

  def main(args: Array[String]): Unit = {



    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Create a configuration object and set parameters(spark mode, application name and a home directory
//    val conf = new SparkConf().setMaster("local[*]").setAppName("mySVM07").setSparkHome("C:\\spark-1.5.2-bin-hadoop2.6")
    // Create Spark Context so we can interact with the custer
    // Use the configuration object you defined above
//    val sc = new SparkContext(conf)
    // Create a SQL Context to provide interface to DataFrame
//    val sqlContext = new SQLContext(sc)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("mySVM07")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    val dataSetSVM = MLUtils.loadLibSVMFile(spark.sparkContext,"../data/sparkml2/chapter6/sample_libsvm_data.txt")

    println("Top 10 rows of LibSVM data")
    dataSetSVM.collect().take(10).foreach(println(_))

    println(" Total number of data vectors =", dataSetSVM.count())
    val distinctData = dataSetSVM.distinct().count()
    println("Distinct number of data vectors = ", distinctData)

    // split the training data into train and test with 30% for train and 70% for actual
    val trainingSetRatio = .20
    val populationTestSetRatio = .80
    val splitRatio = Array(trainingSetRatio, populationTestSetRatio)
    val allDataSVM = dataSetSVM.randomSplit(splitRatio)

    // train the model
    val numIterations = 100
    val myModelSVM = SVMWithSGD.train(allDataSVM(0), numIterations,1,1)

    val predictedClassification = allDataSVM(1).map( x => (myModelSVM.predict(x.features), x.label))

    predictedClassification.collect().foreach(println(_))

    //predictedClassification.collect()foreach(println(_))

    //Quick and dirty way to get a feel for accuracy
    val falsePredictions = predictedClassification.filter(p => p._1 != p._2)
    println(allDataSVM(0).count())
    println(allDataSVM(1).count())
    println(predictedClassification.count())
    println(falsePredictions.count())

    // more systematic to think about accuracy of classification
    // receiver operating characteristic (ROC),
    val metrics = new BinaryClassificationMetrics(predictedClassification)
    val areaUnderROCValue = metrics.areaUnderROC()

    println("The area under ROC curve = ", areaUnderROCValue)

    spark.stop()
  } // end of main

} // end of SVM
