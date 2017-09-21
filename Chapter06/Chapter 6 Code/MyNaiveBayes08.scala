package spark.ml.cookbook.chapter6

/**
 * Created by Siamak Amirghodsi on 3/13/2016.
 */

// myNaiveBayes08 - recipe07

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics, MultilabelMetrics, binary}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object MyNaiveBayes08 {

  def main(args: Array[String]): Unit = {



    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Create a configuration object and set parameters(spark mode, application name and a home directory
//    val conf = new SparkConf().setMaster("local[*]").setAppName("myNaiveBayes08").setSparkHome("C:\\spark-1.5.2-bin-hadoop2.6")
    // Create Spark Context so we can interact with the custer
    // Use the configuration object you defined above
//    val sc = new SparkContext(conf)
    // Create a SQL Context to provide interface to DataFrame
//    val sqlContext = new SQLContext(sc)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("myNaiveBayes08")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    val data = spark.sparkContext.textFile("../data/sparkml2/chapter6/iris.data.prepared.txt")
    val NaiveBayesDataSet = data.map { line =>
      val columns = line.split(',')

      LabeledPoint(columns(4).toDouble , Vectors.dense(columns(0).toDouble,columns(1).toDouble,columns(2).toDouble,columns(3).toDouble ))

    }

    println(" Total number of data vectors =", NaiveBayesDataSet.count())
    val distinctNaiveBayesData = NaiveBayesDataSet.distinct()
    println("Distinct number of data vectors = ", distinctNaiveBayesData.count())

    printf("---------------------------------------------")
    distinctNaiveBayesData.collect().take(10).foreach(println(_))
    printf("---------------------------------------------")

    val allDistinctData = distinctNaiveBayesData.randomSplit(Array(.30,.70),13L)
    val trainingDataSet = allDistinctData(0)
    val testingDataSet = allDistinctData(1)


    println("number of training data =",trainingDataSet.count())
    println("number of test data =",testingDataSet.count())

    val myNaiveBayesModel = NaiveBayes.train(trainingDataSet)

    val predictedClassification = testingDataSet.map( x => (myNaiveBayesModel.predict(x.features), x.label))

    predictedClassification.collect().foreach(println(_))

    val metrics = new MulticlassMetrics(predictedClassification)


    val confusionMatrix = metrics.confusionMatrix
    println("Confusion Matrix= \n",confusionMatrix)

    val myModelStat=Seq(metrics.precision,metrics.fMeasure,metrics.recall)
    myModelStat.foreach(println(_))


  } // end of myNaiveBayes

} // end of object
