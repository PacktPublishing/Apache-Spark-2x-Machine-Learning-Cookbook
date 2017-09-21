
package spark.ml.cookbook.chapter8

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.GaussianMixture
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession


/*
* the data set is a simulated knee pain data set available at
* http://wiki.stat.ucla.edu/socr/index.php/SOCR_Data_KneePainData_041409
*/


object MyGaussianMixture {
  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)

    // setup SparkSession to use for interactions with Spark
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("myGaussianMixture")
      .config("spark.sql.warehouse.dir",  ".")
      .getOrCreate()


      val dataFile ="../data/sparkml2/chapter8/socr_data.txt"

      val trainingData = spark.sparkContext.textFile(dataFile).map { line =>
        Vectors.dense(line.trim.split(' ').map(_.toDouble))
      }.cache()

      val myGM = new GaussianMixture()
            .setK(4 )   // default value is 2, LF, LB, RF, RB
            .setConvergenceTol(0.01)     // using the default value
            .setMaxIterations(100)    // max 100 iteration

      val model = myGM.run(trainingData)


      println("Model ConvergenceTol: "+ myGM.getConvergenceTol)
      println("Model k:"+myGM.getK)
      println("maxIteration:"+myGM.getMaxIterations)

      for (i <- 0 until model.k) {
        println("weight=%f\nmu=%s\nsigma=\n%s\n" format
          (model.weights(i), model.gaussians(i).mu, model.gaussians(i).sigma))
      }

      println("Cluster labels (first <= 50):")
      val clusterLabels = model.predict(trainingData)
      clusterLabels.take(50).foreach { x =>
        print(" " + x)
      }
      println()
      spark.stop()
    }

}

