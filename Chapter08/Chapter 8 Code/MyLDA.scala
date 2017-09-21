package spark.ml.cookbook.chapter8

// scalastyle:off println

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.LDA






object MyLDA {

  def main(args: Array[String]): Unit = {




    Logger.getLogger("org").setLevel(Level.ERROR)

    // setup SparkSession to use for interactions with Spark
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("MyLDA")
      .config("spark.sql.warehouse.dir",  ".")
      .getOrCreate()

    val input = "../data/sparkml2/chapter8/my_lda_data.txt"

    val dataset = spark.read.format("libsvm").load(input)
    dataset.show(5)

    // Trains a LDA model
    val lda = new LDA()
      .setK(5)
      .setMaxIter(10)
      .setFeaturesCol("features")
      .setOptimizer("online")
      .setOptimizeDocConcentration(true)
    val ldaModel = lda.fit(dataset)
    val transformed = ldaModel.transform(dataset)

    val ll = ldaModel.logLikelihood(dataset)
    val lp = ldaModel.logPerplexity(dataset)

    println(s"\t Training data log likelihood: $ll")
    println(s"\t Training data log Perplexity: $lp")
    // describeTopics

    val topics = ldaModel.describeTopics(3)
      // Shows the result
    topics.show(false)
    transformed.show(false)
    transformed.show(true)

    // $example off$
    spark.stop()
  }
}
// scalastyle:on println
