


package spark.ml.cookbook.chapter8


import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.PowerIterationClustering
import org.apache.spark.sql.SparkSession



object MyPowerIterationClustering {

  def main(args: Array[String]) {


    Logger.getLogger("org").setLevel(Level.ERROR)

    // setup SparkSession to use for interactions with Spark
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("myPowerIterationClustering")
      .config("spark.sql.warehouse.dir",  ".")
      .getOrCreate()

    val trainingData =spark.sparkContext.parallelize(List(
      (0L, 1L, 1.0),
      (0L, 2L, 1.0),
      (0L, 3L, 1.0),
      (1L, 2L, 1.0),
      (1L, 3L, 1.0),
      (2L, 3L, 1.0),
      (3L, 4L, 0.1),
      (4L, 5L, 1.0),
      (4L, 15L, 1.0),
      (5L, 6L, 1.0),
      (6L, 7L, 1.0),
      (7L, 8L, 1.0),
      (8L, 9L, 1.0),
      (9L, 10L, 1.0),
      (10L,11L, 1.0),
      (11L, 12L, 1.0),
      (12L, 13L, 1.0),
      (13L,14L, 1.0),
      (14L,15L, 1.0)
    ))

    // Cluster the data into two classes using PowerIterationClustering
    val pic = new PowerIterationClustering()
      .setK(3)
      .setMaxIterations(15)
    val model = pic.run(trainingData)
    model.assignments.foreach { a =>
      println(s"${a.id} -> ${a.cluster}")
    }

    val clusters = model.assignments.collect().groupBy(_.cluster).mapValues(_.map(_.id))
    val assignments = clusters.toList.sortBy { case (k, v) => v.length }
    val assignmentsStr = assignments
      .map { case (k, v) =>
      s"$k -> ${v.sorted.mkString("[", ",", "]")}"
    }.mkString(", ")
    val sizesStr = assignments.map {
      _._2.length
    }.sorted.mkString("(", ",", ")")
    println(s"Cluster assignments: $assignmentsStr\ncluster sizes: $sizesStr")

    spark.stop()
  }

}

