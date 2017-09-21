package  spark.ml.cookbook.chapter4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import scala.util.Random


object SessionvsContext {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
//    Logger.getLogger("akka").setLevel(Level.ERROR)

    // setup SparkSession to use for interactions with Spark
    val session = SparkSession
      .builder
      .master("local[*]")
      .appName("SessionContextRDD")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    import session.implicits._

    // SparkContext
    val context = session.sparkContext

    println("SparkContext")

    val rdd1 = context.makeRDD(Random.shuffle(1 to 10).toList)
    rdd1.collect().foreach(println)
    println("-" * 45)

    val rdd2 = context.parallelize(Random.shuffle(20 to 30).toList)
    rdd2.collect().foreach(println)
    println("\n  End of SparkContext> " + ("-" * 45))

    //SparkSession
    println("\n SparkSession")

    val dataset1 = session.range(40, 50)
    dataset1.show()

    val dataset2 = session.createDataset(Random.shuffle(60 to 70).toList)
    dataset2.show()

    // retrieve underlying RDD from Dataset
    val rdd3 = dataset2.rdd
    rdd3.collect().foreach(println)

    // convert rdd to Dataset
    val rdd4 = context.makeRDD(Random.shuffle(80 to 90).toList)
    val dataset3 = session.createDataset(rdd4)
    dataset3.show()

    session.stop()
  }
}
