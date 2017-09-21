package  spark.ml.cookbook.chapter4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

case class Beatle(id: Long, name: String)

object DatasetvsRDD {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
//    Logger.getLogger("akka").setLevel(Level.ERROR)

    // setup SparkSession to use for interactions with Spark
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("DatasetvsRDD")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    import spark.implicits._

    val ds = spark.read.textFile("../data/sparkml2/chapter4/beatles.txt").map(line => {
      val tokens = line.split(",")
      Beatle(tokens(0).toLong, tokens(1))
    }).as[Beatle]

    println("Dataset Type: " + ds.getClass)
    ds.show()

    val rdd = spark.sparkContext.textFile("../data/sparkml2/chapter4/beatles.txt").map(line => {
      val tokens = line.split(",")
      Beatle(tokens(0).toLong, tokens(1))
    })

    println("RDD Type: " + rdd.getClass)
    rdd.collect().foreach(println)

    val df = spark.read.text("../data/sparkml2/chapter4/beatles.txt").map(
      row => { // Dataset[Row]
        val tokens = row.getString(0).split(",")
        Beatle(tokens(0).toLong, tokens(1))
    }).toDF("bid", "bname")

    println("DataFrame Type: " + df.getClass)
    df.show()

    spark.stop()
  }
}
