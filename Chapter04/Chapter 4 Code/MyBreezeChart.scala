package  spark.ml.cookbook.chapter4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import breeze.plot._

import scala.util.Random


object MyBreezeChart {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("myBreezeChart")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    import spark.implicits._

    val fig = Figure()
    val chart = fig.subplot(0)

    chart.title = "My Breeze-Viz Chart"
    chart.xlim(21,100)
    chart.ylim(0,100000)

    val ages = spark.createDataset(Random.shuffle(21 to 100).toList.take(45)).as[Int]

    ages.show(false)
    val x = ages.collect()
    val y = Random.shuffle(20000 to 100000).toList.take(45)

    val x2 = ages.collect().map(xx => xx.toDouble)
    val y2 = x2.map(xx => (1000 * xx) + (xx * 2))

    chart += scatter(x, y, _ => 0.5)
    chart += plot(x2, y2)

    chart.xlabel = "Age"
    chart.ylabel = "Income"

    fig.refresh()

    spark.stop()
  }
}
