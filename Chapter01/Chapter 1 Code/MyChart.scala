package spark.ml.cookbook.chapter1

import java.awt.Color

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.jfree.chart.plot.{PlotOrientation, XYPlot}
import org.jfree.chart.{ChartFactory, ChartFrame, JFreeChart}
import org.jfree.data.xy.{XYSeries, XYSeriesCollection}

import scala.util.Random


object MyChart {

  def show(chart: JFreeChart) {
    val frame = new ChartFrame("plot", chart)
    frame.pack()
    frame.setVisible(true)
  }

  def configurePlot(plot: XYPlot): Unit = {
    plot.setBackgroundPaint(Color.WHITE)
    plot.setDomainGridlinePaint(Color.BLACK)
    plot.setRangeGridlinePaint(Color.BLACK)
    plot.setOutlineVisible(false)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // setup SparkSession to use for interactions with Spark
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("myChart")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()


    val data = spark.sparkContext.parallelize(Random.shuffle(1 to 15).zipWithIndex)

    data.foreach(println)

    val xy = new XYSeries("")
    data.collect().foreach{ case (y: Int, x: Int) => xy.add(x,y) }
    val dataset = new XYSeriesCollection(xy)

    val chart = ChartFactory.createXYLineChart(
      "MyChart",  // chart title
      "x",               // x axis label
      "y",                   // y axis label
      dataset,                   // data
      PlotOrientation.VERTICAL,
      false,                    // include legend
      true,                     // tooltips
      false                     // urls
    )

    val plot = chart.getXYPlot()
    configurePlot(plot)
    show(chart)
    spark.stop()
  }
}

