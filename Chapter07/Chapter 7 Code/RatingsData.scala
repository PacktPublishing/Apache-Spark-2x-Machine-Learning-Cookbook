package spark.ml.cookbook.chapter7

import java.text.DecimalFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.jfree.chart.{ChartFactory, ChartFrame, JFreeChart}
import org.jfree.chart.axis.NumberAxis
import org.jfree.chart.plot.PlotOrientation
import org.jfree.data.xy.{XYSeries, XYSeriesCollection}



case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

object RatingsData {
  def show(chart: JFreeChart) {
    val frame = new ChartFrame("plot", chart)
    frame.pack()
    frame.setVisible(true)
  }

  def parseRating(str: String): Rating = {
    val columns = str.split("::")
    assert(columns.size == 4)
    Rating(columns(0).toInt, columns(1).toInt, columns(2).toFloat, columns(3).toLong)
  }

  def main(args: Array[String]) {

    val ratingsFile = "../data/sparkml2/chapter7/ratings.dat"

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // setup SparkSession to use for interactions with Spark
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("MovieRating App")
      .config("spark.sql.warehouse.dir",  ".")
      .config("spark.executor.memory", "2g")
      .getOrCreate()

    import spark.implicits._

    val ratings = spark.read.textFile(ratingsFile).map(parseRating)

    val ratingCount = ratings.count()
    println("Number of ratings:  %s".format(ratingCount))

    ratings.createOrReplaceTempView("ratings")
    val resultDF = spark.sql("select ratings.userId, count(*) as count from ratings group by ratings.userId")
    resultDF.show(25, false);

    val scatterPlotDataset = new XYSeriesCollection()
    val xy = new XYSeries("")

    resultDF.collect().foreach({r => xy.add( r.getAs[Integer]("userId"), r.getAs[Integer]("count")) })

    scatterPlotDataset.addSeries(xy)

    val chart = ChartFactory.createScatterPlot(
      "", "User", "Ratings Per User", scatterPlotDataset, PlotOrientation.VERTICAL, false, false, false)
    val chartPlot = chart.getXYPlot()

    val xAxis = chartPlot.getDomainAxis().asInstanceOf[NumberAxis]
    xAxis.setNumberFormatOverride(new DecimalFormat("####"))

    show(chart)

    spark.stop()
  }
}
