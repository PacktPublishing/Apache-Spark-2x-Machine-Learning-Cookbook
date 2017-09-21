package spark.ml.cookbook.chapter7

import java.text.DecimalFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.jfree.chart.{ChartFactory, ChartFrame, JFreeChart}
import org.jfree.chart.axis.NumberAxis
import org.jfree.chart.plot.PlotOrientation
import org.jfree.data.xy.{XYSeries, XYSeriesCollection}


case class MovieData(movieId: Int, title: String, year: Int, genre: Seq[String])

object MovieData {
  def show(chart: JFreeChart) {
    val frame = new ChartFrame("plot", chart)
    frame.pack()
    frame.setVisible(true)
  }

  def parseMovie(str: String): MovieData = {
    val columns = str.split("::")
    assert(columns.size == 3)

    val titleYearStriped = """\(|\)""".r.replaceAllIn(columns(1), " ")
    val titleYearData = titleYearStriped.split(" ")

    MovieData(columns(0).toInt,
      titleYearData.take(titleYearData.size - 1).mkString(" "),
      titleYearData.last.toInt,
      columns(2).split("|"))
  }

  def main(args: Array[String]) {

    val movieFile = "../data/sparkml2/chapter7/movies.dat"

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // setup SparkSession to use for interactions with Spark
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("MovieData App")
      .config("spark.sql.warehouse.dir",  ".")
      .config("spark.executor.memory", "2g")
      .getOrCreate()

    import spark.implicits._

    val movies = spark.read.textFile(movieFile).map(parseMovie)
    movies.createOrReplaceTempView("movies")

    val movieCount = movies.count()
    println("Number of movies:  %s".format(movieCount))

    val moviesByYear = spark.sql("select year, count(year) as count from movies group by year order by year")
    moviesByYear.show(25)

    val histogramDataset = new XYSeriesCollection()
    val xy = new XYSeries("")
    moviesByYear.collect().foreach({
      row => xy.add(row.getAs[Int]("year"), row.getAs[Long]("count"))
    })

    histogramDataset.addSeries(xy)

    val chart = ChartFactory.createHistogram(
      "", "Year", "Movies Per Year", histogramDataset, PlotOrientation.VERTICAL, false, false, false)
    val chartPlot = chart.getXYPlot()

    val xAxis = chartPlot.getDomainAxis().asInstanceOf[NumberAxis]
    xAxis.setNumberFormatOverride(new DecimalFormat("####"))

    show(chart)

    spark.stop()
  }
}
