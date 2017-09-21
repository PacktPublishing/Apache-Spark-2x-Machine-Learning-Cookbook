package spark.ml.cookbook.chapter12

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.jfree.chart.axis.CategoryLabelPositions
import org.jfree.chart.plot.PlotOrientation
import org.jfree.chart.{ChartFactory, ChartFrame, JFreeChart}
import org.jfree.data.category.DefaultCategoryDataset

object ProcessWordCount20 {

  def show(chart: JFreeChart) {
    val frame = new ChartFrame("plot", chart)
    frame.pack()
    frame.setVisible(true)
  }

  def main(args: Array[String]) {

    val input = "../data/sparkml2/chapter12/pg62.txt"

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("ProcessWordCount")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    import spark.implicits._

    Logger.getRootLogger.setLevel(Level.WARN)

    val stopwords = scala.io.Source.fromFile("../data/sparkml2/chapter12/stopwords.txt").getLines().toSet

    val lineOfBook = spark.sparkContext.textFile(input)
      .flatMap(line => line.split("\\W+"))
      .map(_.toLowerCase)
      .filter( s => !stopwords.contains(s))
      .filter( s => s.length > 2)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)

    val top25 = lineOfBook.take(25)

    val dataset = new DefaultCategoryDataset()

    top25.foreach( {case (term: String, count: Int) => dataset.setValue(count, "Count", term) })

    val chart = ChartFactory.createBarChart("Word Count",
      "Words", "Count", dataset, PlotOrientation.VERTICAL,
      false, true, false)

    val plot = chart.getCategoryPlot()
    val domainAxis = plot.getDomainAxis();
    domainAxis.setCategoryLabelPositions(CategoryLabelPositions.DOWN_45);

    show(chart)

    spark.stop()
  }
}
