
package spark.ml.cookbook.chapter9

import java.awt.Color

import org.jfree.chart.plot.{XYPlot, PlotOrientation}
import org.jfree.chart.{ChartFactory, ChartFrame, JFreeChart}
import org.jfree.data.xy.{XYSeries, XYSeriesCollection}

import scala.collection.mutable.ArrayBuffer

// scalastyle:off println
object MyGDLinearRegression {

  val gradientStepError = ArrayBuffer[(Int, Double)]()
  val bStep = ArrayBuffer[(Int, Double)]()
  val mStep = ArrayBuffer[(Int, Double)]()

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

  def compute_error_for_line_given_points(b:Double, m:Double, points: Array[Array[Double]]):Double = {

    var totalError = 0.0

    for( point <- points ) {

      var x = point(0)
      var y = point(1)
      totalError += math.pow(y - (m * x + b), 2)
    }
    return totalError / points.length
  }

  def step_gradient(b_current:Double, m_current:Double, points:Array[Array[Double]], learningRate:Double): Array[Double]= {
    var b_gradient= 0.0
    var m_gradient= 0.0
    var N = points.length.toDouble
    for (point <- points) {
      var x = point(0)
      var y = point(1)

      b_gradient +=  -(2 / N) * (y - ((m_current * x) + b_current))
      m_gradient += -(2 / N) * x * (y - ((m_current * x) + b_current))
    }

    var result  = new Array[Double](2)
    result(0) =  b_current - (learningRate * b_gradient)
    result(1) = m_current - (learningRate * m_gradient)
    return result
  }

  def readCSV(inputFile: String) : Array[Array[Double]] = {
    scala.io.Source.fromFile(inputFile)
      .getLines()
      .map(_.split(",").map(_.trim.toDouble))
      .toArray
  }

  def gradient_descent_runner(points:Array[Array[Double]], starting_b:Double, starting_m:Double, learning_rate:Double, num_iterations:Int):Array[Double]= {
    var b = starting_b
    var m = starting_m
    var result = new Array[Double](2)
    var error = 0.0
    result(0) =b
    result(1) =m
    for (i <-0 to num_iterations) {
      result = step_gradient(result(0), result(1),  points, learning_rate)
      bStep += Tuple2(i, result(0))
      mStep += Tuple2(i, result(1))

      error = compute_error_for_line_given_points(result(0), result(1),  points)
      gradientStepError += Tuple2(i, error)

    }
    return result
  }
  def main(args: Array[String]): Unit = {

    val input = "../data/sparkml2/chapter9/Year_Salary.csv"
    val points = readCSV(input)

    val learning_rate = 0.001// 0.00001 // close .01 , .09 , .01 SPSS , .001 SPSS
    val initial_b = 0
    val initial_m = 0
    val num_iterations = 30000 // 1000 // close 750 , 800, 2000  SPSS , 20000 SPSS

    println(s"Starting gradient descent at b = $initial_b, m =$initial_m, error = "+ compute_error_for_line_given_points(initial_b, initial_m, points))
    println("Running...")
    val result= gradient_descent_runner(points, initial_b, initial_m, learning_rate, num_iterations)
    var b= result(0)
    var m = result(1)

    println( s"After $num_iterations iterations b = $b, m = $m, error = "+ compute_error_for_line_given_points(b, m, points))

    val xy = new XYSeries("")
    gradientStepError.foreach{ case (x: Int,y: Double) => xy.add(x,y) }
    val dataset = new XYSeriesCollection(xy)

    val chart = ChartFactory.createXYLineChart(
      "Gradient Descent",  // chart title
      "Iteration",               // x axis label
      "Error",                   // y axis label
      dataset,                   // data
      PlotOrientation.VERTICAL,
      false,                    // include legend
      true,                     // tooltips
      false                     // urls
    )

    val plot = chart.getXYPlot()
    configurePlot(plot)
    show(chart)

    val bxy = new XYSeries("b")
    bStep.foreach{ case (x: Int,y: Double) => bxy.add(x,y) }

    val mxy = new XYSeries("m")
    mStep.foreach{ case (x: Int,y: Double) => mxy.add(x,y) }

    val stepDataset = new XYSeriesCollection()
    stepDataset.addSeries(bxy)
    stepDataset.addSeries(mxy)

    val stepChart = ChartFactory.createXYLineChart(
      "Gradient Descent Steps",  // chart title
      "Iteration",               // x axis label
      "Steps",                   // y axis label
      stepDataset,               // data
      PlotOrientation.VERTICAL,
      true,                    // include legend
      true,                    // tooltips
      false                    // urls
    )

    val stepPlot = stepChart.getXYPlot()
    configurePlot(stepPlot)

    show(stepChart)
  }
}
// scalastyle:on println

