package spark.ml.cookbook.chapter5

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression.IsotonicRegression

object MyIsotonicRegress {

  def main(args: Array[String]): Unit = {

    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("myIsoTonicRegress")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    val data = spark.read.format("libsvm")
      .load("../data/sparkml2/chapter5/iris.scale.txt")
    data.printSchema()
    data.show(false)




    val Array(training, test) = data.randomSplit(Array(0.7, 0.3), seed = System.currentTimeMillis())

    // logistic regression classifier
    val itr = new IsotonicRegression()

    val itrModel = itr.fit(training)

    println(s"Boundaries in increasing order: ${itrModel.boundaries}")
    println(s"Predictions associated with the boundaries: ${itrModel.predictions}")

    itrModel.transform(test).show()
    spark.stop()
  }
}
