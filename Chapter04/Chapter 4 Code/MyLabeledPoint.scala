package spark.ml.cookbook.chapter4


import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql._
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors

object MyLabeledPoint {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("myLabeledPoint")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()


    val myLabeledPoints = spark.createDataFrame(Seq(
      LabeledPoint(1.0, Vectors.dense(0.0, 1.1, 0.1)),
      LabeledPoint(0.0, Vectors.dense(2.0, 1.0, -1.0)),
      LabeledPoint(0.0, Vectors.dense(2.0, 1.3, 1.0)),
      LabeledPoint(1.0, Vectors.dense(0.0, 1.2, -0.5)),

      LabeledPoint(0.0, Vectors.sparse(3, Array(0,2), Array(1.0,3.0))),
      LabeledPoint(1.0, Vectors.sparse(3, Array(1,2), Array(1.2,-0.4)))

    ))

    myLabeledPoints.show()
    val lr = new LogisticRegression()

    lr.setMaxIter(5)
      .setRegParam(0.01)
    val model = lr.fit(myLabeledPoints)

    println("Model was fit using parameters: " + model.parent.extractParamMap())

    spark.stop()
  }

}