package spark.ml.cookbook.chapter13

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

object IrisData {

  def readFromFile(sc: SparkContext) = {
      sc.textFile("../data/sparkml2/chapter13/iris.data")
        .filter(s => !s.isEmpty)
        .zipWithIndex()
  }

  def toLabelPoints(records: (String, Long)): LabeledPoint = {
      val (record, recordId) = records
      val fields = record.split(",")
      LabeledPoint(recordId,
        Vectors.dense(fields(0).toDouble, fields(1).toDouble,
          fields(2).toDouble, fields(3).toDouble))
  }

  def buildLabelLookup(records: RDD[(String, Long)]) = {
     records.map {
       case (record: String, id: Long) => {
         val fields = record.split(",")
         (id, fields(4))
       }
     }.collect().toMap
  }
}
