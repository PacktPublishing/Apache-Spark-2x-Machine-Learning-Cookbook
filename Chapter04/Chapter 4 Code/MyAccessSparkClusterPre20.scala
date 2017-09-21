
package spark.ml.cookbook.chapter4

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}



object MyAccessSparkClusterPre20 {
  def main(args: Array[String]) {


    val conf = new SparkConf()
                .setAppName("MyAccessSparkClusterPre20")
                .setMaster("local[4]")   // if cluster setMaster("spark://MasterHostIP:7077")
                .set("spark.sql.warehouse.dir", ".")

    val sc = new SparkContext(conf)

    val file = sc.textFile("../data/sparkml2/chapter4/mySampleCSV.csv")
    val headerAndData = file.map(line => line.split(",").map(_.trim))
    val header = headerAndData.first
    val data = headerAndData.filter(_(0) != header(0))
    val maps = data.map(splits => header.zip(splits).toMap)
    val result = maps.take(4)
    result.foreach(println)




    sc.stop()
  }
}

