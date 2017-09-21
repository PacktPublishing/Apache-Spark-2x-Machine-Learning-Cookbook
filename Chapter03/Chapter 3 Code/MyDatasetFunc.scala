package spark.ml.cookbook.chapter3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
//import spark.ml.cookbook.chapter3.{Car, MyDatasetData}

//import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object MyDatasetFunc {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // setup SparkSession to use for interactions with Spark
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("mydatasetfunc")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    import spark.implicits._

    val cars = spark.createDataset(MyDatasetData.carData)
    cars.show(false)

    val modelData = cars.groupByKey(_.make).mapGroups({
      case (make, car) => {
        val carModel = new ListBuffer[String]()
        car.map(_.model).foreach({
            c =>  carModel += c
        })
        (make, carModel)
      }
    })

    modelData.show(false)

    spark.stop()
  }
}
