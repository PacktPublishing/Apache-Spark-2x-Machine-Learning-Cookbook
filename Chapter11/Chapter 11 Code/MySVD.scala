
package spark.ml.cookbook.chapter11

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession


object MySVD {
  def main(args: Array[String]) {



    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("MySVD")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()


    val dataFile = "../data/sparkml2/chapter11/ratings.dat"


    //read data file in as a RDD, partition RDD across <partitions> cores
    val data = spark.sparkContext.textFile(dataFile)

    //parse data and create (user, item, rating) tuples
    val ratingsRDD = data
      .map(line => line.split("::"))
      .map(fields => (fields(0).toInt, fields(1).toInt, fields(2).toDouble))

    //list of distinct items
    val items = ratingsRDD.map(x => x._2).distinct()
    val maxIndex = items.max + 1

    //user ratings grouped by user_id
    val userItemRatings = ratingsRDD.map(x => (x._1, ( x._2, x._3))).groupByKey().cache()
    userItemRatings.take(2).foreach(println)
    //convert each user's rating to tuple of (user_id, SparseVector_of_ratings)
    val sparseVectorData = userItemRatings
      .map(a=>(a._1.toLong, Vectors.sparse(maxIndex,a._2.toSeq))).sortByKey()

    sparseVectorData.take(2).foreach(println)

    val rows = sparseVectorData.map{
      a=> a._2
    }
//    rows.take(2).foreach(println)

    val mat = new RowMatrix(rows)

    val col = 10  //number of leading singular values to keep
    val computeU = true
    val svd = mat.computeSVD(col, computeU)

    println("Singular values are " + svd.s)
//    println("U: " + svd.U)
    println("V:" + svd.V)

    spark.stop()
  }
}
// scalastyle:on println
