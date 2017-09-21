package spark.ml.cookbook.chapter4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import util.Random

object MyDataRDD {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("myDataRDD")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    //PLEASE NOTE to change the total Number to lower to start
    //otherwise it will requires a lot memory and may causing your
    // machine to run out of resources

    val totalNumbers = 1000000

    val data = spark.sparkContext.parallelize((Seq.fill(totalNumbers)( Random.nextInt(10000), Random.nextDouble())))


    data.take(10).foreach(println)

    Logger.getLogger("org").info("start sorting by 1st element and Cache :")
    val sortedCache = data.sortBy(_._1, false)   // ascending =false
    sortedCache.count()
    sortedCache.cache()

    println("sorted by 1st elem cached:")
    sortedCache.take(10).foreach(println)


    Logger.getLogger("org").info("start sorting by 1st element Non Cache :")

    val sortedNoCache = data.sortBy(_._1, false) // ascending =false
    sortedNoCache.count()
    println("sorted by 1st elem no cache:")
    sortedNoCache.take(10).foreach(println)

    println("=====Cached RDD=======")
    val group1 = sortedCache.map(i=> (i._1, i._1*i._2))
      .groupByKey()
      .collect()
    val t11 = System.currentTimeMillis()
    sortedCache.sortBy(_._1, true)   //ascending order
    val t12 = System.currentTimeMillis()
    sortedCache.count()

    println("Time milliseconds taken for sort Cached RDD: "+ ( t12 - t11))
    group1.take(10).foreach(println)

    val t13 = System.currentTimeMillis()
    val sortedCacheCount = sortedCache.count()
    val t14 = System.currentTimeMillis()
    println("Total time milliseconds taken for Cached RDD Count: "+(t14-t13))
    println("Total Cached RDD Count: "+sortedCacheCount)

    println("~~~~~NonCached RDD~~~~~~~")
    val group2 = sortedNoCache.map(i=> (i._1, i._1*i._2))
      .groupByKey()
      .collect()
    val t21 = System.currentTimeMillis()
    sortedNoCache.sortBy(_._1, true) // ascending order
    val t22 = System.currentTimeMillis()
    sortedNoCache.count()

    println("Time milliseconds taken for sort NonCached RDD: "+ ( t22 - t21))
    group2.take(10).foreach(println)

    val t23 = System.currentTimeMillis()
    val sortedNoCacheCount = sortedNoCache.count()
    val t24 = System.currentTimeMillis()
    println("Total time milliseconds taken for NonCached RDD Count: "+(t24-t23))
    println("Total NonCached RDD Count: "+sortedNoCacheCount)

    spark.stop()
  }
}