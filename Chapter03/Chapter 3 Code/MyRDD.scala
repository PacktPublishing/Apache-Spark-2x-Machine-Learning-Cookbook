package spark.ml.cookbook.chapter3

import breeze.numerics.pow
import org.apache.spark.sql.SparkSession
import Array._


import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
  * Created by Siamak Amirghodsi on 6/13/2016.
  */
object MyRDD {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("myRDD")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    val SignalNoise: Array[Double]    = Array(0.2,1.2,0.1,0.4,0.3,0.3,0.1,0.3,0.3,0.9,1.8,0.2,3.5,0.5,0.3,0.3,0.2,0.4,0.5,0.9,0.1)
    val SignalStrength: Array[Double] = Array(6.2,1.2,1.2,6.4,5.5,5.3,4.7,2.4,3.2,9.4,1.8,1.2,3.5,5.5,7.7,9.3,1.1,3.1,2.1,4.1,5.1)

    val parSN=spark.sparkContext.parallelize(SignalNoise) // parrallized signal noise RDD
    val parSS=spark.sparkContext.parallelize(SignalStrength)  // parrallized signal strngth

    println("Signal Noise Local Array " , SignalNoise)
    println("RDD Version of Signal Noise on the cluster ", parSN)

    val parSN2=spark.sparkContext.parallelize(SignalNoise,8) // parrallized signal noise RDD
    val parSS2=spark.sparkContext.parallelize(SignalStrength,8)  // parrallized signal strngth

    println("parSN partition length ", parSN.partitions.length )
    println("parSS partition length ",parSS.partitions.length )
    println("parSN2 partition length ",parSN2.partitions.length )
    println("parSS2 partition length ",parSS2.partitions.length )

    /***
      *
      *
      *
      */

    val dirKVrdd = spark.sparkContext.wholeTextFiles("../data/sparkml2/chapter3/*.txt") // place a large number of small files for demo
    println ("files in the directory as RDD ", dirKVrdd)
    println("total number of files ", dirKVrdd.count())
    println("Keys ", dirKVrdd.keys.count())
    println("Values ", dirKVrdd.values.count())
    dirKVrdd.collect()
    println("Values ", dirKVrdd.first())

    //    **/
    val book1 = spark.sparkContext.textFile("../data/sparkml2/chapter3/a.txt")
    // book1.foreach(println)
    val book2 = book1.flatMap(l => l.split(" "))
    val book3 = book1.flatMap(_.split(" "))
    println("Number of lines = ", book1.count())
    println("Number of words = ", book2.count())
    println("Number of words = ", book3.count())



    //*************** Transformation ******************************/
    val num : Array[Double]    = Array(1,2,3,4,5,6,7,8,9,10,11,12,13)
    val odd : Array[Double]    = Array(1,3,5,7,9,11,13)
    val even : Array[Double]    = Array(2,4,6,8,10,12)
    val animals = List("Rabbit","Crow","Lion","Elephant", "dog","cat", "eagle")
    val birds = List("Crow","eagle")

    val numRDD=spark.sparkContext.parallelize(num)
    val oddRDD=spark.sparkContext.parallelize(odd)
    val evenRDD=spark.sparkContext.parallelize(even)
    val animalsRDD=spark.sparkContext.parallelize(animals)
    val birdsRDD=spark.sparkContext.parallelize(birds)
    // =======================================================map()
    println("=========================================================================1")
    //val oddbitmapRDD = numRDD.map(i => 1%2)
    val o1 = numRDD.map(i => i* 2)
    val o2 = numRDD.map(_ * 2)
    o2.take(3).foreach(println)
    println("=========================================================================2")
    val o3 = numRDD.map( i => i % 2)
    val o4 = numRDD.map(_ %2)
    o3.take(3).foreach(println)
    println("=========================================================================3")
    val lineLengthRDD = book1.map(line => line.length )
    lineLengthRDD.take(3).foreach(println)

    val lineUpperRDD = book1.map(_.toUpperCase )
    lineUpperRDD.take(3).foreach(println)

    val wordRDD = book1.map(_.trim.toUpperCase())
    println(wordRDD)


    def MyUpper(s: String): String = { s.trim.toUpperCase }
    book1.map(MyUpper)
    // ========================================================== filter()
    val myOdd= num.filter( i => (i%2) == 1)
    val myOdd2= num.filter(_ %2 == 1)
    myOdd.take(3).foreach(println)

    val myOdd3= num.map(pow(_,2)).filter(_ %2 == 1)
    myOdd3.take(3).foreach(println)
    //wordRDD.take(3).foreach(println(_))

    println("=================================================")
    val shortLines = book1.filter(_.length < 30).filter(_.length > 0)
    println("Total number of lines = ", book1.count())
    println("Number of Short Lines = ", shortLines.count())
    shortLines.take(3).foreach(println)

    println("=================================================")
    val theLines = book1.map(_.trim.toUpperCase()).filter(_.contains("TWO"))
    println("Total number of lines = ", book1.count())
    println("Number of lines with TWO = ", theLines.count())
    theLines.take(3).foreach(println)

    // ======================================================== flatMap()




    // val wordRDD3 = book1.flatMap(_.trim.split("""[\s\W]+""") ).filter(_.length > 0).map(_.toUpperCase())
    //println("Total number of lines = ", book1.count())
    //println("Number of words = ", wordRDD3.count())
    //wordRDD3.take(5)foreach(println(_))

    // using map() by itself results in a list of list. Each list is a series of words corresponding to for each line
    val wordRDD2 = book1.map(_.trim.split("""[\s\W]+""") ).filter(_.length > 0)
    wordRDD2.take(3)foreach(println(_))

    // using flatMap() we get a single list back corresponding to the words in the file
    val wordRDD3 = book1.flatMap(_.trim.split("""[\s\W]+""") ).filter(_.length > 0).map(_.toUpperCase())
    println("Total number of lines = ", book1.count())
    println("Number of words = ", wordRDD3.count())
    wordRDD3.take(5)foreach(println(_))

    val minValue1= numRDD.reduce(_ min _)
    println("minValue1 = ", minValue1)

    val minValue2 = numRDD.glom().map(_.min).reduce(_ min _)
    println("minValue2 = ", minValue2)

    // Set operation



    // word frequency program
    println("Word count program")
    val myPairRDD = wordRDD3.map( (_,1) ).reduceByKey(_ + _).sortBy(_._2, ascending = false)
    //println("Number of words = ", myPairRDD.length)
    myPairRDD.take(10).foreach(println)


    // ======================================================== set opeeration
    println("=============Set Operation on RDD ===============")
    val intersectRDD = numRDD.intersection(oddRDD)
    println("intersectRDD=")
    intersectRDD.collect.sorted.foreach(println)
    println("unionRDD=")
    val unionRDD = oddRDD.union(evenRDD)
    unionRDD.collect.sorted.foreach(println)
    println("subractRDD=")
    val subtractRDD = numRDD.subtract(oddRDD)
    subtractRDD.collect.sorted.foreach(println)
    println("unionRDD2=")
    val unionRDD2 = oddRDD.union(oddRDD) // it does not remove duplicates
    unionRDD2.collect.sorted.foreach(println)
    // ======================================================== distinct, cartesian
    val namesRDD = spark.sparkContext.parallelize(List("Ed","Jain", "Laura", "Ed"))
    val ditinctRDD = namesRDD.distinct()
    println("distinctRDD=")
    ditinctRDD.collect.foreach(println)

    val cartesianRDD = oddRDD.cartesian(evenRDD)
    println("cartesianRDD=")
    cartesianRDD.collect.foreach(println)

    // ======================================================== zip
    println("================================")
    val zipRDD= parSN.zip(parSS).map(row => row._1 / row._2).collect()
    println("zipRDD=")
    zipRDD.foreach(println)

    // ======================================================== groupby
    val rangeRDD=spark.sparkContext.parallelize(1 to 12,3)
    val groupByRDD= rangeRDD.groupBy( i => {if (i % 2 == 1) "Odd" else "Even"}).collect
    println("groupByRDD=")
    groupByRDD.foreach(println)
    // ======================================================== groupBy , reduceByKey
    println(" -----------------groupBy() vs. reduceByKey()--------------------------")
    val alphabets = Array("a", "b", "a", "a", "a", "b") // two type only to make it simple
    val alphabetsPairsRDD = spark.sparkContext.parallelize(alphabets).map(alphabets => (alphabets, 1))

    val countsUsingReduce = alphabetsPairsRDD
      .reduceByKey(_ + _)
      .collect()

    val countsUsingGroup = alphabetsPairsRDD.groupByKey()
      .map(c => (c._1, c._2.sum))
      .collect()
    println("Output for  groupBy")
    countsUsingGroup.foreach(println(_))
    println("Output for  reduceByKey")
    countsUsingReduce.foreach(println(_))
    // ======================================================= pair RDD
    val keyValuePairs = List(("north",1),("south",2),("east",3),("west",4))
    val keyValueCity1 = List(("north","Madison"),("south","Miami"),("east","NYC"),("west","SanJose"))
    val keyValueCity2 = List(("north","Madison"),("west","SanJose"))

    val keyValueRDD = spark.sparkContext.parallelize(keyValuePairs)
    val keyValueCity1RDD = spark.sparkContext.parallelize(keyValueCity1)
    val keyValueCity2RDD = spark.sparkContext.parallelize(keyValueCity2)

    val keys=keyValueRDD.keys
    val values=keyValueRDD.values

    val kvMappedRDD = keyValueRDD.mapValues(_+100)
    kvMappedRDD.collect().foreach(println(_))
    println("Joined RDD = ")
    val joinedRDD = keyValueRDD.join(keyValueCity1RDD)
    joinedRDD.collect().foreach(println(_))
    println("Left Joined RDD = ")
    val leftJoinedRDD = keyValueRDD.leftOuterJoin(keyValueCity2RDD)
    leftJoinedRDD.collect().foreach(println(_))
    println("Right Joined RDD = ")
    val rightJoinedRDD = keyValueRDD.rightOuterJoin(keyValueCity2RDD)
    rightJoinedRDD.collect().foreach(println(_))
    println("Full Joined RDD = ")
    val fullJoinedRDD = keyValueRDD.fullOuterJoin(keyValueCity2RDD)
    fullJoinedRDD.collect().foreach(println(_))

    println("Group By Key RDD = ")
    val signaltypeRDD = spark.sparkContext.parallelize(List(("Buy",1000),("Sell",500),("Buy",600),("Sell",800)))
    val groupedRDD = signaltypeRDD.groupByKey()
    groupedRDD.collect().foreach(println(_))

    println("Reduce By Key RDD = ")
    //val signaltypeRDD = sc.parallelize(List(("Buy",1000),("Sell",500),("Buy",600),("Sell",800)))
    val reducedRDD = signaltypeRDD.reduceByKey(_+_)
    reducedRDD.collect().foreach(println(_))



    spark.stop()

  } // End of main

} // end of obj

