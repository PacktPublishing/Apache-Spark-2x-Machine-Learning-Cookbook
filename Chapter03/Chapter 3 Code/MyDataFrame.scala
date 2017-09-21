package spark.ml.cookbook.chapter3

/**
  * Created by Siamak Amirghodsi on 2/2/2016.
  */

import org.apache.spark.sql._
//import org.apache.spark.sql.types.{ StructType, StructField, StringType};
import org.apache.log4j.Logger
import org.apache.log4j.Level


/**
  * Created by Siamak Amirghodsi on 1/22/2016.
  */
object MyDataFrame {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("myDataFrame")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    import spark.implicits._


    val numList = List(1,2,3,4,5,6,7,8,9)
    //    val numRDD = sc.parallelize(numList)
    val numRDD = spark.sparkContext.parallelize(numList)

    val numDF = numRDD.toDF("mylist")




    numDF.show


    val myseq = Seq( ("Sammy","North",113,46.0),("Sumi","South",110,41.0), ("Sunny","East",111,51.0),("Safron","West",113,2.0 ))
    val df1 = spark.createDataFrame(myseq).toDF("Name","Region","dept","Hours")
    df1.show()
    df1.printSchema()


    printf("===============================================")

    println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    val customersRDD = spark.sparkContext.textFile("../data/sparkml2/chapter3/customers13.txt") //Customer file
    val custRDD = customersRDD.map {
      line => val cols = line.trim.split(",")
        (cols(0).toInt, cols(1), cols(2), cols(3).toInt)
    }


    val productsRDD = spark.sparkContext.textFile("../data/sparkml2/chapter3/products13.txt") //Product file
    val prodRDD = productsRDD.map {
      line => val cols = line.trim.split(",")
        (cols(0).toInt, cols(1), cols(2), cols(3).toDouble)
    }

    val salesRDD = spark.sparkContext.textFile("../data/sparkml2/chapter3/sales13.txt") //Sales file
    val saleRDD = salesRDD.map {
      line => val cols = line.trim.split(",")
        (cols(0).toInt, cols(1).toInt, cols(2).toDouble)
    }

    // productid, customerid, price
    //prodid,category,dept,price
    val custDF = custRDD.toDF("custid","name","city","age")
    custDF.show()
    custDF.printSchema()

    val prodDF = prodRDD.toDF("prodid","category","dept","priceAdvertised")
    prodDF.show()
    prodDF.printSchema()

    val saleDF = saleRDD.toDF("prodid", "custid", "priceSold")
    saleDF.printSchema()
    saleDF.show() //problem here

    //custDF.registerTempTable("customers") pre spark 2.0.0
    custDF.createOrReplaceTempView("customers")
    val query1DF = spark.sql("select custid, name from customers")
    query1DF.show()

    //prodDF.registerTempTable("products") pre spark 2.0.0
    prodDF.createOrReplaceTempView("products")
    val query2DF = spark.sql("select prodid, priceAdvertised from products")
    query2DF.show()

    //saleDF.registerTempTable("sales") pre spark 2.0.0
    saleDF.createOrReplaceTempView("sales")
    //val query3DF = sqlContext.sql("select prodid, custid, priceSold from sales")
    val query3DF = spark.sql("select sum(priceSold) as totalSold from sales")
    query3DF.show()

    val query4DF = spark.sql("select custid, priceSold, priceAdvertised from sales s, products p where (s.priceSold/p.priceAdvertised < .80) and p.prodid = s.prodid")
    query4DF.show()



    query4DF.explain()

    println("Dataframe via API:")
    custDF.filter("age > 25.0").show()
    custDF.select("name").show()
    custDF.select("name","city").show()
    custDF.select(custDF("name"),custDF("city"),custDF("age")).show()
    custDF.select(custDF("name"),custDF("city"),custDF("age") <50).show()
    custDF.sort("city").groupBy("city").count().show()
    custDF.explain()

    saleDF.filter("priceSold > 17.0").show()
    prodDF.filter("priceAdvertised < 20.0").show()
    prodDF.groupBy("category").count().show()


    //val query5 = sqlContext.sql("select name, custid, priceSold, priceAdvertised from sales s, products p, customers c  where s.priceSold/p.priceAdvertised < .80 and p.prodid = s.prodid")
    //query5.show()





    spark.stop()
  }

}