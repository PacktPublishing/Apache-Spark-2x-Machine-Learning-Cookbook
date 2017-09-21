package spark.ml.cookbook.chapter4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

case class Team(name: String, city: String)

object MyDataset {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // setup SparkSession to use for interactions with Spark
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Dataset")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    import spark.implicits._

    val champs = spark.createDataset(List(Team("Broncos", "Denver"), Team("Patriots", "New England")))
    champs.show(false)

    val teams = spark.read
        .option("Header", "true")
        .csv("../data/sparkml2/chapter4/teams.csv")
        .as[Team]

    teams.show(false)

    val cities = teams.map(t => t.city)
    cities.explain()
    cities.show(false)


    teams.write
        .mode(SaveMode.Overwrite)
        .json("../data/sparkml2/chapter4/teams.json")

    spark.stop()
  }
}