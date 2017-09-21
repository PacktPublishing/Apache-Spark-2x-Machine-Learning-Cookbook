package spark.ml.cookbook.chapter7

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.recommendation.ALS


case class Movie(movieId: Int, title: String, year: Int, genre: Seq[String])
case class FullRating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

object MovieLens {

  def parseMovie(str: String): Movie = {
    val columns = str.split("::")
    assert(columns.size == 3)

    val titleYearStriped = """\(|\)""".r.replaceAllIn(columns(1), " ")
    val titleYearData = titleYearStriped.split(" ")

    Movie(columns(0).toInt,
      titleYearData.take(titleYearData.size - 1).mkString(" "),
      titleYearData.last.toInt,
      columns(2).split("|"))
  }

  def parseFullRating(str: String): FullRating = {
    val columns = str.split("::")
    assert(columns.size == 4)
    FullRating(columns(0).toInt, columns(1).toInt, columns(2).toFloat, columns(3).toLong)
  }

  def main(args: Array[String]) {
    val movieFile = "../data/sparkml2/chapter7/movies.dat"
    val ratingsFile = "../data/sparkml2/chapter7/ratings.dat"

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // setup SparkSession to use for interactions with Spark
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("MovieLens App")
      .config("spark.sql.warehouse.dir",  ".")
      .config("spark.executor.memory", "2g")
      .getOrCreate()

    import spark.implicits._

    val ratings = spark.read.textFile(ratingsFile).map(parseFullRating)

    val ratingCount = ratings.count()
    println("Number of ratings:  %s".format(ratingCount))


    val movies = spark.read.textFile(movieFile).map(parseMovie).cache()
    movies.createOrReplaceTempView("movies")

    val movieCount = movies.count()
    println("Number of movies:  %s".format(movieCount))

    val rs = spark.sql("select movies.title from movies")
    rs.show(25)

    val splits = ratings.randomSplit(Array(0.8, 0.2), 0L)
    val training = splits(0).cache()
    val test = splits(1).cache()

    val numTraining = training.count()
    val numTest = test.count()
    println(s"Training: $numTraining, test: $numTest.")

    val testWithOurUser = spark.createDataset(Seq(
      FullRating(0, 260, 0f, 0), // Star Wars: Episode IV - A New Hope
      FullRating(0, 261, 0f, 0), // Little Women
      FullRating(0, 924, 0f, 0), // 2001: A Space Odyssey
      FullRating(0, 1200, 0f, 0), // Aliens
      FullRating(0, 1307, 0f, 0) // When Harry Met Sally...
    )).as[FullRating]


    val trainWithOurUser = spark.createDataset(Seq(
      FullRating(0, 76, 3f, 0), // Screamers
      FullRating(0, 165, 4f, 0), // Die Hard: With a Vengeance
      FullRating(0, 145, 2f, 0), // Bad Boys
      FullRating(0, 316, 5f, 0), // Stargate
      FullRating(0, 1371, 5f, 0), // Star Trek: The Motion Picture
      FullRating(0, 3578, 4f, 0), // Gladiator
      FullRating(0, 3528, 1f, 0) // Prince of Tides
    )).as[FullRating]


    val testSet = test.union(testWithOurUser)
    test.unpersist()

    val trainSet = training.union(trainWithOurUser)
    training.unpersist()

    val als = new ALS()
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRank(10)
      .setMaxIter(10)
      .setRegParam(0.1)
      .setNumBlocks(10)

    val model = als.fit(trainSet.toDF)

    val predictions = model.transform(testSet.toDF())
    predictions.cache()
    predictions.show(10, false)

    val allPredictions = predictions.join(movies, movies("movieId") === predictions("movieId"), "left")
    allPredictions.select("userId", "rating", "prediction", "title")show(false)

    allPredictions.select("userId", "rating", "prediction", "title").where("userId=0").show(false)

    spark.stop()
  }
}
