package spark.ml.cookbook.chapter12

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover, Word2Vec}
import org.apache.spark.sql.SparkSession

object ProcessWord2Vec20 {

  def main(args: Array[String]) {

    val input = "../data/sparkml2/chapter12/pg62.txt"

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Process Word2Vec  App")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    //import spark.implicits._

    Logger.getRootLogger.setLevel(Level.WARN)

    val df = spark.read.text(input).toDF("text")

    val tokenizer = new RegexTokenizer()
      .setPattern("\\W+")
      .setToLowercase(true)
      .setMinTokenLength(4)
      .setInputCol("text")
      .setOutputCol("raw")
    val rawWords = tokenizer.transform(df)

    val stopWords = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("terms")
      .setCaseSensitive(false)

    val wordTerms = stopWords.transform(rawWords)

    wordTerms.show(false)

    val word2Vec = new Word2Vec()
      .setInputCol("terms")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(wordTerms)

    val synonyms = model.findSynonyms("martian", 10)

    synonyms.show(false)

    spark.stop()
  }
}
