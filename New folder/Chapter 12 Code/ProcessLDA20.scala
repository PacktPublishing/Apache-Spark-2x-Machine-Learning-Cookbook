package spark.ml.cookbook.chapter12

import edu.umd.cloud9.collection.wikipedia.WikipediaPage
import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession



object ProcessLDA20 {

  def parseWikiPage(rawPage: String): Option[(String, String)] = {
      val wikiPage = new EnglishWikipediaPage()
      WikipediaPage.readPage(wikiPage, rawPage)

      if (wikiPage.isEmpty
        || wikiPage.isDisambiguation
        || wikiPage.isRedirect
        || !wikiPage.isArticle) {
        None
      } else {
        Some(wikiPage.getTitle, wikiPage.getContent)
      }
  }

  def main(args: Array[String]) {

    val input = "../data/sparkml2/chapter12/enwiki_dump.xml"

    val jobConf = new JobConf()
    jobConf.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
    jobConf.set("stream.recordreader.begin", "<page>")
    jobConf.set("stream.recordreader.end", "</page>")

    FileInputFormat.addInputPath(jobConf, new Path(input))

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("ProcessLDA App")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    import  spark.implicits._

    Logger.getRootLogger.setLevel(Level.WARN)

    val wiki = spark.sparkContext.hadoopRDD(
      jobConf,
      classOf[org.apache.hadoop.streaming.StreamInputFormat],
      classOf[Text],
      classOf[Text]).sample(false, .1)

    //println(wiki.count())

    val df = spark.createDataFrame(wiki.map(_._1.toString)
      .flatMap(parseWikiPage))
      .toDF("title" ,"text")

    val tokenizer = new RegexTokenizer()
      .setPattern("\\W+")
      .setToLowercase(true)
      .setMinTokenLength(4)
      .setInputCol("text")
      .setOutputCol("raw")
    val rawWords = tokenizer.transform(df)

    val stopWords = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("words")
      .setCaseSensitive(false)

    val wordData = stopWords.transform(rawWords)

    val cvModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setMinDF(2)
      .fit(wordData)

    val cv = cvModel.transform(wordData)
    cv.cache()

    val lda = new LDA()
      .setK(5)
      .setMaxIter(10)
      .setFeaturesCol("features")
    val model = lda.fit(cv)
    val transformed = model.transform(cv)

    val ll = model.logLikelihood(cv)
    val lp = model.logPerplexity(cv)

    println("ll: " + ll)
    println("lp: " + lp)

    val topics = model.describeTopics(5)
    topics.show()

    val vocaList = cvModel.vocabulary

    topics.collect().foreach { r => {
        println("\nTopic: " + r.get(r.fieldIndex("topic")))
        val y = r.getSeq[Int](r.fieldIndex("termIndices")).map(vocaList(_))
              .zip(r.getSeq[Double](r.fieldIndex("termWeights")))
        y.foreach(println)

      }
    }


    // Shows the result
//    topics.show(false)
//    transformed.select("title", "features", "topicDistribution", "words").show(false)

    spark.stop()
  }
}
