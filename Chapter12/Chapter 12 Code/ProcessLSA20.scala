package spark.ml.cookbook.chapter12

import edu.umd.cloud9.collection.wikipedia.WikipediaPage
import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.sql.SparkSession
import org.tartarus.snowball.ext.PorterStemmer


object ProcessLSA20 {

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

  def wordStem(stem: PorterStemmer, term: String): String = {
    stem.setCurrent(term)
    stem.stem()
    stem.getCurrent
  }

  def tokenizePage(rawPageText: String, stopWords: Set[String]): Seq[String] = {
    val stem = new PorterStemmer()

    rawPageText.split("\\W+")
        .map(_.toLowerCase)
        .filterNot(s => stopWords.contains(s))
        .map(s => wordStem(stem, s))
        .filter(s => s.length > 3)
        .distinct
        .toSeq
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
      .appName("ProcessLSA App")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)

    val wikiData = spark.sparkContext.hadoopRDD(
      jobConf,
      classOf[org.apache.hadoop.streaming.StreamInputFormat],
      classOf[Text],
      classOf[Text]).sample(false, .01)


    val wikiPages = wikiData.map(_._1.toString).flatMap(parseWikiPage)

    println("Wiki Page Count: " + wikiPages.count())

    val stopwords = scala.io.Source.fromFile("../data/sparkml2/chapter12/stopwords.txt").getLines().toSet

    val wikiTerms = wikiPages.map{ case(title, text) => tokenizePage(text, stopwords) }

    val hashtf = new HashingTF()
    val tf = hashtf.transform(wikiTerms)

    val idf = new IDF(minDocFreq=2)
    val idfModel = idf.fit(tf)
    val tfidf = idfModel.transform(tf)

    tfidf.cache()
    val rowMatrix = new  RowMatrix(tfidf)
    val svd = rowMatrix.computeSVD(k=25, computeU = true)

    println(svd)

    spark.stop()
  }

}
