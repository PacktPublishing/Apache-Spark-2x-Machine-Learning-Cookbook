package spark.ml.cookbook.chapter6


import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vectors


object MyFirstLogistic
{

  def main(args: Array[String]): Unit =
  {
    // setup SparkSession to use for interactions with Spark
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("myfirstlogistic")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()


    // We Source the data from here: http://www.ats.ucla.edu/stat/data/binary.csv"
    // http://www.ats.ucla.edu/stat/data/binary.csv"=
    /* We use the top 10 row to demonstrate the first program that uses the logistic regression interface in spark's machine library
    // The training data is made of "label" and a vector of features
    // The Features used are score on GRE, student's GPA and Ranking
     */

    val x = Vectors.dense(380.0, 3.61, 3.0)

    val trainingdata=Seq(
      (0.0, Vectors.dense(380.0, 3.61, 3.0)),
      (1.0, Vectors.dense(660.0, 3.67, 3.0)),
      (1.0, Vectors.dense(800.0, 1.3, 1.0)),
      (1.0, Vectors.dense(640.0, 3.19, 4.0)),
      (0.0, Vectors.dense(520.0, 2.93, 4.0)),
      (1.0, Vectors.dense(760.0, 3.00, 2.0)),
      (1.0, Vectors.dense(560.0, 2.98, 1.0)),
      (0.0, Vectors.dense(400.0, 3.08, 2.0)),
      (1.0, Vectors.dense(540.0, 3.39, 3.0)),
      (0.0, Vectors.dense(700.0, 3.92, 2.0)),
      (0.0, Vectors.dense(800.0, 4.0, 4.0)),
      (0.0, Vectors.dense(440.0, 3.22, 1.0)),
      (1.0, Vectors.dense(760.0, 4.0, 1.0)),
      (0.0, Vectors.dense(700.0, 3.08, 2.0)),
      (1.0, Vectors.dense(700.0, 4.0, 1.0)),
      (0.0, Vectors.dense(480.0, 3.44, 3.0)),
      (0.0, Vectors.dense(780.0, 3.87, 4.0)),
      (0.0, Vectors.dense(360.0, 2.56, 3.0)),
      (0.0, Vectors.dense(800.0, 3.75, 2.0)),
      (1.0, Vectors.dense(540.0, 3.81, 1.0))
    )

    // convert to Dataframe and name the headings
    val trainingDF = spark.createDataFrame(trainingdata).toDF("label", "features")

    // We need an Estimator first so we create a LogisticRegression object and
    // set parameters for Max Iterations, Regularization parameter and whether to include or exclude an intercept
    val lr_Estimator = new LogisticRegression().setMaxIter(80).setRegParam(0.01).setFitIntercept(true)
    // snapshot the parameters feed into our logistic regression estimator
    println("--------------------------------------- Explain parameters: -------------------------------------------\n")
    println("LogisticRegression parameters:\n" + lr_Estimator.explainParams() + "\n")
    // Fit the training data and create the model
    val Admission_lr_Model = lr_Estimator.fit(trainingDF)

    // explore the parameters used to estimate the model
    println("Admission_lr_Model parameters:")
    println(Admission_lr_Model.parent.extractParamMap)

    //print model summary
    println("Admission_lr_Model Summary:")
    println(Admission_lr_Model.summary.predictions)
    // Build the model
    val predict=Admission_lr_Model.transform(trainingDF)
    // print a schema as a guideline
    predict.printSchema()

    // Extract pieces that you need looking at schema and parameter explanation output earlier in the program
    // Code made verbose for clarity
    val label1=predict.select("label").collect()
    val features1=predict.select("features").collect()
    val probability=predict.select("probability").collect()
    val prediction=predict.select("prediction").collect()
    val rawPrediction=predict.select("rawPrediction").collect()
    // print model values
    println("Training Set Size=", label1.size )
    println("No.     Original Feature Vector                 Predicted Outcome              confidence                                    probability")
    println("---     ---------------------------           ----------------------      -------------------------                   --------------------")
    for(  i <- 0 to label1.size-1) {
      print(i, "    ", label1(i), features1(i), "                    ", prediction(i), "      ", rawPrediction(i), "       ", probability(i))
      println()
    }

    println("******************************************************************************************")

    spark.stop()

  }// end of func main
} // end of myfistlogistic object