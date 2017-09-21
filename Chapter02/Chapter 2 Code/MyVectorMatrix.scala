package spark.ml.cookbook.chapter2

import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg._
import breeze.linalg.{DenseVector => BreezeVector}
import Array._
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.log4j.Logger
import org.apache.log4j.Level

object MyVectorMatrix {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // setup SparkSession to use for interactions with Spark
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("myVectorMatrix")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()


    val xyz = Vectors.dense("2".toDouble, "3".toDouble, "4".toDouble)
    println(xyz)

    val CustomerFeatures1: Array[Double] = Array(1,3,5,7,9,1,3,2,4,5,6,1,2,5,3,7,4,3,4,1)
    val CustomerFeatures2: Array[Double] = Array(2,5,5,8,6,1,3,2,4,5,2,1,2,5,3,2,1,1,1,1)
    val ProductFeatures1: Array[Double]  = Array(0,1,1,0,1,1,1,0,0,1,1,1,1,0,1,2,0,1,1,0)

    val x = Vectors.dense(CustomerFeatures1)
    val y = Vectors.dense(CustomerFeatures2)
    val z = Vectors.dense(ProductFeatures1)

    val a = new BreezeVector(x.toArray)//x.asBreeze
    val b = new BreezeVector(y.toArray)//y.asBreeze
    val c = new BreezeVector(z.toArray)//z.asBreeze

    val NetCustPref = a+b
    val dotprod = c.dot(NetCustPref)

    println("Net Customer Preference calculated by Scala Vector operations = \n",NetCustPref)
    println("Customer Pref DOT Product calculated by Scala Vector operations =",dotprod)

    val a2=a.toDenseVector
    val b2=b.toDenseVector
    val c2=c.toDenseVector

    val NetCustPref2 = NetCustPref.toDenseVector
    println("Net Customer Pref converted back to Spark Dense Vactor =",NetCustPref2)

    val denseVec1 = Vectors.dense(5,0,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,8,9)
    val sparseVec1 = Vectors.sparse(20, Array(0,2,18,19), Array(5, 3, 8,9))

    println(denseVec1.size)
    println(denseVec1.numActives)
    println(denseVec1.numNonzeros)
    println("denceVec1 presentation = ",denseVec1)

    println(sparseVec1.size)
    println(sparseVec1.numActives)
    println(sparseVec1.numNonzeros)
    println("sparseVec1 presentation = ",sparseVec1)

    //println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    val ConvertedDenseVect : DenseVector= sparseVec1.toDense
    val ConvertedSparseVect : SparseVector= denseVec1.toSparse
    println("ConvertedDenseVect =", ConvertedDenseVect)
    println("ConvertedSparseVect =", ConvertedSparseVect)

    println("Sparse Vector Representation = ",sparseVec1)
    println("Converting Sparse Vector back to Dense Vector",sparseVec1.toDense)

    println("Dense Vector Representation = ",denseVec1)
    println("Converting Dense Vector to Sparse Vector",denseVec1.toSparse)

    // Spark Example
    // 23.0 34.3 21.3
    // 11.0 33.0 22.6
    // 17.0 24.5 22.2
    // will be Stored as 23.0, 11.0, 17.0, 34.3, 33.0, 24.5, 21.3,22.6,22.2

    val denseMat1 = Matrices.dense(3,3,Array(23.0, 11.0, 17.0, 34.3, 33.0, 24.5, 21.3,22.6,22.2))

    val MyArray1= Array(10.0, 11.0, 20.0, 30.3)
    val denseMat3 = Matrices.dense(2,2,MyArray1)

    println("denseMat1=",denseMat1)
    println("denseMat3=",denseMat3)

    val v1 = Vectors.dense(5,6,2,5)
    val v2 = Vectors.dense(8,7,6,7)
    val v3 = Vectors.dense(3,6,9,1)
    val v4 = Vectors.dense(7,4,9,2)

    val Mat11 = Matrices.dense(4,4,v1.toArray ++ v2.toArray ++ v3.toArray ++ v4.toArray)
    println("Mat11=\n", Mat11)

    println("Number of Columns=",denseMat1.numCols)
    println("Number of Rows=",denseMat1.numRows)
    println("Number of Active elements=",denseMat1.numActives)
    println("Number of Non Zero elements=",denseMat1.numNonzeros)
    println("denseMat1 representation of a dense matrix and its value=\n",denseMat1)

    val sparseMat1= Matrices.sparse(3,2 ,Array(0,1,3), Array(0,1,2), Array(11,22,33))
    println("Number of Columns=",sparseMat1.numCols)
    println("Number of Rows=",sparseMat1.numRows)
    println("Number of Active elements=",sparseMat1.numActives)
    println("Number of Non Zero elements=",sparseMat1.numNonzeros)
    println("sparseMat1 representation of a sparse matrix and its value=\n",sparseMat1)

    /*
    From Manual pages of Apache Spark to use as an example to Define Matrices.sparse()
    1.0 0.0 4.0
    0.0 3.0 5.0
    2.0 0.0 6.0
    [1.0, 2.0, 3.0, 4.0, 5.0, 6.0], rowIndices=[0, 2, 1, 0, 1, 2], colPointers=[0, 2, 3, 6]
    */
    val sparseMat33= Matrices.sparse(3,3 ,Array(0, 2, 3, 6) ,Array(0, 2, 1, 0, 1, 2),Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
    println(sparseMat33)
    val denseFeatureVector= Vectors.dense(1,2,1)

    val result0 = sparseMat33.multiply(denseFeatureVector)
    println("SparseMat33 =", sparseMat33)
    println("denseFeatureVector =", denseFeatureVector)
    println("SparseMat33 * DenseFeatureVector =", result0)

    //println("*****************************************************************************")
    val denseVec13 = Vectors.dense(5,3,0)
    println("denseVec2 =", denseVec13)
    println("denseMat1 =", denseMat1)
    val result3= denseMat1.multiply(denseVec13)
    println("denseMat1 * denseVect13 =", result3)

    val transposedMat1= sparseMat1.transpose
    println("Original sparseMat1 =", sparseMat1)
    println("transposedMat1=",transposedMat1)

    val transposedMat2= denseMat1.transpose
    println("Original sparseMat1 =", denseMat1)
    println("transposedMat2=" ,transposedMat2)

    println("================================================================================")

    val denseMat33: DenseMatrix= new DenseMatrix(3, 3, Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0,7.0,8.0,9.0))
    val identityMat33: DenseMatrix = new DenseMatrix(3, 3, Array(1.0, 0.0, 0.0, 0.0,1.0,0.0,0.0,0.0,1.0))
    val result2 =denseMat33.multiply(identityMat33)
    println(result2)

    println(denseMat33.multiply(denseMat33)) // proof in action:  it is not symmetrical:  aTa not equal a

    println("denseMat33 =", denseMat33)
    println("Matrix transposed twice", denseMat33.transpose.transpose)
    println("denseMat33 =", denseMat33)

    /* Vector arithmetic */
    val w1 = Vectors.dense(1,2,3)
    val w2 = Vectors.dense(4,-5,6)
    val w3 = new BreezeVector(w1.toArray)//w1.asBreeze
    val w4=  new BreezeVector(w2.toArray)// w2.asBreeze
    println("w3 + w4 =",w3+w4)
    println("w3 - w4 =",w3+w4)
    println("w3 * w4 =",w3.dot(w4))
    val sv1 = Vectors.sparse(10, Array(0,2,9), Array(5, 3, 13))
    val sv2 = Vectors.dense(1,0,1,1,0,0,1,0,0,13)
    println("sv1 - Sparse Vector = ",sv1)
    println("sv2 - Dense  Vector = ",sv2)
    //    println("sv1  * sve2  =", sv1.asBreeze.dot(sv2.asBreeze))
    println("sv1  * sv2  =", new BreezeVector(sv1.toArray).dot(new BreezeVector(sv2.toArray)))


    // Matrix multipication
    val dMat1: DenseMatrix= new DenseMatrix(2, 2, Array(1.0, 3.0, 2.0, 4.0))
    val dMat2: DenseMatrix = new DenseMatrix(2, 2, Array(2.0,1.0,0.0,2.0))
    println("dMat1 =",dMat1)
    println("dMat2 =",dMat2)
    println("dMat1 * dMat2 =", dMat1.multiply(dMat2)) //A x B
    println("dMat2 * dMat1 =", dMat2.multiply(dMat1)) //B x A   not the same as A xB

    val m = new RowMatrix(spark.sparkContext.parallelize(Seq(Vectors.dense(4, 3), Vectors.dense(3, 2))))
    val svd = m.computeSVD(2, true)
    val v = svd.V
    val sInvArray = svd.s.toArray.toList.map(x => 1.0 / x).toArray
    val sInverse = new DenseMatrix(2, 2, Matrices.diag(Vectors.dense(sInvArray)).toArray)
    val uArray = svd.U.rows.collect.toList.map(_.toArray.toList).flatten.toArray
    val uTranspose = new DenseMatrix(2, 2, uArray) // already transposed because DenseMatrix has a column-major orientation
    val inverse = v.multiply(sInverse).multiply(uTranspose)
    // -1.9999999999998297  2.999999999999767
    // 2.9999999999997637   -3.9999999999996767
    println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    println(inverse)


    val dataVectors = Seq(
      Vectors.dense(0.0, 1.0, 0.0),
      Vectors.dense(3.0, 1.0, 5.0),
      Vectors.dense(0.0, 7.0, 0.0)
    )

    val identityVectors = Seq(
      Vectors.dense(1.0, 0.0, 0.0),
      Vectors.dense(0.0, 1.0, 0.0),
      Vectors.dense(0.0, 0.0, 1.0)
    )

    val dd = dataVectors.map(x => x.toArray).flatten.toArray
    dd.foreach(println(_))

    val dm00: Matrix = Matrices.dense(3, 3, dd)
    print("==============================")
    print("\n", dm00)

    val distMat33 = new RowMatrix(spark.sparkContext.parallelize(dataVectors))

    println("distMatt33 columns - Count =", distMat33.computeColumnSummaryStatistics().count)
    println("distMatt33 columns - Mean =", distMat33.computeColumnSummaryStatistics().mean)
    println("distMatt33 columns - Variance =", distMat33.computeColumnSummaryStatistics().variance)
    println("distMatt33 columns - CoVariance =", distMat33.computeCovariance())

    val distMatIdent33 = new RowMatrix(spark.sparkContext.parallelize(identityVectors))

    val flatArray = identityVectors.map(x => x.toArray).flatten.toArray
    dd.foreach(println(_))

    //flaten it so we can use it in Matrices.dense API call
    val dmIdentity: Matrix = Matrices.dense(3, 3, flatArray)

    val distMat44 = distMat33.multiply(dmIdentity)
    println("distMatt44 columns - Count =", distMat44.computeColumnSummaryStatistics().count)
    println("distMatt44 columns - Mean =", distMat44.computeColumnSummaryStatistics().mean)
    println("distMatt44 columns - Variance =", distMat44.computeColumnSummaryStatistics().variance)
    println("distMatt44 columns - CoVariance =", distMat44.computeCovariance())

    val distInxMat1 = spark.sparkContext.parallelize( List( IndexedRow( 0L, dataVectors(0)), IndexedRow( 1L, dataVectors(1)), IndexedRow( 1L, dataVectors(2))))

    println("distinct elements=", distInxMat1.distinct().count())

    val CoordinateEntries = Seq(
      MatrixEntry(1, 6, 300),
      MatrixEntry(3, 1, 5),
      MatrixEntry(1, 7, 10)
    )

    val distCordMat1 = new CoordinateMatrix(spark.sparkContext.parallelize(CoordinateEntries.toList))
    println("First Row (MarixEntry) =",distCordMat1.entries.first())

    val distBlkMat1 =  distCordMat1.toBlockMatrix().cache()
    distBlkMat1.validate()
    println("Is block empty =", distBlkMat1.blocks.isEmpty())

    spark.stop()
  }

}
