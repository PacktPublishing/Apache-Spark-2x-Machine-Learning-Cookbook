package spark.ml.cookbook.chapter9

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

/**
  * Created by Sammy on 11/29/2016.
  */
object MyGradient {

  def quadratic_function_itself(x:Double):Double = {
    // the function being differentiated
    // We find f(x) after finding the minima at x
    //f(x) = 2x^2 – 8x + 9

    return 2 * math.pow(x,2) - (8*x) +9 //f(x)

  }

    def derivative_of_function(x:Double):Double = {
      // The derivative of f(x)
      return 4 * x  - 8 // d/dx
    }

    def main(args: Array[String]): Unit = {


      var oldMinimumValue = 0.0
      var iteration  = 0;
      var currentMinimumValue = 13.0 // just pick up a random value
      val actualMinima = 2.0 // similar to a label in training phase
      var minimumVector = ArrayBuffer[Double]()
      var costVector = ArrayBuffer[Double]()



      //Scenario 1: a bit granular, but  works around  200 iteration
     // val stepSize = .01
      //val tolerance = 0.0001

      //Sceneario 2 Step size too granular
      //val stepSize = 0.001
      //val tolerance = 0.0001

      // Senario 3 step size too large - bows up
      // val stepSize = 0.5
       //val tolerance = 0.001

      // Senario 4: step size too large - stuck in loop
      val stepSize = 0.6
      val tolerance = 0.001


      while (math.abs(currentMinimumValue - oldMinimumValue) > tolerance) {
        iteration +=1  //= iteration + 1 for debugging when non-convergence
        oldMinimumValue = currentMinimumValue
        val gradient_value_at_point = derivative_of_function(oldMinimumValue)
        val move_by_amount =  gradient_value_at_point * stepSize
        currentMinimumValue = oldMinimumValue - move_by_amount
        costVector += math.pow(actualMinima - currentMinimumValue, 2) // simple square distance (good enough for purpose)
        minimumVector += currentMinimumValue
        print("Iteration= ",iteration,"  currentMinimumValue= ", currentMinimumValue)
        print("\n")

        // if (iteration == 200) break // to check the early iteration when non convergence

      }
      print("\n Cost Vector:  "+ costVector)
      print("\n Minimum Vactor" + minimumVector)
      var minimaXvalue= currentMinimumValue
      var minimaYvalue= quadratic_function_itself(currentMinimumValue)

      print("\n\nGD Algo: Local minimum found at     X = : "+f"$minimaXvalue%1.2f")
      print("\nGD Algo:                        Y=f(x)= : "+f"$minimaYvalue%1.2f")


    }

}
