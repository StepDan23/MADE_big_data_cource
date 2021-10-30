package mymodel

import breeze.linalg.{DenseMatrix, DenseVector, sum}


/**
 * @author stepdan23
 */
class LinearRegression() {
  var weights = new DenseVector[Double](0)
  var bias = 0.0
  var fitted = false
  val EPS = 1e-5

  def fit(x: DenseMatrix[Double], y: DenseVector[Double], lr: Double = 0.0001, numIter: Int = 10000) {
    val m = x.rows
    weights = DenseVector.zeros(x.cols)
    fitted = true

    for (_ <- 0 to numIter) {
      val yPred = predict(x)
      val dW = -2.0 / m * (x.t * (y - yPred))
      val dB = -2.0 / m * sum(y - yPred)

      weights -= (dW * lr)
      bias -= dB * lr
    }

  }

  def predict(x: DenseMatrix[Double]): DenseVector[Double] = {
    if (fitted)
      (x * weights + bias).toDenseVector
    else throw new RuntimeException("model not fitted")
  }
}
