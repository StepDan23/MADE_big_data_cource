package metrics

import breeze.linalg.{DenseVector, sum}


/**
 * @author stepdan23
 */
package object regression  {
    def MeanSquareError(yTrue: DenseVector[Double], yPred: DenseVector[Double]): Double = {
        assert(yPred.size == yTrue.size, "size not equals {} {}".format(yTrue.size, yPred.size))

        sum((yPred - yTrue).map(y => y * y)) * (1.0 / yTrue.size)
    }
}
