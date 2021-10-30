import java.io.File
import scala.math.floor

import breeze.linalg.{DenseMatrix, csvread, csvwrite}
import metrics.regression.MeanSquareError

import mymodel.LinearRegression


/**
 * @author stepdan23
 */
object Main {
  val N_FOLDS = 5
  val TRAIN_PATH = "train.csv"
  val TEST_PATH = "test.csv"
  val OUTPUT_PATH = "prediction.csv"

  def main(args: Array[String]): Unit = {
    val model = new LinearRegression()
    val trainData: DenseMatrix[Double] = csvread(new File(TRAIN_PATH), ',', skipLines = 1)
    val testData: DenseMatrix[Double] = csvread(new File(TEST_PATH), ',', skipLines = 1)
    val x = trainData(::, 0 to trainData.cols - 2)
    val y = trainData(::, trainData.cols - 1).toDenseVector

    val foldSize = floor(trainData.rows / N_FOLDS).toInt
    for (i <- 0 until N_FOLDS) {
      val testInd = i * foldSize until (i + 1) * foldSize - 1
      val trainInd: IndexedSeq[Int] = (0 until testInd.start) ++ (testInd.end until (trainData.rows - 1))

      model.fit(x(trainInd, ::).toDenseMatrix, y(trainInd).toDenseVector)
      val yPred = model.predict(x(testInd, ::).toDenseMatrix)
      val MSE = MeanSquareError(y(testInd), yPred)

      println(f"MSE for fold ${i + 1}/${N_FOLDS}: ${MSE}\n")
    }
    model.fit(x, y)
    val yTest = model.predict(testData)
    csvwrite(new File(OUTPUT_PATH), yTest.toDenseMatrix.t)
  }
}
