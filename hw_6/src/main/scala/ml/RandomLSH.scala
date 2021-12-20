package ml

import org.apache.spark.ml.linalg
import org.apache.spark.ml.feature.{LSHModel, LSHParams}
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.ml.param.LongParam

import org.apache.spark.sql.types.StructType


trait RandomLSHParams extends LSHParams {
  val RandomSeed: LongParam = new LongParam(this, "random_seed", "A random seed")

  def getSeed: Long = $(RandomSeed)

  def setSeed(value: Long): this.type = set(RandomSeed, value)

  setDefault(RandomSeed -> 23L)


  protected def validateAndTransformSchema(schema: StructType): StructType = {
    if (schema.fieldNames.contains($(outputCol))) {
      SchemaUtils.checkColumnType(schema, getOutputCol, new VectorUDT())
      schema
    } else {
      SchemaUtils.appendColumn(schema, schema(getInputCol).copy(name = getOutputCol))
    }
  }
}


class RandomLSHModel(override val uid: String, val planes: Array[Vector])
  extends LSHModel[RandomLSHModel] with RandomLSHParams
{
  def this(planes: Array[Vector]) = this(Identifiable.randomUID("HyperPlanesLSHModel"), planes)

  override protected[ml] def hashFunction(elems: linalg.Vector): Array[linalg.Vector] = {
    val hashValues: Array[Int] = planes.map(
      plane => if (elems.dot(plane) > 0)  1 else -1
    )
    hashValues.map(Vectors.dense(_))
  }

  override protected[ml] def keyDistance(x: linalg.Vector, y: linalg.Vector): Double = {
    Math.sqrt(Vectors.sqdist(x, y))
  }

  override protected[ml] def hashDistance(x: Array[linalg.Vector], y: Array[linalg.Vector]): Double = {
    x.zip(y).count(pair => pair._1 == pair._2) / x.length
  }

  override def copy(extra: ParamMap): RandomLSHModel = {
    copyValues(new RandomLSHModel(uid, planes), extra)
  }
}
