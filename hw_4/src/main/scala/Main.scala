import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{functions => F}

/**
 * @author stepdan23
 */
object Main {
  val DATA_PATH = "hotel_reviews.csv"
  val N_WORDS = 100

  def getSparkSession: SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName("hw_4")
      .getOrCreate()
  }

  def prepareData(dataDf: DataFrame): DataFrame = {
    dataDf
      .drop("Rating")
      .na.drop()
      .withColumn("Review", F.lower(F.col("Review")))
      .withColumn("Review", F.regexp_replace(F.col("Review"), "[^a-zA-Z0-9 ]+", ""))
      .withColumn("id", F.monotonically_increasing_id())
      .withColumn("words", F.split(F.col("Review"), " "))
      .withColumn("size", F.size(F.col("words")))
      .withColumn("word", F.explode(F.col("words")))
      .filter(F.length(F.col("word")) > 1)
      .select("id", "word", "size")
      .cache()
  }

  def tfIdf(wordsDf: DataFrame): DataFrame = {
    val tf = wordsDf
      .groupBy("id", "word", "size")
      .agg(F.count("word") / F.col("size") as "tf")

    val NDocs = wordsDf.select("id").distinct().count()
    val idf = wordsDf
      .groupBy("word")
      .agg(F.countDistinct("id") as "df")
      .withColumn("idf",F.log1p(F.lit(NDocs) / F.col("df")))
      .sort(F.col("df").desc).limit(N_WORDS)

    idf
      .join(tf, "word")
      .withColumn("tf_idf",  F.round(F.col("tf") * F.col("idf"), 3))
  }

  def main(args: Array[String]): Unit = {
    val spark = getSparkSession

    val dataDf = spark.read.option("header", "true").option("inferSchema", "true").csv(DATA_PATH)
    val wordsDf = prepareData(dataDf)
    val tfIdfDf = tfIdf(wordsDf)

    tfIdfDf
      .groupBy("id")
      .pivot("word")
      .agg(F.max(F.col("tf_idf")))
      .sort("id")
      .show(10)
  }
}