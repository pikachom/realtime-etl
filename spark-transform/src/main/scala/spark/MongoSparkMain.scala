package spark


import com.mongodb.spark.sql._
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object MongoSparkMain extends ConfigHelper {
  val kafkaUri = "localhost:9092"
  val kafkaTopic = "test-topic-1"
  val kafkaGroupId = "test"

  val mongoUri = "mongodb://localhost:27017/test"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("spark-transform")
      .master("local[*]")
      .config("spark.mongodb.input.uri", mongoUri)
      .config("spark.mongodb.output.uri", mongoUri)
      .getOrCreate()


    import spark.implicits._

    val kafka_df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaUri)
      .option("subscribe", kafkaTopic)
      .option("group.id", kafkaGroupId)
      .load()
    kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    kafka_df.printSchema()

    kafka_df.write.mode("append").mongo()

  }
}