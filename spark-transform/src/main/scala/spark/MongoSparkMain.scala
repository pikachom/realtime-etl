package spark


import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.sql.ForeachWriter

import scala.collection.JavaConverters._
import scala.collection.mutable
import org.bson.Document

object MongoSparkMain { // extends ConfigHelper {
  val kafkaUri = "localhost:9092"
  val kafkaTopic = "test-topic-1"
  val kafkaGroupId = "test"

  val mongoUri = "mongodb://localhost:27017"

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession
      .builder
      .appName("spark-transform")
      .master("local[*]")
      .config("spark.mongodb.input.uri", mongoUri)
      .config("spark.mongodb.output.uri", mongoUri)
      .config("spark.mongodb.output.database", "local")
      .config("spark.mongodb.output.collection", "po_log")
      .getOrCreate()

    import spark.implicits._

    val kafkaDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaUri)
      .option("group.id", kafkaGroupId)
      .option("subscribe", kafkaTopic)
      .load()

    /** kafkaDf schema
    root
     |-- key: binary (nullable = true)
     |-- value: binary (nullable = true)
     |-- topic: string (nullable = true)
     |-- partition: integer (nullable = true)
     |-- offset: long (nullable = true)
     |-- timestamp: timestamp (nullable = true)
     |-- timestampType: integer (nullable = true)
     */

    val logDf = kafkaDf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]

    /** print logs on console for debugging
    val consoleOutput = logDf.writeStream
      .outputMode("append")
      .format("console")
      .start()
    consoleOutput.awaitTermination()
    */

    val query = logDf.writeStream.outputMode("append")
      .foreach(new ForeachWriter[(String, String)] {
        val writeConfig: WriteConfig = WriteConfig(Map("uri" -> "mongodb://localhost:27017/local.po_log"))
        var mongoConnector: MongoConnector = _
        var logs: mutable.ArrayBuffer[(String, String)] = _

        override def open(partitionId: Long, epochId: Long): Boolean = {
          mongoConnector = MongoConnector(writeConfig.asOptions)
          logs = new mutable.ArrayBuffer[(String, String)]()
          true
        }

        override def process(value: (String, String)): Unit = {
          val logLines = value._2.split("\n")
          logLines.foreach(line => logs.append((value._1, line)))
        }

        override def close(errorOrNull: Throwable): Unit = {
          if (logs.nonEmpty) {
            mongoConnector.withCollectionDo(writeConfig, {collection: MongoCollection[Document] =>
              collection.insertMany(logs.map(log => { new Document(log._1, log._2)}).asJava)
            })
          }
        }
      }).start()
    query.awaitTermination()
  }
}