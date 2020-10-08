package spark


import com.mongodb.spark._
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object MongoSparkMain extends App with ConfigHelper {


  //  val conf = new SparkConf()
  //    .setAppName("spark-transform")
  //    .setMaster("local[*]")
  //
  //  val sc = new SparkContext(conf)
  //  val readConfig = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/", "database" -> "test", "collection" -> "zips")) // 1)
  //  val zipDf = sc.loadFromMongoDB(readConfig).toDF() // 2)
  //  zipDf.printSchema() // 3)
  //  zipDf.show()


  val spark = SparkSession
    .builder
    .appName("spark-transform")
    .master("local[*]")
    .getOrCreate()

  // ???
  import spark.implicits._

  val kafka_df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test-topic-1")
    .option("group.id", "test")
    .load()
  kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]
  kafka_df.printSchema()

  //writestream이 필요함
  //kafka_df.writeStream.foreach()...
  //kafka_df.show(10)



  /**
   * Run this main method to see the output of this quick example or copy the code into the spark shell
   *
   * See: http://spark.apache.org/docs/latest/streaming-programming-guide.html
   * Requires: Netcat running on localhost:9999
   *
   * @param args takes an optional single argument for the connection string
   * @throws Throwable if an operation fails
   */
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = getSparkSession(args) // Don't copy and paste as its already configured in the shell
    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val words: Dataset[String] = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count().map((r: Row) => WordCount(r.getAs[String](0), r.getAs[Long](1)))
    val query = wordCounts.writeStream
      .outputMode("complete")
      .foreach(new ForeachWriter[WordCount] {

        val writeConfig: WriteConfig = WriteConfig(Map("uri" -> "mongodb://localhost/test.coll"))
        var mongoConnector: MongoConnector = _
        var wordCounts: mutable.ArrayBuffer[WordCount] = _

        override def process(value: WordCount): Unit = {
          wordCounts.append(value)
        }

        override def close(errorOrNull: Throwable): Unit = {
          if (wordCounts.nonEmpty) {
            mongoConnector.withCollectionDo(writeConfig, {collection: MongoCollection[Document] =>
              collection.insertMany(wordCounts.map(wc => { new Document(wc.word, wc.count)}).asJava)
            })
          }
        }

        override def open(partitionId: Long, version: Long): Boolean = {
          mongoConnector = MongoConnector(writeConfig.asOptions)
          wordCounts = new mutable.ArrayBuffer[WordCount]()
          true
        }
      })
      .start()

    query.awaitTermination()


  // Query 1
//  println( "States with Populations above 10 Million" )
//  val sqlContext = new SQLContext(sc)
//  import sqlContext.implicits._ // 1)
//  zipDf.groupBy("state")
//    .sum("pop")
//    .withColumnRenamed("sum(pop)", "count") // 2)
//    .filter($"count" > 10000000)
//    .show() // 3)
//
//  // Query 2
//  println( "Average City Population by State" )
//  zipDf
//    .groupBy("state", "city")
//    .sum("pop")
//    .withColumnRenamed("sum(pop)", "count")
//    .groupBy("state")
//    .avg("count")
//    .withColumnRenamed("avg(count)", "avgCityPop")
//    .show()
//
//  // Query 3
//  println( "Largest and Smallest Cities by State" )
//  val popByCity = zipDf // 1)
//    .groupBy("state", "city")
//    .sum("pop")
//    .withColumnRenamed("sum(pop)", "count")
//
//  val minMaxCities = popByCity.join(
//    popByCity
//      .groupBy("state")
//      .agg(max("count") as "max_pop", min("count") as "min_pop") // 2)
//      .withColumnRenamed("state", "r_state"),
//    $"state" === $"r_state" && ( $"count" === $"max_pop" || $"count" === $"min_pop") // 3)
//  )
//    .drop($"r_state")
//    .drop($"max_pop")
//    .drop($"min_pop") // 4)
//  minMaxCities.show()
//
//
//  // SparkSQL:
//  println( "SparkSQL" )
//  zipDf.registerTempTable("zips") // 1)
//  sqlContext.sql( // 2)
//    """SELECT state, sum(pop) AS count
//      FROM zips
//      GROUP BY state
//      HAVING sum(pop) > 10000000"""
//  )
//    .show()
//
//  // Aggregation pipeline integration:
//  println( "Aggregation pipeline integration" )
//  zipDf
//    .filter($"pop" > 0)
//    .show()
//
//  println( "RDD with Aggregation pipeline" )
//  val mongoRDD = sc.loadFromMongoDB(readConfig) // 1)
//  mongoRDD
//    .withPipeline(List( // 2)
//      Document.parse("""{ $group: { _id: "$state", totalPop: { $sum: "$pop" } } }"""),
//      Document.parse("""{ $match: { totalPop: { $gte: 10000000 } } }""")
//    ))
//    .collect()
//    .foreach(println)
//
//  // Writing data in MongoDB:
//  MongoSpark
//    .write(minMaxCities)
//    .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/test" )
//    .option("collection","minMaxCities")
//    .mode("overwrite")
//    .save()
}