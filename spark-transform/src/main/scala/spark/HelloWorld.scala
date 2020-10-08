//package spark
//
//
//import org.apache.spark._
//import org.apache.spark.rdd.RDD
//import com.mongodb.spark._
//import org.apache.spark.sql.SparkSession
//
//case class OneLine(timeStamp : String, threadName : String, logLevel : String, content : String ) extends Serializable
//
//object HelloWorld {
//  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Spark-ETL")
//  conf.set("spark.driver.allowMultipleContexts","true")
//  @transient lazy val sc: SparkContext = new SparkContext(conf)
//
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder()
//      .master("local")
//      .appName("MongoSparkConnectorIntro")
//      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/test.myCollection")
//      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/test.myCollection")
//      .getOrCreate()
//
//    val file = sc.textFile("dummy-log/ProObject_20200921.log")
//    val separator = "] ["
//    val line = rawMessages(file, separator)
//    line.saveToMongoDB()
//    MongoSpark.save(line)
//    /**
//     * for run spark test
//     * */
//    val helloWorldString = "Hello World!"
//    print(helloWorldString)
//
//  }
//
//  /**
//   * regex pattern matching으로 대체해야됨
//   * */
//  def rawMessages(lines : RDD[String], separator : String) : RDD[OneLine] = {
//    lines.map(line => {
//      val column = line.split(separator)
//      if(column.length == 3){
//        OneLine(
//          timeStamp = column(0),
//          threadName = column(1),
//          logLevel = column(2),
//          content = column(3)
//        )}
//      else{
//        OneLine(
//          timeStamp = "-",
//          threadName = "-",
//          logLevel = "-",
//          content = column(0)
//        )}
//      }
//
//
//
//    )}
//}



//
/*
* mongo sql 예시
* */

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
