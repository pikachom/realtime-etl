package spark


import org.apache.spark._
import org.apache.spark.rdd.RDD
import com.mongodb.spark._
import org.apache.spark.sql.SparkSession

case class OneLine(timeStamp : String, threadName : String, logLevel : String, content : String ) extends Serializable

object HelloWorld {
  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Spark-ETL")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myCollection")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.myCollection")
      .getOrCreate()

    val file = sc.textFile("dummy-log/ProObject_20200921.log")
    val separator = ","
    val line = rawMessages(file, separator)
    line.saveToMongoDB()
    MongoSpark.save(line)
    /**
     * for run spark test
     * */
    val helloWorldString = "Hello World!"
    print(helloWorldString)

  }

  /**
   * regex pattern matching으로 대체해야됨
   * */
  def rawMessages(lines : RDD[String], separator : String) : RDD[OneLine] = {
    lines.map(line => {
      val column = line.split(separator)
      OneLine(
        timeStamp = column(0),
        threadName = column(1),
        logLevel = column(2),
        content = column(3)
      )}
    )}
}