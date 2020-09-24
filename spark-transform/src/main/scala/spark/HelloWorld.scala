package spark


import org.apache.spark._
import org.apache.spark.rdd.RDD
import com.mongodb.spark._

case class OneLine(timeStamp : String, threadName : String, logLevel : String, content : String ) extends Serializable

object HelloWorld {
  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Spark-ETL")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

    val file = sc.textFile("dummy-log/ProObject_20200921.log")
    val separator = ","
    val line = rawMessages(file, separator)
    line.saveToMongoDB();





    val helloWorldString = "Hello World!"
    print(helloWorldString)

  }
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