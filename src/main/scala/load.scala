import org.apache.spark._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SQLContext
import scala.collection.JavaConversions._


object load {
  //  val ages = Seq(42, 75, 29, 64)
  //  println(s"The oldest person is ${ages.max}")

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Antenas-Monetizacion").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val textFile2 = sc.textFile("hdfs://localhost:9000/input/cities-01_06_2017.csv")

    textFile2.collect().foreach(println)


  }
}
