import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import dataLoading._
import grizzled.slf4j.Logger



object Main {



  def main(args: Array[String]) {
    val logger = Logger(this.getClass)
    val sparkConf = new SparkConf().setAppName("Antenas-Monetizacion").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val sq = new SQLContext(sc)

    //val textFile2 = sc.textFile("hdfs://localhost:9000/input/cities-01_06_2017.csv")

    //textFile2.collect().foreach(println)
    logger.info("Comienza carga y limpieza de Ciudades")

    cities.load(sc, sq)

    logger.info("Termina carga y limpieza de Ciudades")



  }
}
