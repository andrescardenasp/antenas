package dataLoading

import java.net.URI

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.commons.lang.StringUtils

import org.apache.spark.mllib
import com.typesafe.config.{Config, ConfigFactory}
import grizzled.slf4j.Logger




object antennas {
  //  val ages = Seq(42, 75, 29, 64)
  //  println(s"The oldest person is ${ages.max}")

  def load(sc: SparkContext, sq: SQLContext) {
    val conf = sc.hadoopConfiguration
    val logger = Logger(this.getClass)
    val parameters = ConfigFactory.parseResources("properties.conf").resolve()
    val antennasInput = parameters.getString("hdfs.input.antennas")
    val antennasData = parameters.getString("hdfs.cleanData.antennas")
    val hdfs = FileSystem.get(new URI(parameters.getString("hdfs.url")), conf)

    try {

      // valido si hay ficheros para procesar
      val files = hdfs.listStatus(new Path("/input/cities/"))
      //files.foreach(x=> println(x.getPath))

      var total = 0
      files.foreach(x=> total +=1)
      //println(total)
      if (total > 0) {
        logger.info("Existen ficheros de ciudades para cargar, procede con la carga")
        // Leo los ficheros de la ruta en hdfs.
        val df = sq.read.option("header", "true").option("delimiter", ";").csv(citiesInput).distinct()
        //df.printSchema()
        //    df.show()

        val dfGeo = df.withColumn("lat1", split(col("X1"), ",").getItem(0))
          .withColumn("lon1", split(col("X1"), ",").getItem(1))
          .withColumn("lat2", split(col("X2"), ",").getItem(0))
          .withColumn("lon2", split(col("X2"), ",").getItem(1))
          .withColumn("lat3", split(col("X3"), ",").getItem(0))
          .withColumn("lon3", split(col("X3"), ",").getItem(1))
          .withColumn("lat4", split(col("X4"), ",").getItem(0))
          .withColumn("lon4", split(col("X4"), ",").getItem(1))
          .withColumn("lat5", split(col("X5"), ",").getItem(0))
          .withColumn("lon5", split(col("X5"), ",").getItem(1))
          .drop("X1")
          .drop("X2")
          .drop("X3")
          .drop("X4")
          .drop("X5")
        //dfGeo.show()
        dfGeo.coalesce(1).write.mode(SaveMode.Overwrite).parquet(citiesData)
        logger.info("Se ha escrito el fichero de ciudades en HDFS")
        // Muevo los ficheros a OLD para historificar

        files.foreach(x=> hdfs.rename(x.getPath, new Path("/input/citiesOld/"+StringUtils.substringAfterLast(x.getPath.toString(),"/"))))

      } else {
        logger.warn("No hay ficheros de ciudades para cargar")
      }


    } catch {

      case e: Exception =>
        logger.error("Fallo en la limpieza y escritura de ciudades en HDFS: " + e.printStackTrace())
        println("Fallo en la limpieza y escritura de ciudades en HDFS: ", e.printStackTrace())
        sys.exit(1)

    }

  }


}
