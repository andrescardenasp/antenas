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
import common._
import org.apache.log4j.Logger
import org.apache.spark.sql.SQLContext




object cities {

  val logger: Logger = Logger.getLogger(this.getClass.getName)


  def load(sc: SparkContext, sq: SQLContext) {
    val conf = sc.hadoopConfiguration
    val parameters = ConfigFactory.parseResources("properties.conf").resolve()
    val citiesInput = parameters.getString("hdfs.input.cities")
    val citiesData = parameters.getString("hdfs.cleanData.cities")
    val hdfs = FileSystem.get(new URI(parameters.getString("hdfs.url")), conf)
    import sq.implicits._
    try {

// Valido si hay ficheros para procesar
      val files = hdfs.listStatus(new Path(parameters.getString("hdfs.input.citiesPath")))
      var total = 0
      files.foreach(x=> total +=1)
      //println(total)
      if (total > 0) {
        logger.info("Existen " + total + " ficheros de ciudades para cargar, procede con la carga")
        println("Existen " + total + " ficheros de ciudades para cargar, procede con la carga")
        // Leo los ficheros de la ruta en hdfs.
        val df = sq.read.option("header", "true").option("delimiter", ";").csv(citiesInput).distinct()
          .withColumnRenamed("CityName","cityname")
          .withColumnRenamed("Population","population")
        //df.printSchema()
        //    df.show()

        val dfGeo = df
          .withColumn("lat1", utils.toDouble(split(col("X1"), ",").getItem(1)))
          .withColumn("lon1", utils.toDouble(split(col("X1"), ",").getItem(0)))
          .withColumn("lat2", utils.toDouble(split(col("X2"), ",").getItem(1)))
          .withColumn("lon2", utils.toDouble(split(col("X2"), ",").getItem(0)))
          .withColumn("lat3", utils.toDouble(split(col("X3"), ",").getItem(1)))
          .withColumn("lon3", utils.toDouble(split(col("X3"), ",").getItem(0)))
          .withColumn("lat4", utils.toDouble(split(col("X4"), ",").getItem(1)))
          .withColumn("lon4", utils.toDouble(split(col("X4"), ",").getItem(0)))
          .withColumn("lat5", utils.toDouble(split(col("X5"), ",").getItem(1)))
          .withColumn("lon5", utils.toDouble(split(col("X5"), ",").getItem(0)))
          .drop("X1")
          .drop("X2")
          .drop("X3")
          .drop("X4")
          .drop("X5")
          .withColumn("cityid", monotonically_increasing_id())
        dfGeo.printSchema()
        dfGeo.show()
        dfGeo.coalesce(1).write.mode(SaveMode.Append).parquet(citiesData)
        logger.info("Se ha escrito el fichero de ciudades en HDFS")
        println("Se ha escrito el fichero de ciudades en HDFS")
        // Muevo los ficheros a OLD para historificar
        //files.foreach(x=> hdfs.rename(x.getPath, new Path(parameters.getString("hdfs.input.old.citiesPath")+StringUtils.substringAfterLast(x.getPath.toString(),"/"))))

      } else {
        logger.warn("No hay ficheros de ciudades para cargar")
        println("No hay ficheros de ciudades para cargar")
      }


    } catch {

      case e: Exception =>
        logger.error("Fallo en la limpieza y escritura de ciudades en HDFS: " + e.printStackTrace())
        println("Fallo en la limpieza y escritura de ciudades en HDFS: ", e.printStackTrace())
        sys.exit(1)

    }

  }


}
