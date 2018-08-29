package dataLoading

import java.net.URI

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import java.util.Properties

//import org.apache.commons.lang.StringUtils
//import org.apache.spark.mllib
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import common._


object antennas {


  def load(sc: SparkContext, sq: SQLContext) {
    val conf = sc.hadoopConfiguration
    val logger = Logger.getLogger(this.getClass.getName)
    val parameters = ConfigFactory.parseResources("properties.conf").resolve()
    val antennasInput = parameters.getString("hdfs.input.antennas")
    val antennasData = parameters.getString("hdfs.cleanData.antennas")
    val hdfs = FileSystem.get(new URI(parameters.getString("hdfs.url")), conf)
    try {

      // Valido si hay ficheros para procesar
      val files = hdfs.listStatus(new Path(parameters.getString("hdfs.input.antennasPath")))
      var total = 0
      files.foreach(x => total += 1)
      //println(total)
      if (total > 0) {
        logger.info("Existen " + total + " ficheros de antenas para cargar, procede con la carga")
        println("Existen " + total + " ficheros de antenas para cargar, procede con la carga")
        // Leo los ficheros de la ruta en hdfs.


        val customSchemaAntennas = StructType(Array(

          StructField("antennaid", StringType, false),
          StructField("intensity", IntegerType, false),
          StructField("x", DoubleType, false),
          StructField("y", DoubleType, false)

        ))


        val dfCities = sq.read.parquet(parameters.getString("hdfs.cleanData.cities")).drop("Population")
        println("despues de leer el fichero de ciudades con poligono")
        dfCities.show()





        val df = sq.read.option("header", "true").option("delimiter", ";")
          .schema(customSchemaAntennas).csv(antennasInput)
          .crossJoin(dfCities)
            //.withColumn("antennaInCity", utils.pointInPolygonUDF(col("Point"),col("CityPolygon")))
            .filter(utils.antennaInCityFilter(_))
            .drop("lat1").drop("lat2").drop("lat3").drop("lat4").drop("lat5")
          .drop("lon1").drop("lon2").drop("lon3").drop("lon4").drop("lon5")
            .drop("CityName")



        df.printSchema()
        df.show()
        df.coalesce(1).write.mode(SaveMode.Append).parquet(antennasData)
        logger.info("Se ha escrito el fichero de antenas en HDFS")
        println("Se ha escrito el fichero de antenas en HDFS")
        // Muevo los ficheros a OLD para historificar
        //files.foreach(x=> hdfs.rename(x.getPath, new Path(parameters.getString("hdfs.input.old.antennasPath")+StringUtils.substringAfterLast(x.getPath.toString(),"/"))))

      } else {
        logger.warn("No hay ficheros de antenas para cargar")
        println("No hay ficheros de antenas para cargar")
      }


    } catch {

      case e: Exception =>
        logger.error("Fallo en la limpieza y escritura de antenas en HDFS: " + e.printStackTrace())
        println("Fallo en la limpieza y escritura de antenas en HDFS: ", e.printStackTrace())
        sys.exit(1)

    }

  }


}
