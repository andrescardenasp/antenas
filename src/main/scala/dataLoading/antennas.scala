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

          StructField("AntennaId", StringType, false),
          StructField("Intensity", IntegerType, false),
          StructField("X", DoubleType, false),
          StructField("Y", DoubleType, false)

        ))


/*
        val customSchemaCities = StructType(Array(

          StructField("CityName", StringType, false),
          StructField("Population", IntegerType, false),

          StructField("lat1", StringType, false),
          StructField("lon1", StringType, false),

          StructField("lat2", StringType, false),
          StructField("lon2", DoubleType, false),

          StructField("lat3", StringType, false),
          StructField("lon3", StringType, false),

          StructField("lat4", StringType, false),
          StructField("lon4", StringType, false),

          StructField("lat5", StringType, false),
          StructField("lon5", StringType, false)

        ))*/




        val dfCities = sq.read.parquet(parameters.getString("hdfs.cleanData.cities"))
        println("despues de leer el fichero de ciudades con poligono")
        dfCities.show()





        val df = sq.read.option("header", "true").option("delimiter", ";")
          .schema(customSchemaAntennas).csv(antennasInput)
          .withColumn("antennaId",monotonicallyIncreasingId)
          .withColumn("Point", utils.getPointUDF(col("X"), col("Y")))
          .crossJoin(dfCities)
            .withColumn("antennaInCity", utils.pointInPolygonUDF(col("Point"),col("CityPolygon")))


        df.printSchema()
        df.show()
        df.coalesce(1).write.mode(SaveMode.Overwrite).parquet(antennasData)
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
