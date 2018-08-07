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


  def load(sc: SparkContext, sq: SQLContext) {
    val conf = sc.hadoopConfiguration
    val logger = Logger(this.getClass)
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
        logger.info("Existen ficheros de antenas para cargar, procede con la carga")
        // Leo los ficheros de la ruta en hdfs.
        val df = sq.read.option("header", "true").option("delimiter", ";").csv(antennasInput).distinct()
        df.printSchema()
        df.show()
        df.coalesce(1).write.mode(SaveMode.Overwrite).parquet(antennasData)
        logger.info("Se ha escrito el fichero de antenas en HDFS")
        // Muevo los ficheros a OLD para historificar
        files.foreach(x=> hdfs.rename(x.getPath, new Path(parameters.getString("hdfs.input.old.antennasPath")+StringUtils.substringAfterLast(x.getPath.toString(),"/"))))

      } else {
        logger.warn("No hay ficheros de antenas para cargar")
      }


    } catch {

      case e: Exception =>
        logger.error("Fallo en la limpieza y escritura de antenas en HDFS: " + e.printStackTrace())
        println("Fallo en la limpieza y escritura de antenas en HDFS: ", e.printStackTrace())
        sys.exit(1)

    }

  }


}
