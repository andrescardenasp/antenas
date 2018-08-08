package dataLoading

import java.net.URI

import com.typesafe.config.ConfigFactory
import grizzled.slf4j.Logger
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StructField, StructType, _}



object events {

  val logger = Logger(this.getClass)
  def load(sc: SparkContext, sq: SQLContext) {
    val conf = sc.hadoopConfiguration

    val parameters = ConfigFactory.parseResources("properties.conf").resolve()
    val eventsInput = parameters.getString("hdfs.input.events")
    val eventsData = parameters.getString("hdfs.cleanData.events")
    val hdfs = FileSystem.get(new URI(parameters.getString("hdfs.url")), conf)
    try {

      // Valido si hay ficheros para procesar
      val files = hdfs.listStatus(new Path(parameters.getString("hdfs.input.eventsPath")))
      var total = 0
      files.foreach(x => total += 1)
      //println(total)
      if (total > 0) {
        logger.info("Existen ficheros de eventos para cargar, procede con la carga")
        println("Existen ficheros de eventos para cargar, procede con la carga")
        // Leo los ficheros de la ruta en hdfs.
//ClientId;Date;AntennaId
        val customSchema = StructType(Array(

          StructField("ClientId", StringType, false),
          StructField("Date", StringType, false),
          StructField("AntennaId", StringType, false)

        ))

        val df = sq.read.option("header", "true").option("delimiter", ";").schema(customSchema).csv(eventsInput).distinct()
        df.printSchema()
        df.show()

        val validDateDf = df.filter(validDateDf(_))

        //df.coalesce(1).write.mode(SaveMode.Overwrite).parquet(eventsData)
        //logger.info("Se ha escrito el fichero de eventos en HDFS")
        // Muevo los ficheros a OLD para historificar
        //files.foreach(x=> hdfs.rename(x.getPath, new Path(parameters.getString("hdfs.input.old.eventsPath")+StringUtils.substringAfterLast(x.getPath.toString(),"/"))))

      } else {
        logger.warn("No hay ficheros de eventos para cargar")
        println("No hay ficheros de eventos para cargar")
      }


    } catch {

      case e: Exception =>
        logger.error("Fallo en la limpieza y escritura de eventos en HDFS: " + e.printStackTrace())
        println("Fallo en la limpieza y escritura de eventos en HDFS: ", e.printStackTrace())
        sys.exit(1)

    }

  }


// 05/06/2017-11:20:05.000
  val DATE_TIME_FORMAT = "dd/MM/yyyy-HH:mm:ss.SSS"

  def validateDf(row: Row): Unit = try {
    //assume row.getString(1) with give Datetime string
    java.time.LocalDateTime.parse(row.getString(1), java.time.format.DateTimeFormatter.ofPattern(DATE_TIME_FORMAT))
    //logger.info("Fecha validada correctamente" + dateInRow)
    true
  } catch {
    case ex: java.time.format.DateTimeParseException => {
      // Handle exception if you want
      //logger.error("Fallo en la validación de fechas" + ex)
      false
    }
  }



}
