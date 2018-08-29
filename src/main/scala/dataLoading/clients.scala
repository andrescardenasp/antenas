package dataLoading

import java.net.URI

import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StructField, StructType, _}
import org.apache.spark.sql.{SQLContext, _}


object clients {

  val logger: Logger = Logger.getLogger(this.getClass.getName)

  def load(sc: SparkContext, sq: SQLContext) {
    val conf = sc.hadoopConfiguration

    val parameters = ConfigFactory.parseResources("properties.conf").resolve()
    val clientsInput = parameters.getString("hdfs.input.clients")
    val clientsData = parameters.getString("hdfs.cleanData.clients")
    val hdfs = FileSystem.get(new URI(parameters.getString("hdfs.url")), conf)
    try {

      // Valido si hay ficheros para procesar
      val files = hdfs.listStatus(new Path(parameters.getString("hdfs.input.clientsPath")))
      var total = 0
      files.foreach(x => total += 1)
      //println(total)
      if (total > 0) {
        logger.info("Existen " + total + " ficheros de clientes para cargar, procede con la carga")
        println("Existen " + total + " ficheros de clientes para cargar, procede con la carga")
        // Leo los ficheros de la ruta en hdfs.

        val customSchema = StructType(Array(

          StructField("clientid", StringType, false),
          StructField("age", IntegerType, true),
          StructField("gender", StringType, true),
          StructField("nationality", StringType, true),
            StructField("civilstatus", StringType, true),
          StructField("socioeconomiclevel", StringType, true)
        ))

        val df = sq.read.option("header", "true").option("delimiter", ";").schema(customSchema).csv(clientsInput).distinct()
        df.printSchema()
        df.show()
        df.coalesce(1).write.mode(SaveMode.Append).parquet(clientsData)
        logger.info("Se ha escrito el fichero de clientes en HDFS")
        // Muevo los ficheros a OLD para historificar
        //files.foreach(x=> hdfs.rename(x.getPath, new Path(parameters.getString("hdfs.input.old.clientsPath")+StringUtils.substringAfterLast(x.getPath.toString(),"/"))))

      } else {
        logger.warn("No hay ficheros de clientes para cargar")
        println("No hay ficheros de clientes para cargar")
      }


    } catch {

      case e: Exception =>
        logger.error("Fallo en la limpieza y escritura de clientes en HDFS: " + e.printStackTrace())
        println("Fallo en la limpieza y escritura de clientes en HDFS: ", e.printStackTrace())
        sys.exit(1)

    }

  }


}
