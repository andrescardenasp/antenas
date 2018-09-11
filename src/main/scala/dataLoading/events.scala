package dataLoading

import java.net.URI

import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.sql.types.{StructField, StructType, _}
import org.apache.spark.sql.functions.{col, date_format, from_unixtime, split, to_date, udf, unix_timestamp}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.sql.functions._


object events {

  val logger = Logger.getLogger(this.getClass.getName)
  // 05/06/2017-11:20:05.000
  val DATE_TIME_FORMAT_INPUT = "dd/MM/yyyy-HH:mm:ss.SSS"
  val DATE_TIME_FORMAT_CLEAN = "dd/MM/yyyy"
  val dformatter = DateTimeFormatter.ofPattern(DATE_TIME_FORMAT_CLEAN)

  def load(sc: SparkContext, sq: SQLContext) {
    val conf = sc.hadoopConfiguration
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val parameters = ConfigFactory.parseResources("properties.conf").resolve()
    val eventsInput = parameters.getString("hdfs.input.events")
    val eventsData = parameters.getString("hdfs.cleanData.events")
    val dataToModel = parameters.getString("hdfs.modeldata.data")
    val hdfs = FileSystem.get(new URI(parameters.getString("hdfs.url")), conf)

    try {

      // Valido si hay ficheros para procesar
      val files = hdfs.listStatus(new Path(parameters.getString("hdfs.input.eventsPath")))
      var total = 0
      files.foreach(x => total += 1)
      //println(total)
      if (total > 0) {
        logger.info("Existen " + total + " ficheros de eventos para cargar, procede con la carga")
        println("Existen " + total + " ficheros de eventos para cargar, procede con la carga")
        // Leo los ficheros de la ruta en hdfs.
        //ClientId;Date;AntennaId
        val customSchema = StructType(Array(

          StructField("ClientId", StringType, false),
          StructField("Date", StringType, false),
          StructField("AntennaId", StringType, false)

        ))

        val df = sq.read.option("header", "true").option("delimiter", ";").schema(customSchema).csv(eventsInput)
        //df.printSchema()
        //df.show()
        val totalEvents = df.count()


        val validDf = df.filter(validateDf(_))
          .withColumnRenamed("ClientId","clientid")
          .withColumnRenamed("AntennaId","antennaid")
          .withColumn("time", split(col("Date"), "-").getItem(1))
          .withColumn("date", split(col("Date"), "-").getItem(0))
          .withColumn("day", split(col("Date"), "/").getItem(0))
          .withColumn("month", split(col("Date"), "/").getItem(1))
          .withColumn("year", split(col("Date"), "/").getItem(2))
          .withColumn("dayofweek", date_format(to_date(col("Date"), "dd/MM/yyyy"), "u"))
          .withColumn("hour", split(col("Time"), ":").getItem(0))
          .withColumn("minute", split(col("Time"), ":").getItem(1))
          .withColumn("weekhour", concat(col("dayofweek"),lit("-"),col("hour")))


/*        val pivotedEvents = validDf.select(col("clientid"), col("date"), col("antennaid"), col("time"), col("day"),
          col("month"), col("year"), col("dayofweek"), col("hour"), col("minute"), col("weekhour"),
          count(col("weekhour")).over(partitionBy(col("clientid"),col("antennaid"))).as("Count"))
          .groupBy(col("clientid"),col("antennaid"))
          .pivot("weekhour").agg(first("Count"))*/

        val lscol = sc.parallelize(Seq(
          ("1-00"), ("1-01"), ("1-02"), ("1-03"), ("1-04"), ("1-05"), ("1-06"), ("1-07"), ("1-08"), ("1-09"), ("1-10"), ("1-11"), ("1-12"), ("1-13"), ("1-14"), ("1-15"), ("1-16"), ("1-17"), ("1-18"), ("1-19"), ("1-20"), ("1-21"), ("1-22"), ("1-23"),
          ("2-00"), ("2-01"), ("2-02"), ("2-03"), ("2-04"), ("2-05"), ("2-06"), ("2-07"), ("2-08"), ("2-09"), ("2-10"), ("2-11"), ("2-12"), ("2-13"), ("2-14"), ("2-15"), ("2-16"), ("2-17"), ("2-18"), ("2-19"), ("2-20"), ("2-21"), ("2-22"), ("2-23"),
          ("3-00"), ("3-01"), ("3-02"), ("3-03"), ("3-04"), ("3-05"), ("3-06"), ("3-07"), ("3-08"), ("3-09"), ("3-10"), ("3-11"), ("3-12"), ("3-13"), ("3-14"), ("3-15"), ("3-16"), ("3-17"), ("3-18"), ("3-19"), ("3-20"), ("3-21"), ("3-22"), ("3-23"),
          ("4-00"), ("4-01"), ("4-02"), ("4-03"), ("4-04"), ("4-05"), ("4-06"), ("4-07"), ("4-08"), ("4-09"), ("4-10"), ("4-11"), ("4-12"), ("4-13"), ("4-14"), ("4-15"), ("4-16"), ("4-17"), ("4-18"), ("4-19"), ("4-20"), ("4-21"), ("4-22"), ("4-23"),
          ("5-00"), ("5-01"), ("5-02"), ("5-03"), ("5-04"), ("5-05"), ("5-06"), ("5-07"), ("5-08"), ("5-09"), ("5-10"), ("5-11"), ("5-12"), ("5-13"), ("5-14"), ("5-15"), ("5-16"), ("5-17"), ("5-18"), ("5-19"), ("5-20"), ("5-21"), ("5-22"), ("5-23"),
          ("6-00"), ("6-01"), ("6-02"), ("6-03"), ("6-04"), ("6-05"), ("6-06"), ("6-07"), ("6-08"), ("6-09"), ("6-10"), ("6-11"), ("6-12"), ("6-13"), ("6-14"), ("6-15"), ("6-16"), ("6-17"), ("6-18"), ("6-19"), ("6-20"), ("6-21"), ("6-22"), ("6-23"),
          ("7-00"), ("7-01"), ("7-02"), ("7-03"), ("7-04"), ("7-05"), ("7-06"), ("7-07"), ("7-08"), ("7-09"), ("7-10"), ("7-11"), ("7-12"), ("7-13"), ("7-14"), ("7-15"), ("7-16"), ("7-17"), ("7-18"), ("7-19"), ("7-20"), ("7-21"), ("7-22"), ("7-23")
        )).toDF("allweekhour")


        val pivotedEventsAllHours = validDf.join(lscol, validDf.col("weekhour") === lscol.col("allweekhour"), "right")
          .drop(validDf("weekhour"))
          .withColumn("Count", count(col("allweekhour")).over(partitionBy(col("clientid"),col("antennaid"))))
          .groupBy(col("clientid"),col("antennaid"))
          .pivot("allweekhour")
          .agg(first("Count"))
          //.agg(count("allweekhour"))
          .filter(!col("clientid").isNull && !col("antennaid").isNull)
          .na.fill(0)




        validDf.printSchema()
        validDf.show()
//        pivotedEvents.printSchema()
//        pivotedEvents.show()
        pivotedEventsAllHours.printSchema()
        pivotedEventsAllHours.show()

        val totalEnrichedEvents = validDf.count()
        val filteredEvents = totalEvents-totalEnrichedEvents
        logger.info("en total hay: " + totalEvents + " eventos, se han escrito: " + totalEnrichedEvents + ". Se han filtrado por formato o falta de datos:" + filteredEvents)
        println("en total hay: " + totalEvents + " eventos, se han escrito: " + totalEnrichedEvents + ". Se han filtrado por formato o falta de datos:" + filteredEvents)


        validDf.coalesce(1).write.mode(SaveMode.Overwrite).parquet(eventsData)
        pivotedEventsAllHours.coalesce(1).write.mode(SaveMode.Overwrite).parquet(dataToModel)
        logger.info("Se ha escrito el fichero de eventos en HDFS")
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


  def validateDf(row: Row): Boolean = {

    try {
      //assume row.getString(1) with give Datetime string
      java.time.LocalDateTime.parse(row.getString(1), java.time.format.DateTimeFormatter.ofPattern(DATE_TIME_FORMAT_INPUT))
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



  //
  //  def getDayOfWeek(row: Row): String = {
  //    LocalDate.parse(row.getString(1), dformatter).getDayOfWeek.toString
  //  }


}
