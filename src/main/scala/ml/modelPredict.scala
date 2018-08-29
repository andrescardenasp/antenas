package ml


//import sqlContext.implicits._
import java.net.URI

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{SQLContext, _}
import common.utils

object modelPredict {


  val logger: Logger = Logger.getLogger(this.getClass.getName)


  def modelPipeline(sc: SparkContext, sq: SQLContext) {
    val conf = sc.hadoopConfiguration
    val parameters = ConfigFactory.parseResources("properties.conf").resolve()
    val predictionsData = parameters.getString("hdfs.modeldata.predictions")
    val modelLocation = parameters.getString("hdfs.modeldata.model")
    val pipelineLocation = parameters.getString("hdfs.modeldata.pipeline")
    val hdfs = FileSystem.get(new URI(parameters.getString("hdfs.url")), conf)


    try {


      println("Comienza la carga de los datos en limpio para alimentar al modelo.")
      val dfAntennas = sq.read.parquet(parameters.getString("hdfs.cleanData.antennas"))
      val dfClients = sq.read.parquet(parameters.getString("hdfs.cleanData.clients"))
      val dfEvents = sq.read.parquet(parameters.getString("hdfs.cleanData.events"))
        .join(dfAntennas, "antennaid")
        .join(dfClients, "clientid")
        .drop("Date")
        .drop("Time")
        .drop("Month")
        .drop("Year")
        .drop("Day")
        .drop("Minute")
        .withColumn("Hora", utils.toInt(col("hour")))
        .withColumn("Edad", utils.toInt(col("age")))

      dfEvents.show()

      //val kMeansPredictionModel = pipeline.fit(dfEvents)
      val kMeansPredictionModel = PipelineModel.read.load(modelLocation)

      val predictionResult = kMeansPredictionModel.transform(dfEvents)
        .drop("genderVec", "nationalityVec", "civilVec", "economicVec", "CiudadVec", "weekVec", "features", "genderIndex", "nationalityIndex", "civilIndex", "economicIndex", "CiudadIndex", "weekIndex", "Hora", "Edad")
      predictionResult.show()

      predictionResult.coalesce(1).write.mode(SaveMode.Overwrite).parquet(predictionsData)
      logger.info("Se ha escrito fichero con los resultados despues de aplicar Kmeans")
      println("Se ha escrito fichero con los resultados despues de aplicar Kmeans")


    } catch {

      case e: Exception =>
        logger.error("Fallo en la generacion del modelo: " + e.printStackTrace())
        println("Fallo en la generacion del modelo: ", e.printStackTrace())
        sys.exit(1)

    }


  }
}
