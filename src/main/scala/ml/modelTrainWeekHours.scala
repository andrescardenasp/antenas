package ml

//import sqlContext.implicits._
import java.net.URI

import com.typesafe.config.ConfigFactory
import common.utils
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.col


object modelTrainWeekHours {


  val logger: Logger = Logger.getLogger(this.getClass.getName)


  def modelPipeline(sc: SparkContext, sq: SQLContext) {
    val conf = sc.hadoopConfiguration
    val parameters = ConfigFactory.parseResources("properties.conf").resolve()
    val modelLocation = parameters.getString("hdfs.modeldata.model")
    val pipelineLocation = parameters.getString("hdfs.modeldata.pipeline")
    val hdfs = FileSystem.get(new URI(parameters.getString("hdfs.url")), conf)


    try {

      println("Comienzo la carga de los datos para alimentar al modelo.")
      val dfEventsAntenasWeekHours = sq.read.parquet(parameters.getString("hdfs.modeldata.data"))

      println("En total hay registros en el df de horas de la semana:")
      println(dfEventsAntenasWeekHours.count())


      val assembler = new VectorAssembler().setInputCols(common.utils.weekHoursList.toArray).setOutputCol("features")

      val kmeans = new KMeans().setK(2).setFeaturesCol("features").setPredictionCol("prediction")

      val pipeline = new Pipeline().setStages(Array(
        assembler,
        kmeans
      ))

      dfEventsAntenasWeekHours.show()

      val kMeansPredictionModel = pipeline.fit(dfEventsAntenasWeekHours)

      pipeline.write.overwrite.save(pipelineLocation)
      kMeansPredictionModel.write.overwrite().save(modelLocation)

      logger.info("Se ha guardado el pipeline y el modelo entrenado")
      println("Se ha guardado el pipeline y el modelo entrenado")


    } catch {

      case e: Exception =>
        logger.error("Fallo en la generacion del modelo: " + e.printStackTrace())
        println("Fallo en la generacion del modelo: ", e.printStackTrace())
        sys.exit(1)

    }


  }
}
