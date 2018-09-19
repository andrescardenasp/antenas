package ml

//import sqlContext.implicits._
import java.net.URI

import com.typesafe.config.ConfigFactory
import common.utils
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SQLContext, _}

object modelPredictWeekHours {


  val logger: Logger = Logger.getLogger(this.getClass.getName)


  def modelPipeline(sc: SparkContext, sq: SQLContext) {

    val conf = sc.hadoopConfiguration
    val parameters = ConfigFactory.parseResources("properties.conf").resolve()
    val predictionsData = parameters.getString("hdfs.modeldata.predictions")
    val modelLocation = parameters.getString("hdfs.modeldata.model")
    val pipelineLocation = parameters.getString("hdfs.modeldata.pipeline")
    val hdfs = FileSystem.get(new URI(parameters.getString("hdfs.url")), conf)


    try {


      println("Comienza la carga de los datos para alimentar al modelo.")

      val dfEventsAntenasWeekHours = sq.read.parquet(parameters.getString("hdfs.modeldata.data"))


      //val kMeansPredictionModel = pipeline.fit(dfEvents)
      //val kMeansPredictionModel = PipelineModel.read.load(modelLocation)
      val kMeansPredictionModel = KMeansModel.read.load(modelLocation)
      val assembler = new VectorAssembler().setInputCols(common.utils.weekHoursList.toArray).setOutputCol("features")
      val assembled = assembler.transform(dfEventsAntenasWeekHours)
      val predictionResult = kMeansPredictionModel.transform(assembled).drop("features")

      predictionResult.show()
      predictionResult.printSchema()

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
