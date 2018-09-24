package ml



import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{SQLContext, _}
import org.apache.commons.lang3.StringUtils

object modelPredictWeekHours {

  val logger: Logger = Logger.getLogger(this.getClass.getName)

  def modelPipeline(sc: SparkContext, sq: SQLContext) {

    val parameters = ConfigFactory.parseResources("properties.conf").resolve()
    val predictionsData = parameters.getString("hdfs.modeldata.predictions")
    val modelLocation = parameters.getString("hdfs.modeldata.model")

    try {

      println("Comienza la carga de los datos para alimentar al modelo.")
      // Data reading
      val dfEventsAntenasWeekHours = sq.read.parquet(parameters.getString("hdfs.modeldata.data"))
      // Reading of trained model
      val kMeansPredictionModel = KMeansModel.read.load(modelLocation)
      // Assembler definition and transformation
      val assembler = new VectorAssembler().setInputCols(common.utils.weekHoursList.toArray).setOutputCol("features")
      val assembled = assembler.transform(dfEventsAntenasWeekHours)

      // Using model to predict
      val predictionResult = kMeansPredictionModel.transform(assembled).drop("features")

      predictionResult.show()
      predictionResult.printSchema()

      // Saving Results
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
