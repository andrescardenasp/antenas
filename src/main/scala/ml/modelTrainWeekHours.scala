package ml

import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SQLContext

object modelTrainWeekHours {

  val logger: Logger = Logger.getLogger(this.getClass.getName)

  def modelPipeline(sc: SparkContext, sq: SQLContext) {
    val conf = sc.hadoopConfiguration
    val parameters = ConfigFactory.parseResources("properties.conf").resolve()
    val modelLocation = parameters.getString("hdfs.modeldata.model")

    try {

      println("Comienzo la carga de los datos para alimentar al modelo.")
      val dfEventsAntenasWeekHours = sq.read.parquet(parameters.getString("hdfs.modeldata.data"))

      println("En total hay registros en el df de horas de la semana:")
      println(dfEventsAntenasWeekHours.count())
      dfEventsAntenasWeekHours.show()
      // Assembler definition and transformation
      val assembler = new VectorAssembler().setInputCols(common.utils.weekHoursList.toArray).setOutputCol("features")
      val assembled = assembler.transform(dfEventsAntenasWeekHours)

      // Model definition
      val kmeans = new KMeans().setK(2).setFeaturesCol("features").setPredictionCol("prediction")
      // Model Training
      val kMeansPredictionModel = kmeans.fit(assembled)
      // printing of clusters centers
      for ( center <- kMeansPredictionModel.clusterCenters) println(center)
      //SAving trained model
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
