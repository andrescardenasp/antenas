package ml

//import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
//import org.apache.spark.mllib.linalg.Vectors
//import sqlContext.implicits._
import java.net.URI

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{SQLContext, _}


object modelPredict {


  val logger = Logger.getLogger(this.getClass.getName)


  def modelPipeline(sc: SparkContext, sq: SQLContext) {
    val conf = sc.hadoopConfiguration
    val parameters = ConfigFactory.parseResources("properties.conf").resolve()
    val predictionsData = parameters.getString("hdfs.modeldata.predictions")
    val modelLocation = parameters.getString("hdfs.modeldata.model")
    val pipelineLocation = parameters.getString("hdfs.modeldata.pipeline")
    val hdfs = FileSystem.get(new URI(parameters.getString("hdfs.url")), conf)



    try {

      def getCity(antenna: String): String = {
        if (antenna == "A01") "Madrid"
        else "Logroño"

      }

      val cityUdf = udf(getCity _)

/*
      val indexerGender = new StringIndexer().setInputCol("Gender").setOutputCol("genderIndex")

      val indexerNat = new StringIndexer().setInputCol("Nationality").setOutputCol("nationalityIndex")
      val indexerCivil = new StringIndexer().setInputCol("CivilStatus").setOutputCol("civilIndex")
      val indexerEconomic = new StringIndexer().setInputCol("SocioeconomicLevel").setOutputCol("economicIndex")
      val indexerCity = new StringIndexer().setInputCol("Ciudad").setOutputCol("CiudadIndex")
      val indexerDW = new StringIndexer().setInputCol("dayofweek").setOutputCol("weekIndex")

      val encoderGender = new OneHotEncoder().setInputCol("genderIndex").setOutputCol("genderVec")
      val encoderNat = new OneHotEncoder().setInputCol("nationalityIndex").setOutputCol("nationalityVec")
      val encoderCivil = new OneHotEncoder().setInputCol("civilIndex").setOutputCol("civilVec")
      val encoderEconomic = new OneHotEncoder().setInputCol("economicIndex").setOutputCol("economicVec")
      val encoderCity = new OneHotEncoder().setInputCol("CiudadIndex").setOutputCol("CiudadVec")
      val encoderDW = new OneHotEncoder().setInputCol("weekIndex").setOutputCol("weekVec")

      val assembler = new VectorAssembler().setInputCols(Array("Hora","Edad","genderVec","nationalityVec","civilVec","economicVec","CiudadVec","weekVec")).setOutputCol("features")
//"Hour","Age",
      val kmeans = new KMeans().setK(2).setFeaturesCol("features").setPredictionCol("prediction")

      val pipeline = new Pipeline().setStages(Array(
        indexerGender,
        indexerNat,
        indexerCivil,
        indexerEconomic,
        indexerCity,
        indexerDW,
        encoderGender,
        encoderNat,
        encoderCivil,
        encoderEconomic,
        encoderCity,
        encoderDW,
        assembler,
        kmeans
      ))
*/
      val toInt    = udf[Int, String]( _.toInt)
      val toDouble = udf[Double, String]( _.toDouble)

println("Comienzo la carga de los datos en limpio para alimentar al modelo.")
      val dfAntennas = sq.read.parquet(parameters.getString("hdfs.cleanData.antennas"))
      val dfClients = sq.read.parquet(parameters.getString("hdfs.cleanData.clients"))
      val dfEvents = sq.read.parquet(parameters.getString("hdfs.cleanData.events"))
        //.join(dfAntennas, "AntennaId")
        .join(dfClients, "ClientId")
        .drop("Date")
        .drop("Time")
        .drop("Month")
        .drop("Year")
        .drop("Day")
        .drop("Minute")
        .withColumn("Ciudad", cityUdf(col("AntennaId")))
      .withColumn("Hora", toInt(col("Hour")) )
        .withColumn("Edad", toInt(col("Age")) )

      dfEvents.show()

      //val kMeansPredictionModel = pipeline.fit(dfEvents)
      val kMeansPredictionModel = PipelineModel.read.load(modelLocation)

      val predictionResult = kMeansPredictionModel.transform(dfEvents)
          .drop("genderVec","nationalityVec", "civilVec", "economicVec", "CiudadVec", "weekVec", "features","genderIndex","nationalityIndex","civilIndex","economicIndex","CiudadIndex","weekIndex","Hora", "Edad")
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
