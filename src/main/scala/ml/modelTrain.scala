package ml


//import sqlContext.implicits._
import java.net.URI

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types._
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StructField, StructType, _}
import org.apache.spark.sql.{SQLContext, _}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions.{col, udf}
import common.utils


object modelTrain {


  val logger: Logger = Logger.getLogger(this.getClass.getName)


  def modelPipeline(sc: SparkContext, sq: SQLContext) {
    val conf = sc.hadoopConfiguration
    val parameters = ConfigFactory.parseResources("properties.conf").resolve()
    val modelLocation = parameters.getString("hdfs.modeldata.model")
    val pipelineLocation = parameters.getString("hdfs.modeldata.pipeline")
    val hdfs = FileSystem.get(new URI(parameters.getString("hdfs.url")), conf)


    try {


      val indexerGender = new StringIndexer().setInputCol("Gender").setOutputCol("genderIndex")
      val indexerNat = new StringIndexer().setInputCol("Nationality").setOutputCol("nationalityIndex")
      val indexerCivil = new StringIndexer().setInputCol("CivilStatus").setOutputCol("civilIndex")
      val indexerEconomic = new StringIndexer().setInputCol("SocioeconomicLevel").setOutputCol("economicIndex")
      //val indexerCity = new StringIndexer().setInputCol("Ciudad").setOutputCol("CiudadIndex")
      val indexerDW = new StringIndexer().setInputCol("dayofweek").setOutputCol("weekIndex")

      val encoderGender = new OneHotEncoder().setInputCol("genderIndex").setOutputCol("genderVec")
      val encoderNat = new OneHotEncoder().setInputCol("nationalityIndex").setOutputCol("nationalityVec")
      val encoderCivil = new OneHotEncoder().setInputCol("civilIndex").setOutputCol("civilVec")
      val encoderEconomic = new OneHotEncoder().setInputCol("economicIndex").setOutputCol("economicVec")
     // val encoderCity = new OneHotEncoder().setInputCol("CiudadIndex").setOutputCol("CiudadVec")
      val encoderDW = new OneHotEncoder().setInputCol("weekIndex").setOutputCol("weekVec")

      val assembler = new VectorAssembler().setInputCols(Array("Hora", "Edad", "genderVec", "nationalityVec", "civilVec", "economicVec", "cityId","weekVec")).setOutputCol("features")
      //val assembler = new VectorAssembler().setInputCols(Array("Hora", "Edad", "genderVec", "nationalityVec", "civilVec", "economicVec", "CiudadVec", "weekVec")).setOutputCol("features")
      //"Hour","Age",
      val kmeans = new KMeans().setK(2).setFeaturesCol("features").setPredictionCol("prediction")

      val pipeline = new Pipeline().setStages(Array(
        indexerGender,
        indexerNat,
        indexerCivil,
        indexerEconomic,
        //indexerCity,
        indexerDW,
        encoderGender,
        encoderNat,
        encoderCivil,
        encoderEconomic,
        //encoderCity,
        encoderDW,
        assembler,
        kmeans
      ))



      println("Comienzo la carga de los datos en limpio para alimentar al modelo.")
      val dfAntennas = sq.read.parquet(parameters.getString("hdfs.cleanData.antennas"))
      val dfClients = sq.read.parquet(parameters.getString("hdfs.cleanData.clients"))
      val dfEvents = sq.read.parquet(parameters.getString("hdfs.cleanData.events"))
        .join(dfAntennas, "AntennaId")
        .join(dfClients, "ClientId")
        .drop("Date")
        .drop("Time")
        .drop("Month")
        .drop("Year")
        .drop("Day")
        .drop("Minute")
        .withColumn("Hora", utils.toInt(col("Hour")))
        .withColumn("Edad", utils.toInt(col("Age")))

      dfEvents.show()

      val kMeansPredictionModel = pipeline.fit(dfEvents)


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
