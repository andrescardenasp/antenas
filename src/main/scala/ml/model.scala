package ml

//import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
//import org.apache.spark.mllib.linalg.Vectors
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


object model {


  val logger = Logger.getLogger(this.getClass.getName)


  def modelPipeline(sc: SparkContext, sq: SQLContext) {
    val conf = sc.hadoopConfiguration
    val parameters = ConfigFactory.parseResources("properties.conf").resolve()
    val eventsData = parameters.getString("hdfs.cleanData.events")
    val hdfs = FileSystem.get(new URI(parameters.getString("hdfs.url")), conf)
    val dfAntennas = sq.read.parquet(parameters.getString("hdfs.cleanData.antennas"))
    val dfClients = sq.read.parquet(parameters.getString("hdfs.cleanData.clients"))


    try {

      def getCity(antenna: String): String = {
        if (antenna == "A01") "Madrid"
        else "Logroño"

      }

      val cityUdf = udf(getCity _)
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
        .withColumn("Ciudad", cityUdf(col("AntennaId")))

      dfEvents.show()


    } catch {

      case e: Exception =>
        logger.error("Fallo en la generacion del modelo: " + e.printStackTrace())
        println("Fallo en la generacion del modelo: ", e.printStackTrace())
        sys.exit(1)

    }

    /*  val indexer = new StringIndexer().setInputCol("gender").setOutputCol("genderIndex")
  val encoder = new OneHotEncoder().setInputCol("genderIndex").setOutputCol("genderVec")
  val assembler = new VectorAssembler().setInputCols(Array("income","genderVec")).setOutputCol("features")
  val kmeans = new KMeans().setK(2).setFeaturesCol("features").setPredictionCol("prediction")

  val pipeline = new Pipeline().setStages(Array(indexer, encoder, assembler, kmeans))

  val kMeansPredictionModel = pipeline.fit(input)

  val predictionResult = kMeansPredictionModel.transform(input)
  */
  }
}
