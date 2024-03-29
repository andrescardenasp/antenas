import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import dataLoading._
import ml._
import org.apache.log4j.Logger


object Main {


  def main(args: Array[String]) {
    val blockSize = 1024 * 1024 * 16
    val parameters = ConfigFactory.parseResources("properties.conf").resolve()
    val logger = Logger.getLogger(this.getClass.getName)
    val sparkConf = new SparkConf().setAppName("Antenas-Monetizacion").setMaster("local[3]")
    val sc = new SparkContext(sparkConf)
    val sq = new SQLContext(sc)
    sc.hadoopConfiguration.setInt("dfs.blocksize", blockSize)
    sc.hadoopConfiguration.setInt("parquet.block.size", blockSize)
    val execMode = args(0).toString

    // Check number of parameters
    if (args.length != 1) {
      logger.error("=> wrong parameters number")
      System.err.println("Usage: Only one parameter that indicates execution mode: 1 to train a Kmeans model / 2 to predict using the trained modelTrain")
      System.exit(1)
    } else {
      logger.info(s"Executed with the parameter=${args(0)}.")
      println(s"Executed with the parameter=${args(0)}.")
      println(execMode)
      // Check for valid mode
      if (execMode != "1" && execMode != "2" && execMode != "3" && execMode != "4" && execMode != "5" && execMode != "6") {
        logger.error("=> wrong execution mode")
        System.err.println("wrong execution mode: Usage: Only one parameter that indicates execution mode: 1 to train a Kmeans model / 2 to predict using the trained modelTrain")
        System.exit(1)
      }

    }


    if (execMode == "1") { // 1 = Loads and Train
      loadData()
      logger.info("Comienza Carga y entrenamiento del modelo Kmeans.")
      modelTrainWeekHours.modelPipeline(sc, sq)
      logger.info("Termina Carga y Termina el entrenamiento del modelo Kmeans.")
    }

    else if (execMode == "2") { // 2 = Loads and Predict
      loadData()
      logger.info("Comienza Carga y predicción con el Kmeans previamente entrenado.")
      modelPredictWeekHours.modelPipeline(sc, sq)
      logger.info("Termina Carga y predicción con el Kmeans previamente entrenado.")
    }

    else if (execMode == "3") { // 3 = Get schemas mode
      obtainSchemas()
    }

    else if (execMode == "4") { // 4 = OnlyLoadMode
      loadData()
    }

    else if (execMode == "5") { // 5 = OnlyTrainMode
      logger.info("Comienza entrenamiento del modelo Kmeans.")
      modelTrainWeekHours.modelPipeline(sc, sq)
      logger.info("Termina el entrenamiento del modelo Kmeans.")
    }

    else if (execMode == "6") { // 4 = OnlyPredictMode

      logger.info("Comienza predicción con el Kmeans previamente entrenado.")
      modelPredictWeekHours.modelPipeline(sc, sq)
      logger.info("Termina predicción con el Kmeans previamente entrenado.")
    }


    def loadData(): Unit = {

            // Carga y limpieza de ficheros
            logger.info("Comienza carga y limpieza de Ciudades.")
            cities.load(sc, sq)
            logger.info("Termina carga y limpieza de Ciudades.")

            logger.info("Comienza carga y limpieza de Antenas.")
            antennas.load(sc, sq)
            logger.info("Termina carga y limpieza de Antenas.")

            logger.info("Comienza carga y limpieza de clientes.")
            clients.load(sc, sq)
            logger.info("Termina carga y limpieza de clientes.")

            logger.info("Comienza carga y limpieza de eventos.")
            events.load(sc, sq)
            logger.info("Termina carga y limpieza de eventos.")

    }

    def obtainSchemas(): Unit = {

      println("dfCities:")
      val dfCities = sq.read.parquet(parameters.getString("hdfs.cleanData.cities"))
      dfCities.printSchema()
      dfCities.show()

      println("dfAntennas:")
      val dfAntennas = sq.read.parquet(parameters.getString("hdfs.cleanData.antennas"))
      dfAntennas.printSchema()
      dfAntennas.show()

      println("dfClients:")
      val dfClients = sq.read.parquet(parameters.getString("hdfs.cleanData.clients"))
      dfClients.printSchema()
      dfClients.show()

      println("dfEvents:")
      val dfEvents = sq.read.parquet(parameters.getString("hdfs.cleanData.events"))
      dfEvents.printSchema()
      dfEvents.show()

      println("dfEventsPredictions:")
      val dfEventsPredictions = sq.read.parquet(parameters.getString("hdfs.modeldata.predictions"))
      dfEventsPredictions.printSchema()
      dfEventsPredictions.show()


    }

  }
}
