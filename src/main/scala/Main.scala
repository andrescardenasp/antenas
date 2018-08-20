import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import dataLoading._
import ml._
import org.apache.log4j.Logger



object Main {



  def main(args: Array[String]) {
    val blockSize = 1024 * 1024 * 16
    //val logger = Logger(this.getClass)
    val logger = Logger.getLogger(this.getClass.getName)
    val sparkConf = new SparkConf().setAppName("Antenas-Monetizacion").setMaster("local[3]")
    val sc = new SparkContext(sparkConf)
    val sq = new SQLContext(sc)
    sc.hadoopConfiguration.setInt( "dfs.blocksize", blockSize )
    sc.hadoopConfiguration.setInt( "parquet.block.size", blockSize )


    model


    // Carga y limpieza de ficheros
    logger.info("Comienza carga y limpieza de Ciudades")
    cities.load(sc, sq)
    logger.info("Termina carga y limpieza de Ciudades")

    logger.info("Comienza carga y limpieza de Antenas")
    antennas.load(sc, sq)
    logger.info("Termina carga y limpieza de Antenas")

    logger.info("Comienza carga y limpieza de clientes")
    clients.load(sc, sq)
    logger.info("Termina carga y limpieza de clientes")

    logger.info("Comienza carga y limpieza de eventos")
    events.load(sc, sq)
    logger.info("Termina carga y limpieza de eventos")

    logger.info("Comienza la generación y predicción con el modelo Kmeans ")
    model.modelPipeline(sc, sq)
    logger.info("Termina la generación y predicción con el modelo Kmeans ")


  }
}
