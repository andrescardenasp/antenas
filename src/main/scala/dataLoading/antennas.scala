package dataLoading

import com.typesafe.config.ConfigFactory
import grizzled.slf4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, _}
import org.apache.spark.sql.functions._


object antennas {
  //  val ages = Seq(42, 75, 29, 64)
  //  println(s"The oldest person is ${ages.max}")

  def load(sc: SparkContext, sq: SQLContext) {
    val logger = Logger(this.getClass)
    val parameters = ConfigFactory.parseResources("properties.conf")
    val citiesInput = parameters.getString("hdfs.input.cities")
    val citiesData = parameters.getString("hdfs.cleanData.cities")

    try {
      val df = sq.read.option("header", "true").option("delimiter", ";").csv(citiesInput).distinct()
      df.printSchema()
      //    df.show()
      val dfGeo = df.withColumn("lat1", split(col("X1"), ",").getItem(0))
        .withColumn("lon1", split(col("X1"), ",").getItem(1))
        .withColumn("lat2", split(col("X2"), ",").getItem(0))
        .withColumn("lon2", split(col("X2"), ",").getItem(1))
        .withColumn("lat3", split(col("X3"), ",").getItem(0))
        .withColumn("lon3", split(col("X3"), ",").getItem(1))
        .withColumn("lat4", split(col("X4"), ",").getItem(0))
        .withColumn("lon4", split(col("X4"), ",").getItem(1))
        .withColumn("lat5", split(col("X5"), ",").getItem(0))
        .withColumn("lon5", split(col("X5"), ",").getItem(1))
        .drop("X1")
        .drop("X2")
        .drop("X3")
        .drop("X4")
        .drop("X5")
      dfGeo.show()
      dfGeo.coalesce(1).write.mode(SaveMode.Overwrite).parquet(citiesData)
      logger.info("Se ha escrito el fichero en HDFS")
    } catch {

      case e: Exception =>
        logger.error("Fallo en la limpieza y escritura de ciudades en HDFS: " + e.printStackTrace())
        println("Fallo en la limpieza y escritura de ciudades en HDFS: ", e.printStackTrace())
        sys.exit(1)
    }

  }


}
