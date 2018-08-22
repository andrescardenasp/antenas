package common

import org.apache.spark.sql.functions.udf
import common.PolygonUtils

object utils {

  def getCity(antenna: String): String = {
    if (antenna == "A01") "Madrid"
    else "Logroño"

  }

  val toInt = udf[Int, String](_.toInt)
  val toDouble = udf[Double, String](_.toDouble)

  def getPoint(x: Double, y: Double): GeoPoint = {
    GeoPoint(x, y)
  }

  val getPointUDF = udf[GeoPoint, Double, Double](getPoint(_, _))


  def getCityPolygon(lat1: Double, lon1: Double,
                     lat2: Double, lon2: Double,
                     lat3: Double, lon3: Double,
                     lat4: Double, lon4: Double,
                     lat5: Double, lon5: Double): Polygon = {

    val CityPolList = List(
      GeoPoint(lat1, lon1),
      GeoPoint(lat2, lon2),
      GeoPoint(lat3, lon3),
      GeoPoint(lat4, lon4),
      GeoPoint(lat5, lon5)
    )

    val CityPol = Polygon(CityPolList)
    CityPol
  }

  val getCityPolygonUDF = udf[Polygon, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double](getCityPolygon(_, _, _, _, _, _, _, _, _, _))


  def getAntennaInCity(geoPoint: GeoPoint, poly: Polygon): Boolean = {
    PolygonUtils.pointInPolygon(geoPoint,poly)
  }

  val pointInPolygonUDF = udf[Boolean, GeoPoint, Polygon](getAntennaInCity(_,_))

  val testudf = udf[Polygon, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double](getCityPolygon(_, _, _, _, _, _, _, _, _, _))

}
