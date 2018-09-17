package common

import org.apache.spark.sql.functions.udf
//import common.PolygonUtils
import org.apache.spark.sql.Row

object utils {

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


  def antennaInCityFilter(row: Row): Boolean = {

    val CityPolList = List(
      GeoPoint(row.getDouble(row.fieldIndex("lat1")), row.getDouble(row.fieldIndex("lon1"))),
      GeoPoint(row.getDouble(row.fieldIndex("lat2")), row.getDouble(row.fieldIndex("lon2"))),
      GeoPoint(row.getDouble(row.fieldIndex("lat3")), row.getDouble(row.fieldIndex("lon3"))),
      GeoPoint(row.getDouble(row.fieldIndex("lat4")), row.getDouble(row.fieldIndex("lon4"))),
      GeoPoint(row.getDouble(row.fieldIndex("lat5")), row.getDouble(row.fieldIndex("lon5")))
    )

    val CityPol = Polygon(CityPolList)

    val antennaPoint = GeoPoint(row.getDouble(row.fieldIndex("y")), row.getDouble(row.fieldIndex("x")))

    PolygonUtils.pointInPolygon(antennaPoint,CityPol)


  }

  val weekHoursList= Seq(
    ("1-00"), ("1-01"), ("1-02"), ("1-03"), ("1-04"), ("1-05"), ("1-06"), ("1-07"), ("1-08"), ("1-09"), ("1-10"), ("1-11"), ("1-12"), ("1-13"), ("1-14"), ("1-15"), ("1-16"), ("1-17"), ("1-18"), ("1-19"), ("1-20"), ("1-21"), ("1-22"), ("1-23"),
    ("2-00"), ("2-01"), ("2-02"), ("2-03"), ("2-04"), ("2-05"), ("2-06"), ("2-07"), ("2-08"), ("2-09"), ("2-10"), ("2-11"), ("2-12"), ("2-13"), ("2-14"), ("2-15"), ("2-16"), ("2-17"), ("2-18"), ("2-19"), ("2-20"), ("2-21"), ("2-22"), ("2-23"),
    ("3-00"), ("3-01"), ("3-02"), ("3-03"), ("3-04"), ("3-05"), ("3-06"), ("3-07"), ("3-08"), ("3-09"), ("3-10"), ("3-11"), ("3-12"), ("3-13"), ("3-14"), ("3-15"), ("3-16"), ("3-17"), ("3-18"), ("3-19"), ("3-20"), ("3-21"), ("3-22"), ("3-23"),
    ("4-00"), ("4-01"), ("4-02"), ("4-03"), ("4-04"), ("4-05"), ("4-06"), ("4-07"), ("4-08"), ("4-09"), ("4-10"), ("4-11"), ("4-12"), ("4-13"), ("4-14"), ("4-15"), ("4-16"), ("4-17"), ("4-18"), ("4-19"), ("4-20"), ("4-21"), ("4-22"), ("4-23"),
    ("5-00"), ("5-01"), ("5-02"), ("5-03"), ("5-04"), ("5-05"), ("5-06"), ("5-07"), ("5-08"), ("5-09"), ("5-10"), ("5-11"), ("5-12"), ("5-13"), ("5-14"), ("5-15"), ("5-16"), ("5-17"), ("5-18"), ("5-19"), ("5-20"), ("5-21"), ("5-22"), ("5-23"),
    ("6-00"), ("6-01"), ("6-02"), ("6-03"), ("6-04"), ("6-05"), ("6-06"), ("6-07"), ("6-08"), ("6-09"), ("6-10"), ("6-11"), ("6-12"), ("6-13"), ("6-14"), ("6-15"), ("6-16"), ("6-17"), ("6-18"), ("6-19"), ("6-20"), ("6-21"), ("6-22"), ("6-23"),
    ("7-00"), ("7-01"), ("7-02"), ("7-03"), ("7-04"), ("7-05"), ("7-06"), ("7-07"), ("7-08"), ("7-09"), ("7-10"), ("7-11"), ("7-12"), ("7-13"), ("7-14"), ("7-15"), ("7-16"), ("7-17"), ("7-18"), ("7-19"), ("7-20"), ("7-21"), ("7-22"), ("7-23")
  )


}
