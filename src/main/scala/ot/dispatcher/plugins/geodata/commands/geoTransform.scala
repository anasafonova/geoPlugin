package ot.dispatcher.plugins.geodata.commands

import geotrellis.proj4.LatLng
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import ot.dispatcher.plugins.externaldata.internals.QuotesParser
//import org.apache.spark.implicits._
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}
import org.apache.spark.sql.functions._

class geoTransform(sq: SimpleQuery, utils: PluginUtils) extends QuotesParser(sq, utils) {

  import utils._
  //import geotrellis.proj4.{CRS}

  val requiredKeywords: Set[String] = Set("xcol", "ycol", "sourceBase", "targetBase", "targetCrs", "sourceCrs")
/*  val optionalKeywords: Set[String] = Set(
    "targetCrs", "sourceCrs"
  )*/

  val geoTransf = udf((x: Double, y: Double, srcProj4: String, trgProj4: String) => {
    import geotrellis.proj4.{Transform, LatLng, CRS}

    val src = CRS.fromString(srcProj4)
    val trg = CRS.fromString(trgProj4)

    val new_xy = Transform(src, trg)(x, y)
    new_xy //.toString().replace("(","").replace(")","").split(",")
  })

  override def transform(_df: DataFrame): DataFrame = {
    import geotrellis.proj4.{CRS, LatLng}

    val srcBase: String = getKeyword("sourceBase").get
    val trgBase: String = getKeyword("targetBase").get
    val sourceCrs = getKeyword("sourceCrs").get.replace("'", "").replace("\"", "")
    val targetCrs = getKeyword("targetCrs").get.replace("'", "").replace("\"", "")
    val xcol: String = getKeyword("xcol").get
    val ycol: String = getKeyword("ycol").get

    val srcCrs: CRS = (srcBase, sourceCrs) match {
      //case (_, null) =>
      //  LatLng
      case ("epsg", _) =>
        val srcCrs = CRS.fromEpsgCode(sourceCrs.toInt)
        srcCrs
      case ("string", _) =>
        val srcCrs = CRS.fromString(sourceCrs)
        srcCrs
      case ("name", _) =>
        val srcCrs = CRS.fromName(sourceCrs)
        srcCrs
      case (_, _) =>
        sendError("Unknown transform base.")
    }

    val trgCrs: CRS = (trgBase, targetCrs) match {
      case ("epsg", _) =>
        val trgCrs = CRS.fromEpsgCode(targetCrs.toInt)
        trgCrs
      case ("string", _) =>
        val trgCrs = CRS.fromString(targetCrs)
        trgCrs
      case ("name", _) =>
        val trgCrs = CRS.fromName(targetCrs)
        trgCrs
      case (_, _) =>
        sendError("Unknown transform base.")
    }

    val srcProj4 = srcCrs.toProj4String
    val trgProj4 = trgCrs.toProj4String

    /*val trgCrs: CRS = targetCrs match {
      case null => LatLng
      case _ => CRS.fromEpsgCode(targetCrs.toInt)
    }

    log.debug(s"srcCrs: $srcCrs.")
    log.debug(s"trgCrs: $trgCrs.")*/

    //spark.udf.register("geoTransf", geoTrans)

    val df2 = _df
      .withColumn("src", lit(srcProj4))
      .withColumn("trg", lit(trgProj4))
      .withColumn("xx", geoTransf(col("x"), col("y"), col("src"), col("trg")).getItem("_1"))
      .withColumn("yy", geoTransf(col("x"), col("y"), col("src"), col("trg")).getItem("_2"))
      //.withColumn("transformedXY", geoTransf(col("x"), col("y"), col("src"), col("trg")))

    //val df2 = _df.collect().foreach(row => Transform(row.getAs[Double](col(xcol)), row.getAs[Double](col(ycol))))
    //val df2 = _df.collect().foreach(row => row.getAs[String]("X"))
    //val df = df2.toDf()

//    val fromCrsToCrs = Transform(srcCrs, trgCrs)
//
//    _df.map(row => row.getAs[Double]col("x"))
//
//    fromCrsToCrs(x, y)
//
//    val df = _df.withColumn("newCoords", fromCrsToCrs(col("x"), double(col("y"))))

    //val srcCrsToTrgCrs = Transform(srcCrs, trgCrs)
    //val df = srcCrsToTrgCrs(_df)

    //val targetCols = Seq("x", "y", "newCoords")
    val df = df2.drop("src", "trg")
    df
  }

}