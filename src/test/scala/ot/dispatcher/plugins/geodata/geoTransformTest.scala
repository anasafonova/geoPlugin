package ot.dispatcher.plugins.geodata.commands

import java.io.File
import java.nio.file.{Path, Paths}

import org.apache.spark.sql.DataFrame
import ot.dispatcher.plugins.geodata.commands.geoTransform
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class geoTransformTest extends CommandTest {
  override val dataset: String = """[
                                   |{"x":"60.57","y":"76.36"},
                                   |{"x":"57.67","y":"38.38"}
                                   |]""".stripMargin


/*  override val dataset: String = """[
                           |{"y":"6554887","x":"3616977.4"},
                           |{"y":"6554864.9","x":"3617000.6"}
                           |]""".stripMargin*/

//  override val dataset: String = """[
//                                   |{"y":"6564418.63","x":"448449.26"},
//                                   |{"y":"6567185.81","x":"450179.76"}
//                                   |]""".stripMargin

  val initialDf: DataFrame = jsonToDf(dataset)

  //val initDf: DataFrame = jsonToDf(initData)

  test("Test 0. Command: | geoTransform base=epsg sourceCrs=EPSG:28413 targetCrs=EPSG:28413") {
    initialDf.show()
    val simpleQuery = SimpleQuery(""" sourceBase=epsg sourceCrs=28413 targetBase=epsg targetCrs=28413 xcol=x ycol=y """)
    val commandGeoTransform = new geoTransform(simpleQuery, utils)
    println(commandGeoTransform)
    println(simpleQuery)
    val actual = execute(commandGeoTransform)
    println(actual)
    val expected = """[
  {"x":"60.57","y":"76.36","transformedXY":"(60.57,76.36)"},
  {"x":"57.67","y":"38.38","transformedXY":"(57.67,38.38)"}
  ]"""
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("""Test 1. Command: | geoTransform base=name sourceCrs="EPSG:28413" targetCrs="EPSG:28413"""") {
    initialDf.show()
    val simpleQuery = SimpleQuery(""" sourceBase=name sourceCrs="EPSG:28413" targetBase=name targetCrs="EPSG:2002" xcol=x ycol=y """)
    val commandGeoTransform = new geoTransform(simpleQuery, utils)
    println(commandGeoTransform)
    println(simpleQuery)
    val actual = execute(commandGeoTransform)
    println(actual)
    val expected = """[
{"x":"60.57","y":"76.36","transformedXY":"(8.479251780874155E7,5119.518018340316)"},
{"x":"57.67","y":"38.38","transformedXY":"(8.479287166592991E7,-16849.447744534555)"}
]"""
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 2. Command: | geoTransform base=epsg/string sourceCrs=EPSG:28413 targetCrs=EPSG:28413") {
    initialDf.show()
    val simpleQuery = SimpleQuery(""" sourceBase=epsg sourceCrs=28413 targetBase=string targetCrs="+proj=longlat +ellps=krass +towgs84=23.92,-141.27,-80.9,0,0.35,0.82,-0.12 +no_defs +units=m" xcol=x ycol=y """)
    val commandGeoTransform = new geoTransform(simpleQuery, utils)
    println(commandGeoTransform)
    println(simpleQuery)
    val actual = execute(commandGeoTransform)
    println(actual)
    val expected = """[
                     {"x":"60.57","y":"76.36","transformedXY":"(74.61689632880179,0.0063397404998375775)"},
                     {"x":"57.67","y":"38.38","transformedXY":"(74.61701753743816,0.0031864809024247398)"}
                     ]"""
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 3. Command: | geoTransform base=epsg/string sourceCrs=CS63W03 targetCrs=EPSG:4326") {
    initialDf.show()
    val simpleQuery = SimpleQuery(""" sourceBase=string sourceCrs="+proj=tmerc +lat_0=0 +lon_0=72.05 +k=1 +x_0=3500000 +y_0=0 +ellps=krass +units=m +no_defs" targetBase=epsg targetCrs=4326 xcol=x ycol=y """)
    val commandGeoTransform = new geoTransform(simpleQuery, utils)
    println(commandGeoTransform)
    println(simpleQuery)
    val actual = execute(commandGeoTransform)
    println(actual)
    val expected = """[
                     {"x":"60.57","y":"76.36","transformedXY":"(74.61689632880179,0.0063397404998375775)"},
                     {"x":"57.67","y":"38.38","transformedXY":"(74.61701753743816,0.0031864809024247398)"}
                     ]"""
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 4. Command: | geoTransform base=epsg/string sourceCrs=Pulkovo-42 targetCrs=EPSG:4326") {
    initialDf.show()
    val simpleQuery = SimpleQuery(""" sourceBase=string sourceCrs="+proj=tmerc +lat_0=0 +lon_0=75 +k=1 +x_0=500000 +y_0=0 +ellps=krass +units=m +no_defs" targetBase=epsg targetCrs=4326 xcol=x ycol=y """)
    val commandGeoTransform = new geoTransform(simpleQuery, utils)
    println(commandGeoTransform)
    println(simpleQuery)
    val actual = execute(commandGeoTransform)
    println(actual)
    val expected = """[
                     {"x":"60.57","y":"76.36","transformedXY":"(74.61689632880179,0.0063397404998375775)"},
                     {"x":"57.67","y":"38.38","transformedXY":"(74.61701753743816,0.0031864809024247398)"}
                     ]"""
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 5. Command: | geoTransform base=epsg/string sourceCrs=CS63W03 targetCrs=EPSG:4326") {
    initialDf.show()
    val simpleQuery = SimpleQuery(""" sourceBase=string sourceCrs=cs63w03 targetBase=epsg targetCrs=wgs84 xcol=x ycol=y """)
    val commandGeoTransform = new geoTransform(simpleQuery, utils)
    println(commandGeoTransform)
    println(simpleQuery)
    val actual = execute(commandGeoTransform)
    println(actual)
    val expected = """[
                     {"x":"60.57","y":"76.36","transformedXY":"(74.61689632880179,0.0063397404998375775)"},
                     {"x":"57.67","y":"38.38","transformedXY":"(74.61701753743816,0.0031864809024247398)"}
                     ]"""
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

}
