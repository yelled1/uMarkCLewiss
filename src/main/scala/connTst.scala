package sparksql
/*
import $ivy.`org.apache.spark:spark-core_2.11:2.2.0`
import $ivy.`org.apache.spark:spark-sql_2.11:2.2.0`
import $ivy.`org.scalafx:scalafx_2.11:8.0.102-R11`
import $ivy.`org.apache.spark:spark-mllib_2.11:2.2.0`
import $ivy.`org.apache.spark:spark-graphx_2.11:2.2.0`

val jars = ls! wd/'lib |? (_.ext == "jar")
jars.map(interp.load.cp(_))
*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType, StringType, DateType, DoubleType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.avg
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.FXRenderer

object NOAAData extends App {
  val spark = SparkSession.builder().master("local[2]").appName("NOAA Data").getOrCreate()
  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")
  val dDir= "/media/BK5/DFS/Prj/Data/"

  val tschema = StructType(Array(
    StructField("sid", StringType),
    StructField("date", DateType),
    StructField("mtype", StringType),
    StructField("value", DoubleType)
    ))
  val data2017 = spark.read.schema(tschema).option("dateFormat", "yyyyMMdd").csv(dDir+"2017.csv").limit(100000)
  data2017.schema.printTreeString()

  val tmax2017 = data2017.filter($"mtype" === "TMAX").limit(10000).drop("mtype").withColumnRenamed("value", "tmax")
  val tmin2017 = data2017.filter('mtype === "TMIN").limit(10000).drop("mtype").withColumnRenamed("value", "tmin")
  data2017.unpersist 
  //val xx = data2017.filter(x => x.getString(3) == "TMIN").limit(1000); xx.show
  //val combTemp2017 = tmax2017.join(tmin2017, tmax2017("sid") === tmin2017("sid") && tmax2017("date") === tmin2017("date"))
  val combTemp2017 = tmax2017.join(tmin2017, Seq("sid", "date"))
  tmax2017.unpersist; tmin2017.unpersist
  val dailyTemp2017  = combTemp2017.select('Sid, 'date, ('tmax + 'tmin)/20).withColumnRenamed("((tmax + tmin) / 20)", "avgTemp")

  //dailyTemp2017.show
  //dailyTemp2017.describe().show 

  val sschema = StructType(Array(
    StructField("sid", StringType),
    StructField("lati", DoubleType),
    StructField("long", DoubleType),
    StructField("name", StringType)
  ))

  val stationRDD = spark.sparkContext.textFile(dDir+"ghcnd-stations.txt").map {line => 
    val id = line.substring(0, 11)
    val lati = line.substring(12, 20).toDouble
    val long = line.substring(21, 30).toDouble
    val name = line.substring(41, 71)
    Row(id, lati, long, name)
  }

  val stations = spark.createDataFrame(stationRDD, sschema).cache()
  //stations.show
  val stationsTemp2017 = dailyTemp2017.groupBy('sid).agg(avg('avgTemp))
  //stationsTemp2017.show 
  val joinData2017 = stationsTemp2017.join(stations, "sid")
  //joinData2017.show
  joinData2017.unpersist()
  val localData = joinData2017.collect()

  val temps = localData.map(_.getDouble(1))
  val latis = localData.map(_.getDouble(2))
  val longs = localData.map(_.getDouble(3))

  val cg = ColorGradient(0.0 -> BlueARGB, 10.0 -> GreenARGB, 30.0 -> RedARGB)
  val plot = Plot.scatterPlot(longs, latis, "Global Temps", "Longitude", "Latitude", 3, temps.map(cg))
  FXRenderer(plot, 800, 600)
  

  spark.stop()
}
