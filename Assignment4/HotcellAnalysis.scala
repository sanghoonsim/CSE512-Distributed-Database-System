package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  pickupInfo.createOrReplaceTempView("pickupInfo")  
  val att = spark.sql("select x,y,z,count(*) as c from pickupInfo where x >= "+minX+" and x<= "+maxX+ 
  " and y >= "+minY+" and y <= "+maxY+" and z >= "+minZ+" and z <= "+maxZ+" group by x, y, z")
  att.createOrReplaceTempView("att")    
  val point1 = spark.sql("select sum(c) as sum_P from att")
  val sum_P = point1.first().getLong(0)
  val point2 = spark.sql("select sum(power(c,2)) as sum_Sq from att")
  val sum_Sq = point2.first().getDouble(0)
  val mean = sum_P / numCells.toDouble
  val std = Math.sqrt((sum_Sq / numCells) - Math.pow(mean,2))
  val adjacency = spark.sql("select u.x as x, u.y as y, u.z as z, count(*) as adj_cnt, sum(v.c) as c_Sum" + 
                             " from att as u, att as v" + 
                             " where ((u.x=v.x-1 or u.x=v.x or u.x=v.x+1) and (u.y=v.y-1 or u.y=v.y or u.y=v.y+1) and" +
                             " (u.z=v.z-1 or u.z=v.z or u.z=v.z+1)) group by u.x, u.y, u.z")
  adjacency.createOrReplaceTempView("adjacency")
  spark.udf.register("calZscore",(mean: Double, stdd:Double, c_Sum: Int, numCells:Int, adj_cnt: Int)
  =>((HotcellUtils.calZscore(mean, stdd, c_Sum, numCells, adj_cnt))))  
  val zs =  spark.sql("select x,y,z,calZscore("+mean+", "+std+", c_Sum, "+numCells+", adj_cnt) as fz from adjacency order by fz desc")
  zs.createOrReplaceTempView("zs")
  val res = spark.sql("select x,y,z from zs")
  return res
}
}
