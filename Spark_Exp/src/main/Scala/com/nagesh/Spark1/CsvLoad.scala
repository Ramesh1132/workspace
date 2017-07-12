package com.nagesh.Spark1
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
object CsvLoad {
  val warehouseLocation = "spark-warehouse"
val spark = SparkSession.builder().appName("Spark Hive Example").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()

import spark.implicits._
import spark.sql

 def main(args : Array[String]){
   // val sa = sc.textFile("C:/dim/Test.csv")
  val DimSurveyItem = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true") 
      .option("mode", "DROPMALFORMED")
      .load("C:/dim/Test.csv");
  
  DimSurveyItem.createOrReplaceTempView("Test")
   
   
 val sa =  spark.sql("select a,b,c,SUM(top_box) from Test group by a,b,c union select a,b,null,SUM(top_box) from Test group by a,b union select a,null,null,SUM(top_box) from Test group by a").coalesce(10).cache()
  
  //val sa =  sparksql.sql("select a,b,c,SUM(top_box)from Test group by a,b ROLLUP( (a,b),a,b)").coalesce(10).cache()
  /*// val a = 10
// DimSurveyItem.("")*/
   sa.show()
}
}