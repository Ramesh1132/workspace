package com.pgtest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
//import org.apache.spark.sql.SparkSession
object SampleRollup {
  
  
  
    val conf = new SparkConf().setMaster("local[2]").setAppName("my app")
    val sc = new SparkContext(conf);
 val sparksql = new SQLContext(sc)
 
 
  def main(args : Array[String]){
   // val sa = sc.textFile("C:/dim/Test.csv")
  val DimSurveyItem = sparksql.read
      .format("com.databricks.spark.csv")
      .option("header", "true") 
      .option("mode", "DROPMALFORMED")
      .load("C:/dim/Test.csv");
  
  DimSurveyItem.registerTempTable("Test")
   
   
 val sa =  sparksql.sql("select a,b,c,SUM(top_box) from Test group by a,b,c union select a,b,null,SUM(top_box) from Test group by a,b union select a,null,null,SUM(top_box) from Test group by a").coalesce(10).cache()
  
  //val sa =  sparksql.sql("select a,b,c,SUM(top_box)from Test group by a,b ROLLUP( (a,b),a,b)").coalesce(10).cache()
  /*// val a = 10
// DimSurveyItem.("")*/
   sa.show()
}
}