package com.nagesh.Spark1

import org.apache.spark.SparkConf

import org.apache.spark.SparkContext

import org.apache.spark.SparkContext

object Sample1 {
  def main(args : Array[String]){
    val conf = new SparkConf().setMaster("local").setAppName("Sample");
    val sc = new SparkContext(conf);
    
    val a= sc.parallelize(1 to 8,1);
    val b = a.map(x => (x + x));
    b.collect().foreach(println);
  }
}