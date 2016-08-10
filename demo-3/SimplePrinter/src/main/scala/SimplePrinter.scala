package main

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SimplePrinter { 
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SimplePrinter")
    val sc = new SparkContext(conf)
    sc.makeRDD(1 to 10).foreach(println)
  }
}