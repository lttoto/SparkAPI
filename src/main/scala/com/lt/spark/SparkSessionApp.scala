package com.lt.spark

import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by taoshiliu on 2018/2/17.
  */
object SparkSessionApp {

  def main(args: Array[String]) {

    val path = args(0)

    val spark = SparkSession.builder().appName("SparkSessionApp").master("local[2]").getOrCreate()
    val people = spark.read.json("")
    people.show()

    spark.stop()
  }

}
