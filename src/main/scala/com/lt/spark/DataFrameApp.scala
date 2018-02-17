package com.lt.spark

import org.apache.spark.sql.SparkSession

/**
  * Created by taoshiliu on 2018/2/17.
  */
object DataFrameApp {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("DataFrameApp").master("local[2]").getOrCreate()
    val peopleDF = spark.read.format("json").load("")

    peopleDF.printSchema()
    peopleDF.show()
    peopleDF.select("name").show()
    peopleDF.select(peopleDF.col("name"),(peopleDF.col("age") + 10).as("age2")).show()
    peopleDF.filter(peopleDF.col("age") > 19).show()
    peopleDF.groupBy("age").count().show()
    spark.stop()

  }

}
