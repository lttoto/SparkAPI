package com.lt.spark

import org.apache.spark.sql.SparkSession

/**
  * Created by taoshiliu on 2018/2/17.
  */
object ParquetApp {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("ParquetApp").master("local[2]").getOrCreate()
    spark.sqlContext.setConf("spark.sql.shuffle.partitions","10")
    val userDF = spark.read.format("parquet").load("")
    userDF.printSchema()
    userDF.show()
    userDF.select("name","favorite_color").show()
    userDF.select("name","favorite_color").write.format("json").save("")

    spark.read.format("parquet").option("path","").load()

    //特殊方案（只有Parquet数据可以这么使用）
    spark.read.load("").show()
    spark.stop()
  }

}
