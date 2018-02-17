package com.lt.spark

import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by taoshiliu on 2018/2/17.
  */
object DataFrameRDDApp {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()

    //inferReflection(spark)
    program(spark)
    spark.stop()
  }

  def program(spark: SparkSession): Unit = {
    val rdd = spark.sparkContext.textFile("")
    val infoRDD = rdd.map(_.split(",")).map(line => Row(line(0).toInt, line(1), line(2).toInt))
    val structType = StructType(Array(StructField("id",IntegerType,true),StructField("name",StringType,true),StructField("age",IntegerType,true)))
    val infoDF = spark.createDataFrame(infoRDD,structType)
    infoDF.printSchema()
    infoDF.show()
    infoDF.where(infoDF.col("age") > 30).show()
    infoDF.createOrReplaceTempView("infos")
    spark.sql("select * from infos where age > 30").show()
  }

  def inferReflection(spark: SparkSession): Unit = {
    val rdd = spark.sparkContext.textFile("")

    import spark.implicits._
    val infoDF = rdd.map(_.split(",")).map(line => Info(line(0).toInt, line(1), line(2).toInt)).toDF()
    infoDF.show()
    infoDF.where(infoDF.col("age") > 30).show()
    infoDF.createOrReplaceTempView("infos")
    spark.sql("select * from infos where age > 30").show()
  }

  case class Info(id:Int, name:String, age:Int)

}
