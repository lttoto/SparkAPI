package com.lt.spark

import com.lt.spark.DataFrameRDDApp.Info
import org.apache.spark.sql.SparkSession

/**
  * Created by taoshiliu on 2018/2/17.
  */
object DataFrameCase {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("DataFrameCase").master("local[2]").getOrCreate()

    val rdd = spark.sparkContext.textFile("")

    import spark.implicits._
    val studentDF = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2),line(3))).toDF()
    studentDF.show(30,false)
    studentDF.take(10).foreach(println)
    studentDF.first()
    studentDF.head(3)
    studentDF.select("name","email").show(30,false)
    studentDF.filter("name=''").show()
    studentDF.filter("name='' OR name='NULL'").show()
    studentDF.filter("SUBSTR(name,0,1)='M'").show()
    studentDF.sort(studentDF.col("name")).show()
    studentDF.sort(studentDF.col("name").desc).show()
    studentDF.sort(studentDF.col("name").desc,studentDF.col("id")).show()
    studentDF.select(studentDF.col("name").as("student_name")).show()

    val studentDF2 = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2),line(3))).toDF()
    studentDF.join(studentDF2,studentDF.col("id") === studentDF2.col("id")).show()

    spark.stop()
  }

  case class Student(id:Int,name:String,phone:String,email:String)

}
