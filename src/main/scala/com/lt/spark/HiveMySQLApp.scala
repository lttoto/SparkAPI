package com.lt.spark

import org.apache.spark.sql.SparkSession

/**
  * Created by taoshiliu on 2018/2/17.
  */
object HiveMySQLApp {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("ParquetApp").master("local[2]").getOrCreate()

    val hiveDF = spark.table("emp");
    val mySQLDF = spark.read.format("jdbc").option("url","jdbc:mysql://xxx:3306").option("dbtable","spark.DEPT").option("user","root").option("password","root").option("driver","xxx").load()

    val resultDF = hiveDF.join(mySQLDF,hiveDF.col("deptno") === mySQLDF.col("DEPTNO"))

    resultDF.show()
    resultDF.select(hiveDF.col("empno"),hiveDF.col("ename"),mySQLDF.col("deptno")).show()

    spark.stop()
  }

}
