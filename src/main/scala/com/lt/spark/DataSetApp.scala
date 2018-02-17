package com.lt.spark

import org.apache.spark.sql.SparkSession

/**
  * Created by taoshiliu on 2018/2/17.
  */
object DataSetApp {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("DataSetApp").master("local[2]").getOrCreate()

    import spark.implicits._
    val df = spark.read.option("header","true").option("inferSchema","true").csv("")
    df.show()

    val ds = df.as[Sales]
    ds.map(line => line.itemId).show()

    spark.stop()
  }

  case class Sales(transactionId:Int,customerId:Int,itemId:Int,amountPaid:Double)

}
