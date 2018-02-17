package com.lt.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by taoshiliu on 2018/2/17.
  */
object HiveContextApp {

  def main(args:Array[String]) {

    val path = args(0)

    val sparkConf = new SparkConf()
    //sparkConf.setAppName("HiveContextApp").setMaster("local[2]")


    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    hiveContext.table("emp").show()

    sc.stop()
  }

}
