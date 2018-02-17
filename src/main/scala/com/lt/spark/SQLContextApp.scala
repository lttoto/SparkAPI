package com.lt.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by taoshiliu on 2018/2/17.
  */
object SQLContextApp {

  def main(args:Array[String]) {

    val path = args(0)

    val sparkConf = new SparkConf()
    //sparkConf.setAppName("SQLContextApp").setMaster("local[2]")


    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()

    sc.stop()
  }

}
