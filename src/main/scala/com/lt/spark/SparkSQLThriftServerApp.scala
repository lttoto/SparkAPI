package com.lt.spark

import java.sql.DriverManager

/**
  * Created by taoshiliu on 2018/2/17.
  */
object SparkSQLThriftServerApp {

  def main(args: Array[String]) {
    Class.forName("org.apache.hive.jdbc.HiveDriver")

    val conn = DriverManager.getConnection("jdbc:hive2://hadoop001:10000","hadoop","")
    val pstmt = conn.prepareStatement("select empno,ename,sal from emp")
    val rs = pstmt.executeQuery();
    while(rs.next()) {
      println("empno:" + rs.getInt("empno") +
        " , ename:" + rs.getString("ename") +
        " , sal:" + rs.getString("sal"))
    }

    rs.close()
    pstmt.close()
    conn.close()
  }

}
