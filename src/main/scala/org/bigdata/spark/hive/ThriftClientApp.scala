package org.bigdata.spark.hive

import java.sql.DriverManager

object ThriftClientApp {

  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val conn = DriverManager.getConnection("jdbc:hive2://hadoop002:10000")
    val statement = conn.prepareStatement("select * from test.pokes")
    val resultSet = statement.executeQuery()


    while (resultSet.next()) {
      println(resultSet.getObject(1) + " : " + resultSet.getObject(2))
    }


  }

}
