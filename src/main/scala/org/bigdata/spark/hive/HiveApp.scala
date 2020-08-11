package org.bigdata.spark.hive

import org.apache.spark.sql.{SaveMode, SparkSession}

object HiveApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("HiveApp")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.table("test.pokes").limit(10)
    df.write.mode(SaveMode.Append).saveAsTable("test.pokes001")
    spark.stop()
  }

}
