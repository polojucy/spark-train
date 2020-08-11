package org.bigdata.spark.wc

import org.apache.spark.{SparkConf, SparkContext}

object WordCountApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
//      .setMaster("local")
//      .setAppName("SparkWordCountApp")
    val sc = new SparkContext(conf)


    val df = sc.textFile(args(0))

//    df.collect().foreach(println)
    df.flatMap(_.split(","))
        .map({(_,1)})
        .reduceByKey(_ + _)
        .saveAsTextFile(args(1))
//        .collect()
//        .foreach(println)

    sc.stop()
  }

}
