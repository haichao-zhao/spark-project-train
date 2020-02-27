package com.zhc.batch

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object TestAPP {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("TestAPP").master("local[2]").getOrCreate()

    val rdd: RDD[Int] = spark.sparkContext.parallelize(List(1,2,3,4))

    rdd.foreach(println)

    spark.stop()

  }

}
