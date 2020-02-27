package com.zhc.batch


import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object Demo {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("Demo")
      .master("local[5]")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "8g")
      .getOrCreate()


//    System.setProperty("icode", "33B65DD577711830")

//    val logDF: DataFrame = spark.read.format("com.imooc.bigdata.spark.pk")
//      .option("path", "/Users/zhaohaichao/workspace/javaspace/spark-project-train/data/test-access.log")
//      .load()




    val value: RDD[String] = spark.sparkContext.textFile("file:///Users/zhaohaichao/workspace/javaspace/spark-project-train/data/test-access.log")

    val logDF = ETLApp.log_ETL(value,spark)

    logDF.show(false)

    spark.stop()

  }

}
