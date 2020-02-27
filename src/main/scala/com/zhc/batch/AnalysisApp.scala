package com.zhc.batch

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime


/**
  *
  * 使用Spark对HBase中的数据做统计分析操作
  *
  * 1） 统计每个国家每个省份的访问量
  * 2） 统计不同浏览器的访问量
  */
object AnalysisApp extends Logging {

  def main(args: Array[String]): Unit = {

    val starttime: DateTime = DateTime.now()

    val spark = SparkSession.builder()
      .appName("AnalysisApp")
      .master("local[2]")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "4g")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    //获取要进行分析的日期
    val day = "20190130"
    val tableName = "access_" + day

    //连接HBase
    val conf = new Configuration()
    conf.set("hbase.rootdir", "hdfs://localhost:8020/hbase")
    conf.set("hbase.zookeeper.quorum", "localhost:2181")

    //设置要从哪个表中去读数据
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    val scan = new Scan()

    //设置要查询的cf
    scan.addFamily("o".getBytes)

    //设置要查询的列
    scan.addColumn("o".getBytes, "country".getBytes)
    scan.addColumn("o".getBytes, "province".getBytes)

    scan.addColumn("o".getBytes, "browsername".getBytes)

    // 设置Scan
    conf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))

    // 通过Spark的newAPIHadoopRDD读取数据
    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = spark.sparkContext.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    hbaseRDD.cache()

    //TODO...  统计每个国家每个省份的访问量  ==> TOP10
    hbaseRDD.map(x => {
      val country: String = Bytes.toString(x._2.getValue("o".getBytes, "country".getBytes))
      val province: String = Bytes.toString(x._2.getValue("o".getBytes, "province".getBytes))
      ((country, province), 1)
    }).reduceByKey(_ + _)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1))
      .take(10)
      .foreach(println)

    logError("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    //TODO...  统计不同浏览器的访问量 RDD 方式
    hbaseRDD.map(x => {
      val browsername: String = Bytes.toString(x._2.getValue("o".getBytes, "browsername".getBytes))

      (browsername, 1)
    }).reduceByKey(_ + _)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1))
      .foreach(println)

    logError("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    //TODO...  统计不同浏览器的访问量 DataFream 方式
    hbaseRDD.map(x => {
      val browsername: String = Bytes.toString(x._2.getValue("o".getBytes, "browsername".getBytes))

      Browser(browsername)
    }).toDF()
      .groupBy("browsername")
      .count()
      .sort(desc("count"))
      .show(false)

    logError("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    //TODO...  统计不同浏览器的访问量 Spark SQL 方式
    hbaseRDD.map(x => {
      val browsername: String = Bytes.toString(x._2.getValue("o".getBytes, "browsername".getBytes))

      Browser(browsername)
    }).toDF().createOrReplaceTempView("tmp")

    spark.sql("select browsername,count(1) cnt from tmp group by browsername order by cnt desc")
      .show(false)

    logError("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")


    hbaseRDD.unpersist(true)

    spark.stop()

    val endtime: DateTime = DateTime.now()

    val a = endtime.toDate.getTime - starttime.toDate.getTime

    println(a)

  }

  case class Browser(browsername: String)

}
