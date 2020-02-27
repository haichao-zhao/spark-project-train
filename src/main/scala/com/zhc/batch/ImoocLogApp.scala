package com.zhc.batch

import java.util.zip.CRC32

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ImoocLogApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ImoocLogApp")
      .master("local")
      .config("spark.driver.memory", "1g")
      .config("spark.executor.memory", "2g")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val day = "20190130"

    val input = s"hdfs://localhost:8020/access/$day/*"
    //    val input = "file:///Users/zhaohaichao/workspace/javaspace/spark-project-train/data/test-access.log"

    //    access1.log-20190131
    val value: RDD[String] = spark.sparkContext.textFile(input)

    val logDF = ETLApp.log_ETL(value, spark)

    val hbaseinfoRDD = logDF.rdd.map(x => {
      val ip = x.getAs[String]("ip")
      val country = x.getAs[String]("country")
      val province = x.getAs[String]("province")
      val city = x.getAs[String]("city")
      val formattime = x.getAs[String]("formattime")
      val method = x.getAs[String]("method")
      val url = x.getAs[String]("url")
      val protocal = x.getAs[String]("protocal")
      val status = x.getAs[String]("status")
      val bytessent = x.getAs[String]("bytessent")
      val referer = x.getAs[String]("referer")
      val browsername = x.getAs[String]("browsername")
      val browserversion = x.getAs[String]("browserversion")
      val osname = x.getAs[String]("osname")
      val osversion = x.getAs[String]("osversion")
      val ua = x.getAs[String]("ua")

      val columns = scala.collection.mutable.HashMap[String, String]()
      columns.put("ip", ip)
      columns.put("country", country)
      columns.put("province", province)
      columns.put("city", city)
      columns.put("formattime", formattime)
      columns.put("method", method)
      columns.put("url", url)
      columns.put("protocal", protocal)
      columns.put("status", status)
      columns.put("bytessent", bytessent)
      columns.put("referer", referer)
      columns.put("browsername", browsername)
      columns.put("browserversion", browserversion)
      columns.put("osname", osname)
      columns.put("osversion", osversion)

      // HBase API  Put

      val rowkey = getRowKey(day, referer + url + ip + ua) // HBase的rowkey
      val put = new Put(Bytes.toBytes(rowkey)) // 要保存到HBase的Put对象

      // 每一个rowkey对应的cf中的所有column字段
      for ((k, v) <- columns) {
        if (null == v) {
          put.addColumn(Bytes.toBytes("o"), Bytes.toBytes(k.toString), Bytes.toBytes("unknown"));
        } else {
          put.addColumn(Bytes.toBytes("o"), Bytes.toBytes(k.toString), Bytes.toBytes(v.toString));
        }
      }

      (new ImmutableBytesWritable(rowkey.getBytes), put)
    })

    val conf = new Configuration()
    conf.set("hbase.rootdir", "hdfs://localhost:8020/hbase")
    conf.set("hbase.zookeeper.quorum", "localhost:2181")

    val tableName: String = createTable(day, conf)

    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    hbaseinfoRDD.saveAsNewAPIHadoopFile(
      "hdfs://localhost:8020/etl/access/hbase",
      classOf[ImmutableBytesWritable],
      classOf[Put],
      classOf[TableOutputFormat[ImmutableBytesWritable]],
      conf
    )

    spark.stop()
  }


  def getRowKey(time: String, info: String) = {

    /**
      * 由于rowkey是采用time_crc32(info)进行拼接
      * 只要是字符串拼接，尽量不要使用+  TODO... 是一个非常经典的面试题(Java/Bigdata)
      *
      * StringBuffer vs StringBuilder
      */

    val builder = new StringBuilder(time)
    builder.append("_")

    val crc32 = new CRC32()
    crc32.reset()
    if (StringUtils.isNotEmpty(info)) {
      crc32.update(info.getBytes())
    }
    builder.append(crc32.getValue)

    builder.toString()
  }


  def createTable(day: String, conf: Configuration) = {

    val tableName = "access_" + day

    var conn: Connection = null
    var admin: Admin = null
    try {

      conn = ConnectionFactory.createConnection(conf)
      admin = conn.getAdmin

      val table: TableName = TableName.valueOf(tableName)

      if (admin.tableExists(table)) {
        admin.disableTable(table)
        admin.deleteTable(table)
      }

      val tableDesc = new HTableDescriptor(table)
      val columnDesc = new HColumnDescriptor("o")

      tableDesc.addFamily(columnDesc)
      admin.createTable(tableDesc)

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (null != admin) {
        admin.close()
      }
      if (null != conn) {
        conn.close()
      }
    }

    tableName

  }

}
