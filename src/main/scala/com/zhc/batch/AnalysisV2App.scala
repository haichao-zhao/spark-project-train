package com.zhc.batch

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

import scala.util.{Failure, Success, Try}


/**
  *
  * 使用Spark对HBase中的数据做统计分析操作
  *
  * 1） 统计每个国家每个省份的访问量
  * 2） 统计不同浏览器的访问量
  *
  * 将浏览器的统计结果写入到MySQL中
  */
object AnalysisV2App extends Logging {

  def main(args: Array[String]): Unit = {

    val starttime: DateTime = DateTime.now()

    val spark = SparkSession.builder()
      .appName("AnalysisV2App")
      .master("local[2]")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "4g")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

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

    //TODO...  统计不同浏览器的访问量 RDD 方式
    val browserRDD = hbaseRDD.map(x => {
      val browsername: String = Bytes.toString(x._2.getValue("o".getBytes, "browsername".getBytes))

      (browsername, 1)
    }).reduceByKey(_ + _)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1))

    logError("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    hbaseRDD.unpersist(true)

    //将统计结果写入到MySQL中
    browserRDD.coalesce(1).foreachPartition(part => {

      Try {
        Class.forName("com.mysql.jdbc.Driver")
        val url = "jdbc:mysql://localhost:3306/spark?characterEncoding=UTF-8&useSSL=false"
        val user = "root"
        val password = "root"
        val conn: Connection = DriverManager.getConnection(url, user, password)

        val autoCommit: Boolean = conn.getAutoCommit
        conn.setAutoCommit(false)

        val sql = "insert into browser_stat (day,browser,cnt) values (?,?,?)"
        val pstmt: PreparedStatement = conn.prepareStatement(sql)
        pstmt.addBatch(s"delete from browser_stat where day = $day")

        part.foreach(x => {
          pstmt.setString(1, day)
          pstmt.setString(2, x._1)
          pstmt.setInt(3, x._2)

          pstmt.addBatch()
        })

        pstmt.executeBatch()
        conn.commit()

        (conn, autoCommit)
      } match {
        case Success((conn, autoCommit)) => {
          conn.setAutoCommit(autoCommit)
          if (null != conn) {
            conn.close()
          }
        }
        case Failure(exception) => throw exception
      }

    })

    spark.stop()

    val endtime: DateTime = DateTime.now()

    val a = endtime.toDate.getTime - starttime.toDate.getTime

    println(a)

  }

  case class Browser(browsername: String)

}
