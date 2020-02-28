package com.zhc.streaming.spark

import java.lang

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zhc.streaming.utils.{ParamsConf, RedisPool}
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object StreamingV2App {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamingV2App")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val value: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,
      ParamsConf.kafkaParams,
      ParamsConf.topic.toSet
    )

    //    value.map(x=>x._2).count().print()

    /**
      * 统计每天付费成功的总订单数
      * 统计每天付费成功的总订单金额
      */
    value.foreachRDD(rdd => {
      val data = rdd.map(x => JSON.parseObject(x._2))
        .map(x => {

          val time: String = x.getString("time")
          val flag: String = x.getString("flag")
          val fee: lang.Long = x.getLong("fee")

          val success: (Long, Long) = if (flag == "1") (1, fee) else (0, 0)

          val day = time.substring(0, 8)
          val hour = time.substring(8, 10)
          val min = time.substring(10, 12)

          (day, hour, min, List(1, success._1, success._2))
        })

      //天
      data.map(x => (x._1, x._4))
        .reduceByKey((a, b) => {
          a.zip(b).map(x => {
            println(x._1)
            println(x._2)
            x._1 + x._2
          })
        }).foreachPartition(part => {
        val jedis: Jedis = RedisPool.getJedis()
        part.foreach(x => {
          jedis.hincrBy("imooc-" + x._1, "total", x._2(0))
          jedis.hincrBy("imooc-" + x._1, "success", x._2(1))
          jedis.hincrBy("imooc-" + x._1, "fee", x._2(2))
        })
      })


      //小时
      data.map(x => ((x._1, x._2), x._4))
        .reduceByKey((a, b) => {
          a.zip(b).map(x => {
            println(x)
            x._1 + x._2
          })
        }).foreachPartition(part => {
        val jedis: Jedis = RedisPool.getJedis()
        part.foreach(x => {
          jedis.hincrBy("imooc-" + x._1._1 + x._1._2, "total", x._2(0))
          jedis.hincrBy("imooc-" + x._1._1 + x._1._2, "success", x._2(1))
          jedis.hincrBy("imooc-" + x._1._1 + x._1._2, "fee", x._2(2))
        })
      })

    })

    ssc.start()
    ssc.awaitTermination()

  }

}
