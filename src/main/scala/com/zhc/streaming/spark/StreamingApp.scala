package com.zhc.streaming.spark

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zhc.streaming.utils.{ParamsConf, RedisPool}
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object StreamingApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamingApp")

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
      val data: RDD[JSONObject] = rdd.map(x => JSON.parseObject(x._2))
      data.cache()

      data.map(x => {

        val day: String = x.getString("time").substring(0, 8)
        val flagResult: Int = if (x.getString("flag") == "1") 1 else 0

        (day, flagResult)
      }).reduceByKey(_ + _)
        .foreachPartition(part => {
          val jedis: Jedis = RedisPool.getJedis()
          part.foreach(x => {
            jedis.incrBy("imoocOrderCount-" + x._1, x._2)
          })
        })


      println("+++++++++++++++++++++++++++++++++++++++++++++")

      //统计每天付费成功的总订单金额
      data.map(x => {

        val day: String = x.getString("time").substring(0, 8)
        val fee: Long = if (x.getString("flag") == "1") x.getString("fee").toLong else 0

        (day, fee)
      }).reduceByKey(_ + _)
        .foreachPartition(part => {
          val jedis: Jedis = RedisPool.getJedis()
          part.foreach(x => {
            jedis.incrBy("imoocOrderFee-" + x._1, x._2)
          })
        })

      data.unpersist(true)
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
