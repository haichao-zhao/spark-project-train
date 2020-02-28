package com.zhc.streaming.utils

import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.StringDeserializer

/**
  *
  * 项目参数配置读取类
  *
  * 配置统一管理
  */
object ParamsConf {

  private lazy val config = ConfigFactory.load()

  val topic = config.getString("kafka.topic").split(",")
  val groupId = config.getString("kafka.group.id")
  val brokers = config.getString("kafka.broker.list")

  val redisHost = config.getString("redis.host")
  val redisDB = config.getInt("redis.db")

  val kafkaParams = Map[String, String](
    "metadata.broker.list" -> brokers,
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "group.id" -> groupId,
    "auto.offset.reset" -> "largest",
    "enable.auto.commit" -> "false"
  )


  def main(args: Array[String]): Unit = {
    println(ParamsConf.topic)
    println(ParamsConf.groupId)
    println(ParamsConf.brokers)

    println(ParamsConf.redisHost)
    println(ParamsConf.redisDB)
  }


}
