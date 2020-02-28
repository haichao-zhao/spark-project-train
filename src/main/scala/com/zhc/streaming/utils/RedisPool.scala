package com.zhc.streaming.utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * Redis 连接池
  */
object RedisPool {
  val poolConfig = new GenericObjectPoolConfig()
  poolConfig.setMaxIdle(10)
  poolConfig.setMaxTotal(1000)

  private lazy val jedisPool = new JedisPool(poolConfig, ParamsConf.redisHost)


  def getJedis() = {
    val jedis = jedisPool.getResource
    jedis.select(ParamsConf.redisDB)
    jedis
  }
}
