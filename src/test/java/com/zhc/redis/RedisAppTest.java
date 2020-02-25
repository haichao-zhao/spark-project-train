package com.zhc.redis;

import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;

public class RedisAppTest {

    @Test
    public void test() {

        Jedis jedis = RedisUtils.getJedis();
        Assert.assertEquals("zhc", jedis.get("name"));

    }

}
