package com.test.bigdata.test;

import com.test.bigdata.rtmk.utils.RuntimeConfig;
import redis.clients.jedis.Jedis;

public class HistoryConditionValueMoni {


    public static void main(String[] args) {
        // 模拟用户  20  21   22

        Jedis jedis = RuntimeConfig.redis();
        // KEY[规则ID]   => { FIELD[条件ID:用户ID] ,  VALUE[历史值] }
        jedis.hset("4","dynCondition1:20","1");
        jedis.hset("4","dynCondition1:21","2");
        jedis.hset("4","dynCondition1:22","5");
        jedis.hset("4","dynCondition1:23","3");


        jedis.close();

    }
}
