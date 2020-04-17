package com.flink.sink.redis;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class RankingRedisMapper implements RedisMapper<Tuple2<String, Long>> {


    private String ZSET_NAME_PREFIX ;

    public RankingRedisMapper(String keyName) {
        this.ZSET_NAME_PREFIX = keyName;
    }

    @Override
    public RedisCommandDescription getCommandDescription() {

        return new RedisCommandDescription(RedisCommand.ZADD, ZSET_NAME_PREFIX);
    }

    @Override
    public String getKeyFromData(Tuple2<String, Long> data) {

        return JSONObject.toJSONString(data.f0);
    }

    @Override
    public String getValueFromData(Tuple2<String, Long> data) {

        return JSONObject.toJSONString(data.f1);
    }

}
