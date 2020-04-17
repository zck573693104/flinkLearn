package org.fuwushe.sink.redis;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class GmvRedisMapper implements RedisMapper<Tuple2<String, String>> {

    private String HASH_NAME_PREFIX = null;

    public GmvRedisMapper(String key){
        this.HASH_NAME_PREFIX = key;
    }


    @Override
    public RedisCommandDescription getCommandDescription() {

        return new RedisCommandDescription(RedisCommand.HSET, HASH_NAME_PREFIX);
    }

    @Override
    public String getKeyFromData(Tuple2<String, String> data) {

        return JSONObject.toJSONString(data.f0);
    }

    @Override
    public String getValueFromData(Tuple2<String, String> data) {
        //上层是对象转json 了
        return data.f1;
    }

}
