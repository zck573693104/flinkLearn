package com.kafka;

import org.apache.flink.streaming.connectors.kafka.KafkaContextAware;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class MyKafkaSerializationSchema<T> implements KafkaSerializationSchema<T>, KafkaContextAware<T> {
    private static String encoding = "UTF8";

    @Override
    public String getTargetTopic(T element) {
        return "test";
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(T element, @Nullable Long timestamp) {
        return new ProducerRecord(getTargetTopic(element),element);
    }

//    @Override
//    public String getTargetTopic(ProducerRecord<String, String> record) {
//        return record.topic();
//    }
//
//    @Override
//    public ProducerRecord<byte[], byte[]> serialize(ProducerRecord<String, String> record, @Nullable Long timestamp) {
//        String key = StringUtils.isNotBlank(record.key()) ? record.key() : "default_key";
//        return new ProducerRecord(record.topic(),
//                record.partition(),
//                key,
//                record.value());
//    }
}
