package com.flink.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class KafkaProperties {

    public static Properties getProducerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "master:9092,worker-2:9092");
        /**
         * Producer希望leader返回接受消息后的确认信息. 可选值 all, -1, 0 1. 默认值为1.
         * 1.> acks=0 不需要等待leader尽心确认. 此时retries设置无效. 响应里来自服务端的offset总是-1.
         *     Producer只管发不管发送成功与否。延迟低，容易丢失数据。
         * 2.> acks=1 表示leader写入成功（但是并没有刷新到磁盘）后即向Producer响应。延迟中等，但是一旦
         *     leader副本挂了，就会丢失数据。
         * 3.> acks=all 等待数据完成副本的复制, 等同于-1. 假如需要保证消息不丢失, 需要使用该设置. 同时
         *     需要设置unclean.leader.election.enable为true, 保证当ISR列表为空时, 选择其他存活的副本作为新的leader.
         */
        props.put("acks", "all");
        /**
         * 设置大于零的值时，Producer会发送失败后会进行重试。
         */
        props.put("retries", 0);
        /**
         * Producer批量发送同一个partition消息以减少请求的数量从而提升客户端和服务端的性能，默认大小是16348 byte(16k).
         * 发送到broker的请求可以包含多个batch, 每个batch的数据属于同一个partition，太小的batch会降低吞吐.太大会浪费内存.
         */
        props.put("batch.size", 16384);
        /**
         * batch.size和liner.ms配合使用，前者限制大小后者限制时间。前者条件满足的时候，同一partition的消息会立即发送,
         * 此时linger.ms的设置无效，假如要发送的消息比较少, 则会等待指定的时间以获取更多的消息，此时linger.ms生效
         * 默认设置为0ms(没有延迟).
         */
        props.put("linger.ms", 1);
        /**
         * Producer可以使用的最大内存来缓存等待发送到server端的消息.默认值33554432 byte(32m)
         */
        props.put("buffer.memory", 33554432);
        props.put("compression.type", "snappy");
        props.put("max.request.size", 10485760);
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.kerberos.service.name", "kafka");
        props.setProperty("java.security.auth.login.config","/load/data/jaas.conf");
        props.setProperty("java.security.krb5.conf","/load/data/krb5.conf");
        return props;
    }

    public static Properties getConsumerProps(String proOrTest) {
        if (proOrTest.equals("prod") || proOrTest.equals("test") ){
            Properties props = new Properties();
            props.put("bootstrap.servers", "master:9092,worker-2:9092");
            props.put("security.protocol", "SASL_PLAINTEXT");
            props.put("sasl.kerberos.service.name", "kafka");
            props.setProperty("java.security.auth.login.config","/load/data/jaas.conf");
            props.setProperty("java.security.krb5.conf","/load/data/krb5.conf");
            //        //动态感知kafka主题分区的增加 单位毫秒
            props.setProperty("flink.partition-discovery.interval-millis", "5000");
            props.put("enable.auto.commit",true);
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
            return props;
        }
        else {
           return getLocal();
        }


    }
    public static Properties getLocal() {
        Properties props = new Properties();
        props.put("enable.auto.commit",true);
        props.put("bootstrap.servers", "localhost:9092");
        props.setProperty("flink.partition-discovery.interval-millis", "5000");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        return props;
    }
}
