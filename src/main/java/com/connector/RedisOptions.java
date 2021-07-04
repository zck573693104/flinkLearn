package com.connector;
 
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
 
/** 
 * Option utils for redis table source sink. 
 */ 
public class RedisOptions {
 
    private RedisOptions() {}


    public static final ConfigOption<String> HOST =
            ConfigOptions.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Redis table host.");
 
    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(6379)
                    .withDescription(
                            "The Redis table port.");

    public static final ConfigOption<String> COMMAND =
            ConfigOptions.key("command")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Redis get value command.");
 
    public static final ConfigOption<Integer> EXPIRE =
            ConfigOptions.key("expire")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The Redis table expire time.");
}