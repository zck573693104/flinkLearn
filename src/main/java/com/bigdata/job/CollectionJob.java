package com.bigdata.job;

import groovy.lang.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Iterator;
import java.util.List;

public class CollectionJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

// Create a DataStream from a list of elements
        DataStream<Integer> myInts = env.fromElements(1, 2, 3, 4, 5);

        myInts.print();

// Create a DataStream from an Iterator
        Iterator<Long> longIt = List.of(1L).iterator();
        DataStream<Long> myLongs = env.fromCollection(longIt, Long.class);
        env.execute();
    }
}
