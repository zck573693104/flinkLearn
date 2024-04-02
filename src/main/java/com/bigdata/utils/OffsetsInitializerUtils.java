package com.bigdata.utils;

import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class OffsetsInitializerUtils {

    public static OffsetsInitializer getOffsetsInitializer(String model){
        OffsetsInitializer offsetsInitializer = OffsetsInitializer.committedOffsets();
        switch (model) {
            case  "earliest":
                offsetsInitializer = OffsetsInitializer.earliest();
                break;
            case  "latest":
                offsetsInitializer = OffsetsInitializer.latest();
                break;
        }
        return offsetsInitializer;
    }
}
