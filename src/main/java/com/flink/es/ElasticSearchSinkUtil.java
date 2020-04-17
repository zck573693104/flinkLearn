package com.flink.es;

import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticSearchSinkUtil {

    public static void sink(DataStream<String> input) throws UnknownHostException {

        Map<String, String> config = new HashMap<>();

        config.put("bulk.flush.max.actions", "40");

        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("localhost"), 9300));

        input.addSink(new ElasticsearchSink<>(config, transportAddresses, new ElasticsearchSinkFunction<String>() {
            public IndexRequest createIndexRequest(String element) {


                return Requests.indexRequest()
                        .index("my-indexs")
                        .type("my-types")
                        .source(element);
            }

            @Override
            public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                indexer.add(createIndexRequest(element));
            }
        }));


    }

    public static void update(DataStream<Tuple2<String,Integer>> input) throws UnknownHostException {

        Map<String, String> config = new HashMap<>();

        config.put("bulk.flush.max.actions", "1");

        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("localhost"), 9300));


        input.addSink(new ElasticsearchSink<>(config, transportAddresses, new ElasticsearchSinkFunction<Tuple2<String,Integer>>() {
            public UpdateRequest updateIndexRequest(Tuple2<String,Integer> element ) throws IOException {
                String id=element.f0;
                Integer status=element.f1;
                UpdateRequest updateRequest=new UpdateRequest();
                //设置表的index和type,必须设置id才能update
                updateRequest.index("my-indexs").type("my-types").id(id)
                        .doc(XContentFactory.jsonBuilder().startObject()
                                .field("age",100)
                                .field("name","zck").endObject());
                return updateRequest;

            }

            @SneakyThrows
            @Override
            public void process(Tuple2<String,Integer> element, RuntimeContext ctx, RequestIndexer indexer) {
                indexer.add(updateIndexRequest(element));
            }
        }));

    }
}