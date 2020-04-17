package org.fuwushe.es;

import com.alibaba.fastjson.JSONObject;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class AsyncEsDataRequest extends RichAsyncFunction<String, Tuple2<String, String>> {

    private transient TransportClient transportClient;

    private transient volatile Cache<String, Tuple2<String, String>> cityPercent;

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化ElasticSearch-Client
        //缓存设置
        transportClient = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
        cityPercent = CacheBuilder.<Tuple2<String, String>, String>newBuilder().maximumSize(10).expireAfterWrite(5, TimeUnit.MINUTES)
                .removalListener(
                        //生成环境,可以注销,这个是测试观察缓存使用
                        new RemovalListener<Object, Object>() {
                            @Override
                            public void onRemoval(RemovalNotification<Object, Object> notification) {
                                System.out.println(notification.getKey() + " wa remove,cause is:" + notification.getCause());
                            }
                        }
                ).build();
    }

    @Override
    public void close() throws Exception {
        transportClient.close();
    }


    @Override
    public void asyncInvoke(String input, ResultFuture<Tuple2<String, String>> resultFuture) throws Exception {
        //若缓存里存在,直接从缓存里读取key
        Tuple2<String, String> stationPercent = cityPercent.getIfPresent(input);
        if (stationPercent != null) {
            System.out.println("get data from the cache :" + stationPercent);
            resultFuture.complete(Collections.singleton(stationPercent));
        } else {
            search(input, resultFuture);
        }

    }

    //异步去读Es表
    private void search(String input, ResultFuture<Tuple2<String, String>> resultFuture) {
        SearchRequest searchRequest = new SearchRequest("my-indexs");
        QueryBuilder builder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("name", input));
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(builder);
        searchRequest.source(sourceBuilder);
        ActionListener<SearchResponse> listener = new ActionListener<SearchResponse>() {
            //成功
            @Override
            public void onResponse(SearchResponse searchResponse) {
                Tuple2<String, String> stationPercent = null;
                SearchHit[] searchHits = searchResponse.getHits().getHits();
                if (searchHits.length > 0) {
                    JSONObject jsonObject = JSONObject.parseObject(searchHits[0].getSourceAsString());
                    stationPercent = new Tuple2<>(input, jsonObject.getString("age"));
                    cityPercent.put(input, stationPercent);
                }
                System.out.println("get data from the es :" + stationPercent);
                resultFuture.complete(Collections.singleton(stationPercent));
            }

            //失败
            @Override
            public void onFailure(Exception e) {
                resultFuture.complete(Collections.singleton(new Tuple2<String, String>(input, null)));
            }
        };
        transportClient.search(searchRequest, listener);
    }

    @Override
    public void timeout(String input, ResultFuture<Tuple2<String, String>> resultFuture) throws Exception {
        resultFuture.complete(Collections.singleton(new Tuple2<>(input, "失败")));
    }
}

