package com.flink.demo;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class CoGourpDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env=ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Long, String>> source1=env.fromElements(
                 Tuple2.of(1L,"xiaoming"),
                Tuple2.of(2L,"xiaowang"));

        DataSet<Tuple2<Long, String>> source2=env.fromElements(
                Tuple2.of(2L,"xiaoli"),
                Tuple2.of(1L,"shinelon"),
                Tuple2.of(3L,"hhhhhh"));

        source1.coGroup(source2)
                .where(0).equalTo(0)
                .with(new CoGroupFunction<Tuple2<Long,String>, Tuple2<Long,String>, Object>() {

                    @Override
                    public void coGroup(Iterable<Tuple2<Long, String>> iterable,
                                        Iterable<Tuple2<Long, String>> iterable1, Collector<Object> collector) throws Exception {
                        Map<Long,String> map=new HashMap<Long,String>();
                        for(Tuple2<Long,String> tuple:iterable){
                            String str=map.get(tuple.f0);
                            if(str==null){
                                map.put(tuple.f0,tuple.f1);
                            }else{
                                if(!str.equals(tuple.f1))
                                    map.put(tuple.f0,str+" "+tuple.f1);
                            }
                        }

                        for(Tuple2<Long,String> tuple:iterable1){
                            String str=map.get(tuple.f0);
                            if(str==null){
                                map.put(tuple.f0,tuple.f1);
                            }else{
                                if(!str.equals(tuple.f1))
                                    map.put(tuple.f0,str+" "+tuple.f1);
                            }
                        }
                        collector.collect(map);
                    }
                }).print();

    }
}
