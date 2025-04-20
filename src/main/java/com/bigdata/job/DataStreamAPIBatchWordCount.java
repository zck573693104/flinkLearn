package com.bigdata.job;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;

/**
 * DataStream API 批处理
 * (启动jar包时 指定模式)
 */
@RestController
public class DataStreamAPIBatchWordCount {

    @PostConstruct
    public void test() throws Exception {

        // 1. 创建流式的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取文件 (有界流)
        DataStreamSource<String> lineDataStreamSource = env.readTextFile("input/words.txt");

        // 3. 转换计算
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = lineDataStreamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4. 分组操作  wordAndOneTuple.keyBy(0) 根据0索引位置分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKeyedStream = wordAndOneTuple.keyBy(item -> item.f0);

        // 5. 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneKeyedStream.sum(1);

        // 6. 打印
        sum.print();

        // 7. 启动执行 上面步骤只是定义了流的执行流程
        env.execute();

        // 数字表示子任务编号 (默认是cpu的核心数 同一个词会出现在同一个子任务上进行叠加)
//        3> (java,1)
//        9> (test,1)
//        5> (hello,1)
//        3> (java,2)
//        5> (hello,2)
//        9> (test,2)
//        9> (world,1)
//        9> (test,3)


    }
}

