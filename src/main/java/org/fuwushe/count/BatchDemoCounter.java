package org.fuwushe.count;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

public class BatchDemoCounter {
    public static void main(String[] args) throws Exception{
        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> data = env.fromElements("a", "b", "c", "d","e","f");
        DataSet<String> result = data.map(new RichMapFunction<String, String>() {
            //1: 创建累加器
           private IntCounter numLines = new IntCounter();
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //2: 注册累加器
                getRuntimeContext().addAccumulator("num-lines",this.numLines);
            }
            //int sum = 0;
            @Override
            public String map(String value) throws Exception {
                // 如果并行度为 1, 使用普通的累加求和即可, 但是设置多个并行度, 则普通的累加求和结果就不准了
                //sum++;
                //System.out.println("sum:"+sum);
                this.numLines.add(1);
                return value;
            }
        }).setParallelism(1);
        //result.print();
        result.writeAsText("/load/data/test");
        JobExecutionResult jobResult = env.execute("counter");
        //3: 获取累加器
        int num = jobResult.getAccumulatorResult("num-lines");
        System.out.println("num:"+num);
    }
}