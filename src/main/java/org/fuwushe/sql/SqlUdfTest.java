package org.fuwushe.sql;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;
import org.fuwushe.qnx.registerfunction.LineToColumn;
import org.fuwushe.sql.udf.FromUnixTimeUDF;
import org.fuwushe.sql.udf.Split;

import java.util.Iterator;

public class SqlUdfTest {
    public static void main(String []args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
        tableEnv.registerFunction("split", new Split("#"));
        tableEnv.registerFunction("from_unixtime", new FromUnixTimeUDF());
        tableEnv.registerFunction("wAvg", new WeightedAvg());
        tableEnv.registerFunction("line_to_column", new LineToColumn());
        DataSet<String> input = env.readTextFile("/load/data/udf.txt");
        DataSet<UdfData> topInput = input.map(new MapFunction<String,UdfData>() {
            @Override
            public UdfData map(String s) throws Exception {
                return JSONObject.parseObject(s,UdfData.class);
            }
        });
        Table udfTable = tableEnv.fromDataSet(topInput);
        tableEnv.registerTable("udf_table", udfTable);

        //ScalarFunction
        Table udfResult = tableEnv.sqlQuery(" select from_unixtime(`time`) as creatTime,itemId FROM udf_table order by  creatTime desc  ");
        tableEnv.toDataSet(udfResult, UdfResult.class).print();
        //TableFunction
        Table udtfResult1 =  tableEnv.sqlQuery("SELECT action, word, length FROM udf_table, LATERAL TABLE(split(unionId)) as T(word, length)");
        Table udtfResult2 =  tableEnv.sqlQuery("SELECT  action, word, length FROM udf_table LEFT JOIN LATERAL TABLE(split(unionId)) as T(word, length) ON TRUE");
        tableEnv.toDataSet(udtfResult1, UdtfResult.class).print();
        tableEnv.toDataSet(udtfResult2, UdtfResult.class).print();
        //AggregateFunction 6 1
        Table udafResult = tableEnv.sqlQuery("SELECT itemId, wAvg(price,wegiht) AS avgPoints FROM udf_table GROUP BY itemId");
        tableEnv.toDataSet(udafResult, UdafResult.class).print();

        Table lint_to_column = tableEnv.sqlQuery("SELECT itemId, line_to_column(rankIndex) AS avgPoints FROM udf_table GROUP BY itemId");
        tableEnv.toDataSet(lint_to_column, Row.class).print();

    }

    /**
     * Accumulator for WeightedAvg.
     */
    public static class WeightedAvgAccum {
        public long sum = 0;
        public int count = 0;
    }
    /**
     * Weighted Average user-defined aggregate function.
     */
    public static class WeightedAvg extends AggregateFunction<Long, WeightedAvgAccum> {
        @Override
        public WeightedAvgAccum createAccumulator() {
            return new WeightedAvgAccum();
        }
        @Override
        public Long getValue(WeightedAvgAccum acc) {
            if (acc.count == 0) {
                return null;
            } else {
                return acc.sum / acc.count;
            }
        }
        public void accumulate(WeightedAvgAccum acc, long iValue, int iWeight) {
            acc.sum += iValue * iWeight;
            acc.count += iWeight;
        }
        public void retract(WeightedAvgAccum acc, long iValue, int iWeight) {
            acc.sum -= iValue * iWeight;
            acc.count -= iWeight;
        }
        public void merge(WeightedAvgAccum acc, Iterable<WeightedAvgAccum> it) {
            Iterator<WeightedAvgAccum> iter = it.iterator();
            while (iter.hasNext()) {
                WeightedAvgAccum a = iter.next();
                acc.count += a.count;
                acc.sum += a.sum;
            }
        }
        public void resetAccumulator(WeightedAvgAccum acc) {
            acc.count = 0;
            acc.sum = 0L;
        }
    }

    public static class UdafResult {

        public UdafResult() {
            super();
        }


        public String itemId;
        public long avgPoints;

        public UdafResult(String itemId, long avgPoints) {

            this.itemId = itemId;
            this.avgPoints = avgPoints;
        }

        @Override
        public String toString() {

            return "UdafResult{" + "itemId='" + itemId + '\'' + ", avgPoints=" + avgPoints + '}';
        }
    }

    public static class UdtfResult {

        public UdtfResult() {
            super();
        }

        public String action;
        public String word;
        public int length;

        public UdtfResult(String action, String word, int length) {

            this.action = action;
            this.word = word;
            this.length = length;
        }

        @Override
        public String toString() {

            return "UdtfResult{" + "action='" + action + '\'' + ", word='" + word + '\'' + ", length=" + length + '}';
        }
    }

    public static class UdfResult {

        public UdfResult() {
            super();
        }

        public String itemId;
        public String creatTime;

        public UdfResult(String itemId, String creatTime) {

            this.itemId = itemId;
            this.creatTime = creatTime;
        }

        @Override
        public String toString() {

            return "Result{" + "itemId='" + itemId + '\'' + ", creatTime='" + creatTime + '\'' + '}';
        }
    }



    public static class UdfData {

        public UdfData(String action, String itemId, String time, String unionId, Integer rankIndex, Integer wegiht,
                long price) {

            this.action = action;
            this.itemId = itemId;
            this.time = time;
            this.unionId = unionId;
            this.rankIndex = rankIndex;
            this.wegiht = wegiht;
            this.price = price;
        }

        public String action;
        public String itemId;
        public String time;
        public String unionId;
        public Integer rankIndex;
        public Integer wegiht;
        public long price;
      

        public UdfData() {
            super();
        }
        
    }
}
