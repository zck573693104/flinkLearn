package com.flink.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;


public class SqlRetractStream {

    public static void main(String []args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        DataStream<Tuple2<Long,Long>> ds = env.fromElements(Tuple2.of(1L, 100L), Tuple2.of(2L, 100L), Tuple2.of(3L, 3L), Tuple2.of(4L, 4L), Tuple2.of(5L, 5L));
        Table table = tableEnv.fromDataStream(ds, "num, score");
        Table result = tableEnv.sqlQuery(
                "SELECT SUM(num) as sumNum FROM " + table + " WHERE num =1 ");
        DataStream<Tuple2<Boolean, Long>> dataStreamResult = tableEnv.toRetractStream(result,Long.class);
        dataStreamResult.print();

        env.execute("sql test");


    }

    private static class RandomFibonacciSource implements SourceFunction<PlayerData> {
        private static final long serialVersionUID = 1L;


        private volatile boolean isRunning = true;


        @Override
        public void run(SourceContext<PlayerData> sourceContext) throws Exception {
            while (isRunning){
                PlayerData playerData = new PlayerData("16-17","拉塞尔-威斯布鲁克",10);
                sourceContext.collect(playerData);
            }

        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static class Result {
        public String player;
        public Long num;

        public Result() {
            super();
        }
        public Result(String player, Long num) {
            this.player = player;
            this.num = num;
        }
        @Override
        public String toString() {
            return player + ":" + num;
        }
    }



    public static class PlayerData {
        /**
         * 赛季，球员，出场，首发，时间，助攻，抢断，盖帽，得分
         */
        public String season;
        public String player;
        public int play_num;

        public PlayerData() {
            super();
        }

        public PlayerData(String season,
                String player,
                int play_num
        ) {
            this.season = season;
            this.player = player;
            this.play_num = play_num;

        }
    }
}
