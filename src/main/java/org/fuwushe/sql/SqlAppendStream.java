package org.fuwushe.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Random;

public class SqlAppendStream {

    public static void main(String []args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        DataStream<PlayerData> ds = env.addSource(new RandomFibonacciSource());
        Table table = tableEnv.fromDataStream(ds, "play_num, player");
        Table result = tableEnv.sqlQuery(
                "SELECT SUM(play_num) as play_num FROM " + table + " WHERE player ='拉塞尔-威斯布鲁克' ");
        DataStream<Tuple2<Boolean, Integer>> dataStreamResult = tableEnv.toRetractStream(result,Integer.class);
        dataStreamResult.print();

        env.execute("sql test");


    }

    private static class RandomFibonacciSource implements SourceFunction<PlayerData> {
        private static final long serialVersionUID = 1L;


        private volatile boolean isRunning = true;


        @Override
        public void run(SourceContext<PlayerData> sourceContext) throws Exception {
            while (isRunning){
                Random random = new Random();
                PlayerData playerData = new PlayerData("16-17","拉塞尔-威斯布鲁克",random.nextInt(10));
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
