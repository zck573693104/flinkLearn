package org.fuwushe.demo;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Event> dataStream = env
                .fromElements(
                        new Event(1, "math", 1),
                        new Event(1, "english", 1),
                        new Event(2, "math", 2),
                        new Event(2, "english", 3),
                        new Event(3, "math", 4),
                        new Event(3, "english", 4));
        dataStream.keyBy("id").sum("score").print();

        env.execute("key test");
    }


    public static class Event {

        public int id;

        public String name;

        public int score;

        public Event() {

        }

        public Event(int id, String name, int score) {

            this.id = id;
            this.name = name;
            this.score = score;
        }


        @Override
        public String toString() {

            return "Event{" + "id=" + id + ", name='" + name + '\'' + ", score=" + score + '}';
        }
    }
}
