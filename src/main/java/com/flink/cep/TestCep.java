package com.flink.cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TestCep {

    public static void main(String []args) throws Exception {
        test();
    }
    public static void test() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<LoginEvent> loginEventStream = env.fromCollection(Arrays.asList(
                new LoginEvent("1","192.168.0.1","fail"),
                new LoginEvent("1","192.168.0.2","fail"),
              //  new LoginEvent("1","192.168.0.3","fail"),
                new LoginEvent("2","192.168.10,10","success"),
                new LoginEvent("2","192.168.10,11","fail"),
                new LoginEvent("2","192.168.10,12","fail")

        ));

        Pattern<LoginEvent, LoginEvent> loginEventPattern = Pattern.<LoginEvent>begin("begin")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) throws Exception {

                        return loginEvent.getType().equals("fail");
                    }
                }).next("next").where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) throws Exception {

                        return loginEvent.getType().equals("fail");
                    }
                }).within(Time.seconds(1));
        PatternStream<LoginEvent> patternStream = CEP
                .pattern(loginEventStream.keyBy(LoginEvent::getUserId), loginEventPattern);

        DataStream<List<LoginWarning>> loginFailDataStream = patternStream.select(new PatternSelectFunction<LoginEvent, List<LoginWarning>>() {
            @Override
            public List<LoginWarning> select(Map<String, List<LoginEvent>> pattern) throws Exception {

                List<LoginWarning> list = new ArrayList<>();
               // List<LoginEvent> first = pattern.get("begin");
                List<LoginEvent> second = pattern.get("next");
               // fillParam(first,list);
                fillParam(second,list);
                return list;
            }
        });

        loginFailDataStream.print();
        env.execute();
    }

    private static void fillParam(List<LoginEvent> first, List<LoginWarning> list) {
        for (LoginEvent loginEvent:first){
            list.add(new LoginWarning(loginEvent.getUserId(),loginEvent.getType(),loginEvent.getIp()));
        }

    }

    public static class LoginEvent implements Serializable {

        private String userId;//用户ID

        private String ip;//登录IP

        private String type;//登录类型

        public String getUserId() {

            return userId;
        }

        public void setUserId(String userId) {

            this.userId = userId;
        }

        public String getIp() {

            return ip;
        }

        public void setIp(String ip) {

            this.ip = ip;
        }

        public String getType() {

            return type;
        }

        public void setType(String type) {

            this.type = type;
        }

        public LoginEvent() {

        }

        public LoginEvent(String userId, String ip, String type) {

            this.userId = userId;
            this.ip = ip;
            this.type = type;
        }
        // gets sets
    }

    public static class LoginWarning implements Serializable {

        private String userId;

        private String type;

        private String ip;

        public String getUserId() {

            return userId;
        }

        public void setUserId(String userId) {

            this.userId = userId;
        }

        public String getType() {

            return type;
        }

        public void setType(String type) {

            this.type = type;
        }

        public String getIp() {

            return ip;
        }

        public void setIp(String ip) {

            this.ip = ip;
        }

        public LoginWarning() {

        }

        public LoginWarning(String userId, String type, String ip) {

            this.userId = userId;
            this.type = type;
            this.ip = ip;
        }
        @Override
        public String toString() {
            return "LoginWarning{" +
                    "userId='" + userId + '\'' +
                    ", type='" + type + '\'' +
                    ", ip='" + ip + '\'' +
                    '}';
        }
    }

}
