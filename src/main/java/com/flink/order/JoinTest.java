package com.flink.order;

import com.flink.order.vo.OrderVO;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import com.flink.order.vo.AreaVO;

import java.util.ArrayList;
import java.util.List;


public class JoinTest {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<OrderVO> orderVOList = new ArrayList<>();
        List<AreaVO> areaVOList = new ArrayList<>();
        OrderVO orderVO = new OrderVO();
        orderVO.setAreaId(1L);
        orderVO.setAreaId(1L);
        orderVOList.add(orderVO);

        AreaVO areaVO = new AreaVO();
        areaVO.setId(1L);
        areaVO.setName("北京");
        areaVOList.add(areaVO);
        DataStream<OrderVO> orderStream = env.fromCollection(orderVOList);
        DataStream<AreaVO> areaStream = env.fromCollection(areaVOList);
        orderStream.join(areaStream).where(new KeySelector<OrderVO, Long>() {
            @Override
            public Long getKey(OrderVO orderVO) throws Exception {

                return orderVO.getAreaId();
            }
        }).equalTo(new KeySelector<AreaVO, Long>() {
            @Override
            public Long getKey(AreaVO areaVO) throws Exception {

                return areaVO.getId();
            }
        }).window(TumblingProcessingTimeWindows.of(Time.seconds(1))).apply(new JoinFunction<OrderVO, AreaVO, String>() {
            @Override
            public String join(OrderVO orderVO, AreaVO areaVO) throws Exception {

                return areaVO.getName();
            }
        }).print();
        env.execute("join test");
    }
}
