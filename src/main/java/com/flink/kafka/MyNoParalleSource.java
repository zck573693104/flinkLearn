package com.flink.kafka;

import com.alibaba.fastjson.JSONObject;
import com.flink.order.vo.OrderVO;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.LocalDateTime;
import java.util.Random;

//使用并行度为1的source
public class MyNoParalleSource implements SourceFunction<String> {//1

    //private long count = 1L;
    private boolean isRunning = true;
    
    /**
     * 主要的方法
     * 启动一个source
     * 大部分情况下，都需要在这个run方法中实现一个循环，这样就可以循环产生数据了
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while(isRunning){
                //图书的排行榜
                Random random = new Random();
//                UserBehaviorVO userBehaviorVO = new UserBehaviorVO();
//                userBehaviorVO.setBehavior("pv");
//                userBehaviorVO.setCategoryId(1L);
//                userBehaviorVO.setItemId(Long.valueOf(random.nextInt(10)));
//                userBehaviorVO.setSubCustomerId(1L);
//                userBehaviorVO.setTimestamp(System.currentTimeMillis());
//                userBehaviorVO.setUserId(1L);
            OrderVO orderVO = new OrderVO();
            orderVO.setAreaId(1L);
            orderVO.setOrderId(1L);
            orderVO.setItemId(1L);
            orderVO.setOrderQty(1L);
            orderVO.setPrice(100D);

                ctx.collect(JSONObject.toJSONString(orderVO));
                System.out.println(LocalDateTime.now()+":"+JSONObject.toJSONString(orderVO));
                //每2秒产生一条数据
                Thread.sleep(10000);
        }
    }
    //取消一个cancel的时候会调用的方法
    @Override
    public void cancel() {
        isRunning = false;
    }
}