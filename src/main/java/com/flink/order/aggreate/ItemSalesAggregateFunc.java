package com.flink.order.aggreate;

import com.flink.order.vo.OrderVO;
import org.apache.flink.api.common.functions.AggregateFunction;

public  class ItemSalesAggregateFunc implements AggregateFunction<OrderVO, Long, Long> {


    @Override
    public Long createAccumulator() {

        return 0L;
    }

    @Override
    public Long add(OrderVO value, Long acc) {

        return acc + value.getOrderQty();
    }

    @Override
    public Long getResult(Long acc) {

        return acc;
    }

    @Override
    public Long merge(Long acc1, Long acc2) {

        return acc1 + acc2;
    }
}



