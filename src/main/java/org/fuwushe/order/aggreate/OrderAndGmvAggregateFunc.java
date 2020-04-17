package org.fuwushe.order.aggreate;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.fuwushe.order.OrderAccumulator;
import org.fuwushe.order.vo.OrderVO;
import org.fuwushe.utils.DateUtil;
import org.fuwushe.utils.FMathUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;

public class OrderAndGmvAggregateFunc implements AggregateFunction<OrderVO, OrderAccumulator, OrderAccumulator> {

    private static Logger logger = LoggerFactory.getLogger(OrderAndGmvAggregateFunc.class);

    @Override
    public OrderAccumulator createAccumulator() {

        return new OrderAccumulator();
    }

    @Override
    public OrderAccumulator add(OrderVO orderVO, OrderAccumulator acc) {

        double gmv = 0D;
        if (orderVO.getType().equals("order")) {
            gmv = orderVO.getAccountPayAmount();

        }
        if (orderVO.getType().equals("orderDetail")) {
            gmv = FMathUtil.mul(orderVO.getPrice(), Double.valueOf(orderVO.getOrderQty()));

        }
        // TODO: 2019/12/17 今天启动 却处理的第一数据竟然是 负数的，应该是正数先到
        if (gmv < 0 && acc.getOrderIdset()== null){
            return acc;
        }
        if (gmv >= 0) {
            acc.addSubOrderdetailSum(1L);
            acc.addOrderId(orderVO.getOrderId());
        } else if (gmv < 0) {
            acc.addSubOrderdetailSum(-1L);
            HashSet<Long> orderIdset = acc.getOrderIdset();
            orderIdset.remove(orderVO.getOrderId());
        }
        acc.addGmv(gmv);
        acc.setItemId(orderVO.getItemId());
        acc.setProductCategoryCode(orderVO.getProductCategoryCode());
        acc.addQuantitySum(orderVO.getOrderQty());
        acc.setAreaId(orderVO.getAreaId());
        acc.setSupplierId(orderVO.getSupplierId());
        acc.setSubCustomerId(orderVO.getSubCustomerId());
        acc.setOrderType(orderVO.getOrderType());
        acc.setDate(DateUtil.dateToStr(orderVO.getOrderCreateTime(), DateUtil.YYYY_MM_DD));
        return acc;
    }

    @Override
    public OrderAccumulator getResult(OrderAccumulator acc) {

        return acc;
    }

    @Override
    public OrderAccumulator merge(OrderAccumulator acc1, OrderAccumulator acc2) {

        acc1.addOrderIds(acc2.getOrderIdset());
        acc1.addSubOrderdetailSum(acc2.getSubOrderDetailSum());
        acc1.addQuantitySum(acc2.getOrderQuantitySum());
        acc1.addGmv(acc2.getGmv());
        return acc1;
    }
}
