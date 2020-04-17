package com.flink.order.rich;

import com.flink.order.vo.OrderVO;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.functions.FilterFunction;

import java.util.Date;

public class FilTerOldOrderData implements FilterFunction<OrderVO> {

    @Override
    public boolean filter(OrderVO orderVO)  {
        if (orderVO.getOrderCreateTime() == null) {
            return false;
        }
        if (DateUtils.isSameDay(orderVO.getOrderCreateTime(),new Date())) {
            return true;
        }

        return false;
    }
}
