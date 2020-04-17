package org.fuwushe.order.rich;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.fuwushe.order.vo.OrderVO;

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
