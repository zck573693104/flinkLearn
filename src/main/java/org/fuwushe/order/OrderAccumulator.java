package org.fuwushe.order;

import lombok.Data;
import org.fuwushe.utils.FMathUtil;

import java.io.Serializable;
import java.util.HashSet;

@Data
public class OrderAccumulator implements Serializable {

    private static final long serialVersionUID = -3121020864238485990L;

    private String productCategoryCode;

    private String date;

    private String orderType;

    private long areaId;

    private long itemId;

    private long subCustomerId;

    private long supplierId;

    private long orderQuantitySum;

    private HashSet<Long> orderIdset;

    private long subOrderDetailSum;

    private double gmv;

    public void addQuantitySum(long quantity) {

        this.orderQuantitySum += quantity;

    }

    public void addSubOrderdetailSum(long subOrderDetailSum) {

        this.subOrderDetailSum += subOrderDetailSum;

    }

    public void addGmv(double gmv) {

        this.gmv = FMathUtil.add(this.gmv, gmv);

    }

    public void addOrderId(long orderId) {
        if (this.orderIdset ==null){
            this.orderIdset = new HashSet<>();
        }
        this.orderIdset.add(orderId);
    }

    public void addOrderIds(HashSet<Long> orderIds) {

        this.orderIdset.addAll(orderIds);

    }
}
