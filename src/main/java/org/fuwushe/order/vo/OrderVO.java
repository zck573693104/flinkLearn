package org.fuwushe.order.vo;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;

@Data
public class OrderVO implements Serializable {


    private static final long serialVersionUID = -1196475280436815036L;

    private long areaId;

    private long subCustomerId;

    private long orderId;

    //一笔订单的总实收
    private double accountPayAmount;

    private long supplierId;

    private long itemId;

    private String productCategoryCode;

    private long orderQty;

    private double price;

    //订单类型（dc：配送，supply是直送）
    private String orderType;

    //order  orderDetail 为了flink区分计算
    private String type;

    private Date orderCreateTime;
}
