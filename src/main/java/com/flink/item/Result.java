package com.flink.item;

import lombok.Data;

import java.io.Serializable;


@Data
public class Result implements Serializable {

    private static final long serialVersionUID = 1422543153662821631L;

    public String itemId;     // 商品ID

    public String windowEndDate;  // 窗口结束时间戳

    public long viewCount;  // 商品的点击量

    public String behavior;
}
