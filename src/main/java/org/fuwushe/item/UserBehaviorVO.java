package org.fuwushe.item;

import lombok.Data;

/** 用户行为数据结构 **/
@Data
public class UserBehaviorVO  {


    public Long areaId;        //省id

    public Long userId;         // 用户ID

    public Long subCustomerId;  //门店id

    public Long supplierId;    //服务商id

    public String itemId;         // 商品ID

    public String content;         // 热搜内容

    public Long categoryId;      // 商品类目ID

    private String productCategoryCode;

    public String behavior;     // 用户行为, 包括("pv 点击 0.1", "buy 购买 2", "cart 加入购物车 0.5", "fav 收藏 0.5")

    public Long timestamp;      // 行为发生的时间戳，单位毫秒
}