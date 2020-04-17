package com.flink.item;

import lombok.Data;

import java.io.Serializable;

/** 商品点击量(窗口操作的输出类型) */
@Data
public class ItemViewCount implements Serializable {

    private static final long serialVersionUID = 921290186172653382L;

    public String itemId;     // 商品ID

    public long windowEnd;  // 窗口结束时间戳

    public long viewCount;  // 商品的点击量

    public static ItemViewCount of(String itemId, long windowEnd, long viewCount) {

        ItemViewCount result = new ItemViewCount();
        result.itemId = itemId;
        result.windowEnd = windowEnd;
        result.viewCount = viewCount;
        return result;
    }
}
