package com.flink.constant;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class RedisConstants {


    //省
    public static  String HASH_NAME_PROVINCE_PREFIX ;
    //门店
    public static  String HASH_NAME_SUB_CUSTOMER_PREFIX ;
    //一级品类
    public static  String HASH_NAME_CATEGORY_CODE_PREFIX ;

    //商品的热门销量排行榜
    public static  String ZSET_NAME_PRODUCT_QTY_PREFIX ;
    //pv
    public static  String ZSET_NAME_PRODUCT_PV_PREFIX ;
    //cart
    public static  String ZSET_NAME_PRODUCT_CART_PREFIX ;
    //SEARCH_RECORD
    public static  String ZSET_NAME_PRODUCT_SEARCH_RECORD_PREFIX ;


    public static Map<String,String> setAndGet(String pordOrTest) throws IOException {

        Map<String,String> map = new HashMap<>();
        InputStream resourceAsStream = null;
        if (pordOrTest.contains("prod")){
            resourceAsStream = RedisConstants.class.getResourceAsStream("/constants-prod.properties");
        }
        else {
            resourceAsStream = RedisConstants.class.getResourceAsStream("/constants-test.properties");
        }
        Properties properties = new Properties();
        properties.load(resourceAsStream);
        HASH_NAME_PROVINCE_PREFIX = properties.getProperty("HASH_NAME_PROVINCE_PREFIX");
        HASH_NAME_SUB_CUSTOMER_PREFIX = properties.getProperty("HASH_NAME_SUB_CUSTOMER_PREFIX");
        HASH_NAME_CATEGORY_CODE_PREFIX = properties.getProperty("HASH_NAME_CATEGORY_CODE_PREFIX");
        ZSET_NAME_PRODUCT_QTY_PREFIX = properties.getProperty("ZSET_NAME_PRODUCT_QTY_PREFIX");
        ZSET_NAME_PRODUCT_PV_PREFIX = properties.getProperty("ZSET_NAME_PRODUCT_PV_PREFIX");
        ZSET_NAME_PRODUCT_CART_PREFIX = properties.getProperty("ZSET_NAME_PRODUCT_CART_PREFIX");
        ZSET_NAME_PRODUCT_SEARCH_RECORD_PREFIX = properties.getProperty("ZSET_NAME_PRODUCT_SEARCH_RECORD_PREFIX");

        map.put("HASH_NAME_PROVINCE_PREFIX",HASH_NAME_PROVINCE_PREFIX);
        map.put("HASH_NAME_SUB_CUSTOMER_PREFIX",HASH_NAME_SUB_CUSTOMER_PREFIX);
        map.put("HASH_NAME_CATEGORY_CODE_PREFIX",HASH_NAME_CATEGORY_CODE_PREFIX);
        map.put("ZSET_NAME_PRODUCT_QTY_PREFIX",ZSET_NAME_PRODUCT_QTY_PREFIX);
        map.put("ZSET_NAME_PRODUCT_PV_PREFIX",ZSET_NAME_PRODUCT_PV_PREFIX);
        map.put("ZSET_NAME_PRODUCT_CART_PREFIX",ZSET_NAME_PRODUCT_CART_PREFIX);
        map.put("ZSET_NAME_PRODUCT_SEARCH_RECORD_PREFIX",ZSET_NAME_PRODUCT_SEARCH_RECORD_PREFIX);
        return map;
    }

}
