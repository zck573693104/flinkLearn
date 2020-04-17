cd /usr/local/flink-1.9.0 &&  ./bin/flink run -m yarn-cluster -yn 2 -yjm 2048 -ytm 2048 -ynm test_cart_detal \
-c HotItemsProcessJob  \
/usr/local/project/flink-log/target/flink-demo-0.0.1-SNAPSHOT.jar -d \
--group_name flinkTESTCARTUserBehaviorGroup  --topic_name testUserBehaviorTopic \
--behavior cart --redis_key ZSET_NAME_PRODUCT_CART_PREFIX \
--prod_or_test test