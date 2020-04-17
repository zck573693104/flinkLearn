cd /usr/local/flink-1.9.0 &&  ./bin/flink run -m yarn-cluster -yn 1 -yjm 2048 -ytm 2048 -ynm pro_pv_detal \
-c HotItemsProcessJob  \
/usr/local/project/flink-log/target/flink-demo-0.0.1-SNAPSHOT.jar -d \
--group_name flinkProdPVUserBehaviorGroup  --behavior pv \
--topic_name prodUserBehaviorTopic --redis_key ZSET_NAME_PRODUCT_PV_PREFIX \
--prod_or_test prod