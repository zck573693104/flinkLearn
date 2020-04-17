cd /usr/local/flink-1.9.0 &&  ./bin/flink run -m yarn-cluster -yn 2 -yjm 2048 -ytm 2048 -ynm pro_searchRecord_detal \
-c HotItemsProcessJob  \
/usr/local/project/flink-log/target/flink-demo-0.0.1-SNAPSHOT.jar -d \
--group_name flinkProdCARTUserBehaviorGroup --topic_name prodUserBehaviorTopic \
--behavior searchRecord --redis_key ZSET_NAME_PRODUCT_SEARCH_RECORD_PREFIX --prod_or_test test