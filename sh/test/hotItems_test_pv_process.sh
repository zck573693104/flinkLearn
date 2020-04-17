cd /usr/local/flink-1.9.0 &&  ./bin/flink run -m yarn-cluster -yn 1 -yjm 2048 -ytm 2048 -ynm test_pv_detal \
-c org.fuwushe.order.job.HotItemsProcessJob  \
/usr/local/project/flink-log/target/flink-demo-0.0.1-SNAPSHOT.jar -d \
--group_name flinkTESTPVUserBehaviorGroup --topic_name testUserBehaviorTopic --behavior pv  \
--redis_key ZSET_NAME_PRODUCT_PV_PREFIX \
--prod_or_test test