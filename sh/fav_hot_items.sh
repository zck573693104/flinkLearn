cd /usr/local/flink-1.9.0 &&  ./bin/flink run -m yarn-cluster -yn 1 -yjm 1024 -ytm 1024 -ynm cart_detal \
-c org.fuwushe.item.job.HotItemsEvnJob  \
/usr/local/project/flink-log/target/flink-demo-0.0.1-SNAPSHOT.jar \
--group_name flinkProdcartUserBehaviorGroup  --behavior cart \
--prod_or_test test --topic_name testUserBehaviorTopic -d