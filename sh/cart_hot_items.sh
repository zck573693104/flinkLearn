cd /usr/local/flink-1.9.0 &&  ./bin/flink run -m yarn-cluster -yn 1 -yjm 1024 -ytm 1024 -ynm cart_detal \
-c HotItemsEvnJob  \
/usr/local/project/flink-log/target/flink-demo-0.0.1-SNAPSHOT.jar -d --group_name flinkProdCARTUserBehaviorGroup  --behavior cart