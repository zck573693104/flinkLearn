cd /usr/local/flink-1.9.0 &&  ./bin/flink run -m yarn-cluster -yn 4 -yjm 5012 -ytm 5012 -ynm prod_order_detail_detal \
-c org.fuwushe.order.job.OrderDetailProcessJob  \
/usr/local/project/flink-log/target/flink-demo-0.0.1-SNAPSHOT.jar -d \
--group_name flinkProdOrderDetailGroup --topic_name prodOrderDetailTopic \
--prod_or_test prod