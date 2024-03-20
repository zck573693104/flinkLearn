./bin/flink run -m yarn-cluster -ynm flink_user_app_data_ddl -ys 2 -c com.hycan.bigdata.job.SchemaJob \
/opt/Bigdata/jar/flink-sql-parser-0.0.1-SNAPSHOT.jar \
--path /load/data/flink_user_app_data_ddl.sql --active pro





