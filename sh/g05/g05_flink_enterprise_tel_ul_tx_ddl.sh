./bin/flink run -m yarn-cluster -ynm new_g05_flink_enterprise_tel_ul_tx_ddl -ys 5 -c com.hycan.bigdata.job.SchemaJob \
/opt/Bigdata/jar/flink-sql-parser-0.0.1-SNAPSHOT.jar \
--path /load/data/new_g05_flink_enterprise_tel_ul_tx_ddl.sql --active pro





