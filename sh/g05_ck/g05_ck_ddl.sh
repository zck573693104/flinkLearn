./bin/flink run -m yarn-cluster -ynm g05_ck_ddl -ys 1 -c com.hycan.bigdata.job.SchemaJob \
/opt/Bigdata/jar/flink-sql-parser-0.0.1-SNAPSHOT.jar \
--path /load/data/g05_ck_ddl.sql --active pro