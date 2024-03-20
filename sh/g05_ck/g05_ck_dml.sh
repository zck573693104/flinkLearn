./bin/flink run -m yarn-cluster -ynm g05_ck_ddl -ys 3 -c com.hycan.bigdata.job.SchemaJob \
/opt/Bigdata/jar/flink-sql-parser-0.0.1-SNAPSHOT.jar \
--path /load/data/hbase.sql \
--parallelism 5



../bin/flink run -m yarn-cluster -yt ssl/ -ys 2 -p 4 \
-c com.hycan.bigdata.job.SchemaJob \
/opt/Bigdata/jar/flink-sql-parser-0.0.1-SNAPSHOT.jar \
--path /load/data/hbase.sql \
--parallelism 5