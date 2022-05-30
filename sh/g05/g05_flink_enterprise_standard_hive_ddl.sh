./bin/flink run -m yarn-cluster -ynm g05_type -ys 1 -c com.hycan.bigdata.job.SchemaJob \
/opt/Bigdata/jar/flink-sql-parser-0.0.1-SNAPSHOT.jar \
--path /load/data/g05_ck_ddl.sql





./bin/flink run -m yarn-cluster -ynm hive -ys 2 -c com.hycan.bigdata.job.SchemaJob /opt/Bigdata/jar/flink-sql-parser-0.0.1-SNAPSHOT.jar --path /load/data/hive.sql --parallelism 2 --chk_time 400 --hive_parallelism 5 --jobType BATCH