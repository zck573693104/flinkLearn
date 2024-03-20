use ods_national_standard;
drop table if exists hive_table;
drop table if exists kafka_table;
SET table.sql-dialect=hive;
CREATE TABLE hive_table (
  user_id STRING,
  order_amount DOUBLE
) PARTITIONED BY (dt STRING) STORED AS orc TBLPROPERTIES (
    'partition.time-extractor.timestamp-pattern'='$dt',
    'sink.partition-commit.trigger'='partition-time',
    'sink.partition-commit.delay'='0 s',
    'sink.partition-commit.watermark-time-zone'='Asia/Shanghai',
    'sink.partition-commit.policy.kind'='metastore,success-file'
);
SET table.sql-dialect=default;

CREATE TABLE kafka_table (
  user_id STRING,
  order_amount DOUBLE,
  log_ts TIMESTAMP(3),
  WATERMARK FOR log_ts AS log_ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_behavior',
  'properties.bootstrap.servers' = '192.168.0.115:9092,192.168.0.244:9092,192.168.0.3:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json'
);


