CREATE TABLE kafka_table (
                             vid STRING,
                             brake DOUBLE,
                             hard_to DOUBLE,
                             log_ts TIMESTAMP(3),
                             WATERMARK FOR log_ts AS log_ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json'
);
select vid,count(if(brake > 3.0451)) as brakes,count(if(hard_to > 3.0451)) as hard_tos
from (
         select *, row_number() over (partition by vid order by log_ts desc)
     );


