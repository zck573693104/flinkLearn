use ods_enterprise;
drop table if exists user_app_data;
drop table if exists kafka_user_app_data;
SET table.sql-dialect=hive;
create table user_app_data(
       app_name string,
       app_version string,
       app_func string,
       latitude string,
       event_name string,
       app_id string,
       event_value map<string,string>,
       event_time string,
       longitude string,
       info_str string,
       `date` string,
       hardware_ver string,
       device_id string,
       device_os_name string,
       device_os_ver string,
       net_type string,
       vehicle_type string,
       platform string,
       mcu_software_ver string,
       car_vin string,
       protocol_version string,
       account_id string,
       device_brand string,
       ai_in_car_id string,
       car_brand string,
       event_number string
) PARTITIONED BY (dt STRING) STORED AS parquet TBLPROPERTIES (
    'parquet.compression'='snappy',
    'partition.time-extractor.timestamp-pattern'='$dt',
    'sink.partition-commit.trigger'='partition-time',
    'sink.partition-commit.delay'='1 h',
    'sink.partition-commit.watermark-time-zone'='Asia/Shanghai',
    'sink.partition-commit.policy.kind'='metastore,success-file'
    );

SET table.sql-dialect=default;

create table kafka_user_app_data (
                               eventBodyList array<row<
                                   app_name string,
                               app_version string,
                               app_func string,
                               latitude string,
                               event_name string,
                               app_id string,
                               event_value map<string,string>,
                               event_time string,
                               longitude string>>,
                               operation row<
                                   info_str string,
                               `date` string,
                               hardware_ver string,
                               device_id string,
                               device_os_name string,
                               device_os_ver string,
                               net_type string,
                               vehicle_type string,
                               platform string,
                               mcu_software_ver string,
                               car_vin string,
                               protocol_version string,
                               account_id string,
                               device_brand string,
                               ai_in_car_id string,
                               car_brand string,
                               event_number string>
) WITH (
   'connector' = 'kafka',
  'topic' = 'avntTopic',
  'properties.bootstrap.servers' = '${kafka.gb.bootstrap.servers}',
  'properties.group.id' = 'avntTopic_group_id',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);