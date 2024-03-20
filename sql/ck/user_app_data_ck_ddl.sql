use ods_enterprise;
drop table if exists user_app_data_ck;
drop table if exists user_app_data_ck_kafka;
create table user_app_data_ck
(
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
    event_number string,
    dt DATE
) with (
 'connector' = 'clickhouse',
    'url' = 'clickhouse://172.26.112.212:8123',
    'database-name' = 'ods_enterprise',
    'table-name' = 'user_app_data',
    'sink.batch-size' = '20000',
    'sink.flush-interval' = '30000',
    'sink.max-retries' = '3',
	'username'='ck_admin',
	'password'='Gacnio-wlzx@2021',
	'use-local'='true'
);

create table user_app_data_ck_kafka (
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
                                        event_number string>) WITH (
   'connector' = 'kafka',
  'topic' = 'hycan-period-compress',
  'properties.bootstrap.servers' = '${kafka.gb.bootstrap.servers}',
  'properties.group.id' = 'new_ck_enterprise_tel_ul_tx',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json'
);
