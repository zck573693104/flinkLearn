use ods_enterprise;
insert into user_app_data
SELECT
    app_name,
    app_version,
    app_func,
    latitude,
    event_name,
    app_id,
    event_value,
    event_time,
    longitude,
    operation.info_str,
    operation.`date`,
    operation.hardware_ver,
    operation.device_id,
    operation.device_os_name,
    operation.device_os_ver,
    operation.net_type,
    operation.vehicle_type,
    operation.platform,
    operation.mcu_software_ver,
    operation.car_vin,
    operation.protocol_version,
    operation.account_id,
    operation.device_brand,
    operation.ai_in_car_id,
    operation.car_brand,
    operation.event_number,
    if (operation.`date` is not  null and operation.`date` <> '',DATE_FORMAT( operation.`date`, 'yyyy-MM-dd' ),'9999-99-99')

FROM
    kafka_user_app_data, UNNEST ( eventBodyList ) AS t ( app_name, app_version, app_func, latitude, event_name, app_id, event_value, event_time, longitude );




