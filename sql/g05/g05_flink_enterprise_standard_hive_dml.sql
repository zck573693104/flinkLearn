use ods_enterprise;
INSERT INTO g05_enterprise_standard
SELECT
    CONCAT_WS( '&', gatherTime, vin ) AS id,
    TO_TIMESTAMP(FROM_UNIXTIME(CAST(gatherTime AS bigint)/ 1000, 'yyyy-MM-dd HH:mm:ss')) AS receive_time,
    TO_TIMESTAMP(FROM_UNIXTIME(CAST(gatherTime AS bigint)/ 1000, 'yyyy-MM-dd HH:mm:ss')) As message_time,
    vin,
    'message_type',

    cast( zhengcheshuju[1].signalValue AS INT ) as vehicle_status,
    cast( zhengcheshuju[2].signalValue AS INT ) as charge_status,
    cast( zhengcheshuju[3].signalValue AS INT ) as operation_mode,
    cast( zhengcheshuju[4].signalValue AS DOUBLE ) as speed,
    cast( zhengcheshuju[5].signalValue AS DOUBLE ) as mileage,
    cast( zhengcheshuju[6].signalValue AS DOUBLE ) as total_volt,
    cast( zhengcheshuju[7].signalValue AS DOUBLE ) as total_current,
    cast( zhengcheshuju[8].signalValue AS DOUBLE ) as soc,
    cast( zhengcheshuju[9].signalValue AS INT ) as dc_dc_status,
    cast( zhengcheshuju[10].signalValue AS INT ) as gear,
    cast( zhengcheshuju[11].signalValue AS INT ) as insulation_resistance,
    cast( zhengcheshuju[12].signalValue AS DOUBLE ) as accelerator_value,
    cast( zhengcheshuju[13].signalValue AS DOUBLE ) as brake_value,

    cast( qudongdianjishuju[1][1].signalValue AS INT ) as motor_sn_1,
    cast( qudongdianjishuju[1][2].signalValue AS INT ) as status_1,
    cast( qudongdianjishuju[1][3].signalValue AS DOUBLE ) as controller_temp_1,
    cast( qudongdianjishuju[1][4].signalValue AS DOUBLE ) as rotation_speed_1,
    cast( qudongdianjishuju[1][5].signalValue AS DOUBLE ) as rotation_torque_1,
    cast( qudongdianjishuju[1][6].signalValue AS DOUBLE ) as temp_1,
    cast( qudongdianjishuju[1][7].signalValue AS DOUBLE ) as controller_in_volt_1,
    cast( qudongdianjishuju[1][8].signalValue AS DOUBLE ) as controller_dc_bus_current_1,
    cast( qudongdianjishuju[2][1].signalValue AS INT ),
    cast( qudongdianjishuju[2][2].signalValue AS INT ),
    cast( qudongdianjishuju[2][3].signalValue AS DOUBLE ),
    cast( qudongdianjishuju[2][4].signalValue AS DOUBLE ),
    cast( qudongdianjishuju[2][5].signalValue AS DOUBLE ),
    cast( qudongdianjishuju[2][6].signalValue AS DOUBLE ),
    cast( qudongdianjishuju[2][7].signalValue AS DOUBLE ),
    cast( qudongdianjishuju[2][8].signalValue AS DOUBLE ),
    cast( qudongdianjishuju[3][1].signalValue AS INT ),
    cast( qudongdianjishuju[3][2].signalValue AS INT ),
    cast( qudongdianjishuju[3][3].signalValue AS DOUBLE ),
    cast( qudongdianjishuju[3][4].signalValue AS DOUBLE ),
    cast( qudongdianjishuju[3][5].signalValue AS DOUBLE ),
    cast( qudongdianjishuju[3][6].signalValue AS DOUBLE ),
    cast( qudongdianjishuju[3][7].signalValue AS DOUBLE ),
    cast( qudongdianjishuju[3][8].signalValue AS DOUBLE ),
    cast( qudongdianjishuju[4][1].signalValue AS INT ),
    cast( qudongdianjishuju[4][2].signalValue AS INT ),
    cast( qudongdianjishuju[4][3].signalValue AS DOUBLE ),
    cast( qudongdianjishuju[4][4].signalValue AS DOUBLE ),
    cast( qudongdianjishuju[4][5].signalValue AS DOUBLE ),
    cast( qudongdianjishuju[4][6].signalValue AS DOUBLE ),
    cast( qudongdianjishuju[4][7].signalValue AS DOUBLE ),
    cast( qudongdianjishuju[4][8].signalValue AS DOUBLE ),

    cast(weizhishuju[1].signalValue as int) AS loaction_status,
    weizhishuju[2].signalValue AS loaction_longitude,
    weizhishuju[3].signalValue AS loaction_latitude,

    cast( jizhishuju[1].signalValue AS INT ) as highest_volt_battery_pkg_sn,
    cast( jizhishuju[2].signalValue AS INT ) as highest_volt_cell_sn,
    cast( jizhishuju[3].signalValue AS DOUBLE ) as cell_highest_volt,
    cast( jizhishuju[4].signalValue AS INT ) as lowest_volt_battery_pkg_sn,
    cast( jizhishuju[5].signalValue AS INT ) as lowest_volt_cell_sn,
    cast( jizhishuju[6].signalValue AS DOUBLE ) as cell_lowest_volt,
    cast( jizhishuju[7].signalValue AS INT ) as highest_temp_battery_pkg_sn,
    cast( jizhishuju[8].signalValue AS INT ) as highest_temp_probe_sn,
    cast( jizhishuju[9].signalValue AS DOUBLE ) as highest_temp,
    cast( jizhishuju[10].signalValue AS INT ) as lowest_temp_battery_pkg_sn,
    cast( jizhishuju[11].signalValue AS INT ) as lowest_temp_probe_sn,
    cast( jizhishuju[12].signalValue AS DOUBLE ) as lowest_temp,

    cast( baojingshuju[1].signalValue AS INT ) as alarm_level,
    cast( baojingshuju[2].signalValue AS STRING ) as alarm_flag,
    cast( baojingshuju[3].signalValue AS INT ) as alarm_reess_fault_cnt,
    cast( baojingshuju[4].signalValue AS STRING ) as reess_fault_list,
    cast( baojingshuju[5].signalValue AS INT ) as motor_fault_cnt,
    cast( baojingshuju[6].signalValue AS STRING ) as motor_fault_list,
    cast( baojingshuju[7].signalValue AS INT ) as engine_fault_cnt,
    cast( baojingshuju[8].signalValue AS STRING ) as engine_fault_list,
    cast( baojingshuju[9].signalValue AS INT ) as other_fault_cnt,
    cast( baojingshuju[10].signalValue AS STRING ) as other_fault_list ,

    baoJingBiaoZhiBinaryValue,
    alarmSize,

    dianyashuju[1].`no` as pressure_no  ,
    dianyashuju[1].totalDianya  ,
    cast(dianyashuju[1].begCellSerialNo as int) ,
    dianyashuju[1].totalDianliu  ,
    dianyashuju[1].totalCellCount  ,
    dianyashuju[1].bzdtdcCount  ,
    dianyashuju[1].dianyaValues  ,

    wendushuju[1].probeCount,
    wendushuju[1].wenduValues,
    DATE_FORMAT( log_ts, 'yyyy-MM-dd' )
FROM
    g05_kafka_enterprise_standard;