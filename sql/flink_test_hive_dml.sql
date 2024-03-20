use ods_national_standard;
INSERT INTO hive_table (user_id,order_amount)
SELECT user_id, order_amount, DATE_FORMAT(log_ts, 'yyyy-MM-dd')
FROM kafka_table;