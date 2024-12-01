create table k8s_kafka (
id STRING,
datas string
) WITH (
      'connector' = 'kafka',
      'topic' = 'demo_topic',
      'properties.bootstrap.servers' = '127.0.0.1:9092',
      'properties.group.id' = 'k8s_sr_group',
      'format' = 'json',
      'scan.startup.mode' = 'latest-offset',
      'scan.topic-partition-discovery.interval' = '5000'
      );



     select id,datas from k8s_kafka;
     
	 
	 
	 