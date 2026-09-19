-- Flink 血缘追踪表结构

-- 表血缘关系表
CREATE TABLE IF NOT EXISTS table_lineage (
    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键 ID',
    job_id VARCHAR(100) NOT NULL COMMENT 'Flink JobID',
    job_name VARCHAR(255) COMMENT '作业名称',
    sql TEXT COMMENT 'SQL 语句',
    source_table VARCHAR(255) NOT NULL COMMENT '源表名',
    target_table VARCHAR(255) NOT NULL COMMENT '目标表名',
    process_type VARCHAR(50) COMMENT '处理类型 (INSERT/SELECT/UPDATE/DELETE)',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    remark TEXT COMMENT '备注信息',
    
    INDEX idx_job_id (job_id),
    INDEX idx_source_table (source_table),
    INDEX idx_target_table (target_table),
    INDEX idx_create_time (create_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='表血缘关系表';

-- 表信息表
CREATE TABLE IF NOT EXISTS table_info (
    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键 ID',
    table_name VARCHAR(255) NOT NULL COMMENT '表名',
    database_name VARCHAR(255) COMMENT '数据库名',
    table_type VARCHAR(50) COMMENT '表类型 (TABLE/VIEW/TEMPORARY_TABLE)',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    remark TEXT COMMENT '备注',
    
    UNIQUE KEY uk_table_name (table_name, database_name),
    INDEX idx_create_time (create_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='表信息表';

-- 字段血缘表（可选，用于字段级别的追踪）
CREATE TABLE IF NOT EXISTS column_lineage (
    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键 ID',
    lineage_id BIGINT COMMENT '关联的表血缘 ID',
    source_column VARCHAR(255) NOT NULL COMMENT '源字段',
    target_column VARCHAR(255) NOT NULL COMMENT '目标字段',
    
    INDEX idx_lineage_id (lineage_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='字段血缘表';

-- 插入示例数据
INSERT INTO table_lineage (job_id, job_name, sql, source_table, target_table, process_type, remark) 
VALUES 
('job_123456789', 'Kafka to MySQL ETL', 
 'INSERT INTO mysql_table SELECT * FROM kafka_topic', 
 'kafka_topic', 'mysql_table', 'INSERT', 'Kafka 数据同步到 MySQL'),
('job_234567890', 'Data Aggregation', 
 'INSERT INTO agg_table SELECT user_id, COUNT(*) FROM mysql_table GROUP BY user_id', 
 'mysql_table', 'agg_table', 'INSERT', '用户行为聚合');
