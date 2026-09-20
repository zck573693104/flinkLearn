lexer grammar PrestoSqlLexer;

// ============================================
// 关键字定义（继承 Flink/Spark 基础）
// ============================================

KW_INSERT: [Ii][Nn][Ss][Ee][Rr][Tt];
KW_INTO: [Ii][Nn][Tt][Oo];
KW_OVERWRITE: [Oo][Vv][Ee][Rr][Ww][Rr][Ii][Tt][Ee];
KW_SELECT: [Ss][Ee][Ll][Ee][Cc][Tt];
KW_FROM: [Ff][Rr][Oo][Mm];
KW_JOIN: [Jj][Oo][Ii][Nn];
KW_LEFT: [Ll][Ee][Ff][Tt];
KW_RIGHT: [Rr][Ii][Gg][Hh][Tt];
KW_FULL: [Ff][Uu][Ll][Ll];
KW_INNER: [Ii][Nn][Nn][Ee][Rr];
KW_OUTER: [Oo][Uu][Tt][Ee][Rr];
KW_ON: [Oo][Nn];
KW_WHERE: [Ww][Hh][Ee][Rr][Ee];
KW_GROUP: [Gg][Rr][Oo][Uu][Pp];
KW_BY: [Bb][Yy];
KW_HAVING: [Hh][Aa][Vv][Ii][Nn][Gg];
KW_ORDER: [Oo][Rr][Dd][Ee][Rr];
KW_LIMIT: [Ll][Ii][Mm][Ii][Tt];
KW_AS: [Aa][Ss];
KW_WITH: [Ww][Ii][Tt][Hh];
KW_CREATE: [Cc][Rr][Ee][Aa][Tt][Ee];
KW_TABLE: [Tt][Aa][Bb][Ll][Ee];
KW_VIEW: [Vv][Ii][Ee][Ww];
KW_CAST: [Cc][Aa][Ss][Tt];
KW_FUNCTION: [Ff][Uu][Nn][Cc][Tt][Ii][Oo][Nn];
KW_DISTINCT: [Dd][Ii][Ss][Tt][Ii][Nn][Cc][Tt];
KW_ALL: [Aa][Ll][Ll];
KW_TRUE: [Tt][Rr][Uu][Ee];
KW_FALSE: [Ff][Aa][Ll][Ss][Ee];
KW_NULL: [Nn][Uu][Ll][Ll];
KW_NOT: [Nn][Oo][Tt];
KW_AND: [Aa][Nn][Dd];
KW_OR: [Oo][Rr];
KW_IS: [Ii][Ss];
KW_FOR: [Ff][Oo][Rr];
KW_PARTITION: [Pp][Aa][Rr][Tt][Ii][Tt][Ii][Oo][Nn];
KW_PRIMARY: [Pp][Rr][Ii][Mm][Aa][Rr][Yy];
KW_KEY: [Kk][Ee][Yy];
KW_CONSTRAINT: [Cc][Oo][Nn][Ss][Tt][Rr][Aa][Ii][Nn][Tt];
KW_UNIQUE: [Uu][Nn][Ii][Qq][Uu][Ee];
KW_REFERENCES: [Rr][Ee][Ff][Ee][Rr][Ee][Nn][Cc][Ee][Ss];
KW_DEFAULT: [Dd][Ee][Ff][Aa][Uu][Ll][Tt];
KW_COMMENT: [Cc][Oo][Mm][Mm][Ee][Nn][Tt];
KW_PRIMARY_KEY: [Pp][Rr][Ii][Mm][Aa][Rr][Yy] [Kk][Ee][Yy];
KW_UNBOUNDED: [Uu][Nn][Bb][Oo][Uu][Nn][Dd][Ee][Dd];
KW_PRECEDING: [Pp][Rr][Ee][Cc][Ee][Dd][Ii][Nn][Gg];
KW_FOLLOWING: [Ff][Oo][Ll][Ll][Oo][Ww][Ii][Nn][Gg];
KW_CURRENT: [Cc][Uu][Rr][Rr][Ee][Nn][Tt];
KW_ROW: [Rr][Oo][Ww];
KW_ROWS: [Rr][Oo][Ww][Ss];
KW_RANGE: [Rr][Aa][Nn][Gg][Ee];
KW_BETWEEN: [Bb][Ee][Tt][Ww][Ee][Ee][Nn];
KW_IN: [Ii][Nn];
KW_UNION: [Uu][Nn][Ii][Oo][Nn];
KW_INTERSECT: [Ii][Nn][Tt][Ee][Rr][Ss][Ee][Cc][Tt];
KW_EXCEPT: [Ee][Xx][Cc][Ee][Pp][Tt] | [Mm][Ii][Nn][Uu][Ss];
KW_DROP: [Dd][Rr][Oo][Pp];
KW_ALTER: [Aa][Ll][Tt][Ee][Rr];

// ============================================
// Presto 特有关键字
// ============================================

KW_EXPLAIN: [Ee][Xx][Pp][Ll][Aa][Ii][Nn];
KW_ANALYZE: [Aa][Nn][Aa][Ll][Yy][Zz][Ee];
KW_EXECUTE: [Ee][Xx][Ee][Cc][Uu][Tt][Ee];
KW_DEALLOCATE: [Dd][Ee][Aa][Ll][Ll][Oo][Cc][Aa][Tt][Ee];
KW_PREPARE: [Pp][Rr][Ee][Pp][Aa][Rr][Ee];
KW_DESCRIBE: [Dd][Ee][Ss][Cc][Rr][Ii][Bb][Ee];
KW_SHOW: [Ss][Hh][Oo][Ww];
KW_SET: [Ss][Ee][Tt];
KW_RESET: [Rr][Ee][Ss][Ee][Tt];
KW_LOCK: [Ll][Oo][Cc][Kk];
KW_UNLOCK: [Uu][Nn][Ll][Oo][Cc][Kk];
KW_FLUSH: [Ff][Ll][Uu][Ss][Hh];
KW_TRUNCATE: [Tt][Rr][Uu][Nn][Cc][Aa][Tt][Ee];
KW_REPAIR: [Rr][Ee][Pp][Aa][Ii][Rr];
KW_OPTIMIZE: [Oo][Pp][Tt][Ii][Mm][Ii][Zz][Ee];
KW_VACUUM: [Vv][Aa][Cc][Uu][Uu][Mm];
KW_REFRESH: [Rr][Ee][Ff][Rr][Ee][Ss][Hh];
KW_INVALIDATE: [Ii][Nn][Vv][Aa][Ll][Ii][Dd][Aa][Tt][Ee];
KW_CACHE: [Cc][Aa][Cc][Hh][Ee];
KW_SYNC: [Ss][Yy][Nn][Cc];
KW_ASYNC: [Aa][Ss][Yy][Nn][Cc];
KW_MANUAL: [Mm][Aa][Nn][Uu][Aa][Ll];
KW_AUTOMATIC: [Aa][Uu][Tt][Oo][Mm][Aa][Tt][Ii][Cc];
KW_ENABLED: [Ee][Nn][Aa][Bb][Ll][Ee][Dd];
KW_DISABLED: [Dd][Ii][Ss][Aa][Bb][Ll][Ee][Dd];
KW_ROLLBACK: [Rr][Oo][Ll][Ll][Bb][Aa][Cc][Kk];
KW_SAVEPOINT: [Ss][Aa][Vv][Ee][Pp][Oo][Ii][Nn][Tt];
KW_RELEASE: [Rr][Ee][Ll][Ee][Aa][Ss][Ee];
KW_WORK: [Ww][Oo][Rr][Kk];
KW_SESSION: [Ss][Ee][Ss][Ss][Ii][Oo][Nn];
KW_LOCAL: [Ll][Oo][Cc][Aa][Ll];
KW_GLOBAL: [Gg][Ll][Oo][Bb][Aa][Ll];
KW_TRANSACTION: [Tt][Rr][Aa][Nn][Ss][Aa][Cc][Tt][Ii][Oo][Nn];
KW_ISOLATION: [Ii][Ss][Oo][Ll][Aa][Tt][Ii][Oo][Nn];
KW_READ: [Rr][Ee][Aa][Dd];
KW_WRITE: [Ww][Rr][Ii][Tt][Ee];
KW_COMMIT: [Cc][Oo][Mm][Mm][Ii][Tt];
KW_SERIALIZABLE: [Ss][Ee][Rr][Ii][Aa][Ll][Ii][Zz][Aa][Bb][Ll][Ee];
KW_REPEATABLE: [Rr][Ee][Pp][Ee][Aa][Tt][Aa][Bb][Ll][Ee];
KW_COMMITTED: [Cc][Oo][Mm][Mm][Ii][Tt][Tt][Ee][Dd];
KW_UNCOMMITTED: [Uu][Nn][Cc][Oo][Mm][Mm][Ii][Tt][Tt][Ee][Dd];
KW_SNAPSHOT: [Ss][Nn][Aa][Pp][Ss][Hh][Oo][Tt];
KW_SCHEMA: [Ss][Cc][Hh][Ee][Mm][Aa];
KW_CATALOG: [Cc][Aa][Tt][Aa][Ll][Oo][Gg];
KW_DATASOURCE: [Dd][Aa][Tt][Aa][Ss][Oo][Uu][Rr][Cc][Ee];
KW_NODE: [Nn][Oo][Dd][Ee];
KW_CLUSTER: [Cc][Ll][Uu][Ss][Tt][Ee][Rr];
KW_POOL: [Pp][Oo][Oo][Ll];
KW_RESOURCE: [Rr][Ee][Ss][Oo][Uu][Rr][Cc][Ee];
KW_PRIORITY: [Pp][Rr][Ii][Oo][Rr][Ii][Tt][Yy];
KW_WEIGHT: [Ww][Ee][Ii][Gg][Hh][Tt];
KW_SOURCE: [Ss][Oo][Uu][Rr][Cc][Ee];
KW_SINK: [Ss][Ii][Nn][Kk];
KW_CONNECTOR: [Cc][Oo][Nn][Nn][Ee][Cc][Tt][Oo][Rr];
KW_PROPERTIES: [Pp][Rr][Oo][Pp][Ee][Rr][Tt][Ii][Ee][Ss];
KW_PROPERTY: [Pp][Rr][Oo][Pp][Ee][Rr][Tt][Yy];
KW_LOCATION: [Ll][Oo][Cc][Aa][Tt][Ii][Oo][Nn];
KW_FORMAT: [Ff][Oo][Rr][Mm][Aa][Tt];
KW_RECORD: [Rr][Ee][Cc][Oo][Rr][Dd];
KW_FIELD: [Ff][Ii][Ee][Ll][Dd];
KW_COLUMN: [Cc][Oo][Ll][Uu][Mm][Nn];
// KW_SCHEMA: 已在第 112 行定义
KW_TYPE: [Tt][Yy][Pp][Ee];
KW_ENUM: [Ee][Nn][Uu][Mm];
KW_STRUCT: [Ss][Tt][Rr][Uu][Cc][Tt];
KW_MAP: [Mm][Aa][Pp];
KW_ARRAY: [Aa][Rr][Rr][Aa][Yy];
// KW_ROW: 已在第 55 行定义
KW_DECIMAL: [Dd][Ee][Cc][Ii][Mm][Aa][Ll];
KW_DOUBLE: [Dd][Oo][Uu][Bb][Ll][Ee];
KW_REAL: [Rr][Ee][Aa][Ll];
KW_FLOAT: [Ff][Ll][Oo][Aa][Tt];
KW_BOOLEAN: [Bb][Oo][Oo][Ll][Ee][Aa][Nn];
KW_TINYINT: [Tt][Ii][Nn][Yy][Ii][Nn][Tt];
KW_SMALLINT: [Ss][Mm][Aa][Ll][Ll][Ii][Nn][Tt];
KW_INTEGER: [Ii][Nn][Tt][Ee][Gg][Ee][Rr];
KW_INT: [Ii][Nn][Tt];
KW_BIGINT: [Bb][Ii][Gg][Ii][Nn][Tt];
KW_VARBINARY: [Vv][Aa][Rr][Bb][Ii][Nn][Aa][Rr][Yy];
KW_CHAR: [Cc][Hh][Aa][Rr];
KW_VARCHAR: [Vv][Aa][Rr][Cc][Hh][Aa][Rr];
KW_JSON: [Jj][Ss][Oo][Nn];
KW_IPADDRESS: [Ii][Pp][Aa][Dd][Dd][Rr][Ee][Ss][Ss];
KW_UUID: [Uu][Uu][Ii][Dd];
KW_TIME: [Tt][Ii][Mm][Ee];
KW_TIMESTAMP: [Tt][Ii][Mm][Ee][Ss][Tt][Aa][Mm][Pp];
KW_DATE: [Dd][Aa][Tt][Ee];
KW_INTERVAL: [Ii][Nn][Tt][Ee][Rr][Vv][Aa][Ll];
KW_REGEXP: [Rr][Ee][Gg][Ee][Xx][Pp];
KW_PATTERN: [Pp][Aa][Tt][Tt][Ee][Rr][Nn];
KW_SYNTAX: [Ss][Yy][Nn][Tt][Aa][Xx];
KW_ERROR: [Ee][Rr][Rr][Oo][Rr];
KW_MESSAGE: [Mm][Ee][Ss][Ss][Aa][Gg][Ee];
KW_LEVEL: [Ll][Ee][Vv][Ee][Ll];
KW_MODULE: [Mm][Oo][Dd][Uu][Ll][Ee];
KW_NAME: [Nn][Aa][Mm][Ee];
KW_VERSION: [Vv][Ee][Rr][Ss][Ii][Oo][Nn];
KW_DESCRIPTION: [Dd][Ee][Ss][Cc][Rr][Ii][Pp][Tt][Ii][Oo][Nn];
KW_CATEGORY: [Cc][Aa][Tt][Ee][Gg][Oo][Rr][Yy];
KW_TAGS: [Tt][Aa][Gg][Ss];
KW_OWNER: [Oo][Ww][Nn][Ee][Rr];
KW_CREATED_AT: 'CREATED_AT';
KW_MODIFIED_AT: 'MODIFIED_AT';
KW_STATISTICS: [Ss][Tt][Aa][Tt][Ii][Ss][Tt][Ii][Cc][Ss];
KW_METRICS: [Mm][Ee][Tt][Rr][Ii][Cc][Ss];
KW_SUMMARY: [Ss][Uu][Mm][Mm][Aa][Rr][Yy];
KW_DETAIL: [Dd][Ee][Tt][Aa][Ii][Ll];
KW_PROFILES: [Pp][Rr][Oo][Ff][Ii][Ll][Ee][Ss];
KW_DISTRIBUTION: [Dd][Ii][Ss][Tt][Rr][Ii][Bb][Uu][Tt][Ii][Oo][Nn];
KW_SPLIT: [Ss][Pp][Ll][Ii][Tt];
KW_FILE: [Ff][Ii][Ll][Ee];
KW_FILES: [Ff][Ii][Ll][Ee][Ss];
KW_PATH: [Pp][Aa][Tt][Hh];
KW_URI: [Uu][Rr][Ii];
KW_URL: [Uu][Rr][Ll];
KW_PROTOCOL: [Pp][Rr][Oo][Tt][Oo][Cc][Oo][Ll];
KW_HOST: [Hh][Oo][Ss][Tt];
KW_PORT: [Pp][Oo][Rr][Tt];
KW_DATABASE: [Dd][Aa][Tt][Aa][Bb][Aa][Ss][Ee];
KW_CATALOG_NAME: 'CATALOG_NAME';
KW_SCHEMA_NAME: 'SCHEMA_NAME';
KW_TABLE_NAME: 'TABLE_NAME';
KW_COLUMN_NAME: 'COLUMN_NAME';
KW_COLUMN_LIST: 'COLUMN_LIST';
KW_TABLE_LIST: 'TABLE_LIST';
KW_SCHEMA_LIST: 'SCHEMA_LIST';
KW_CATALOG_LIST: 'CATALOG_LIST';
KW_FUNCTION_LIST: 'FUNCTION_LIST';
KW_PROCEDURE_LIST: 'PROCEDURE_LIST';
KW_GRANT: [Gg][Rr][Aa][Nn][Tt];
KW_REVOKE: [Rr][Ee][Vv][Oo][Kk][Ee];
KW_ROLE: [Rr][Oo][Ll][Ee];
KW_USER: [Uu][Ss][Ee][Rr];
// KW_GROUP: 已在第 20 行定义
KW_PRINCIPAL: [Pp][Rr][Ii][Nn][Cc][Ii][Pp][Aa][Ll];
KW_PRIVILEGE: [Pp][Rr][Ii][Vv][Ii][Ll][Ee][Gg][Ee];
KW_PERMISSION: [Pp][Ee][Rr][Mm][Ii][Ss][Ss][Ii][Oo][Nn];
KW_ACCESS: [Aa][Cc][Cc][Ee][Ss][Ss];
KW_CONTROL: [Cc][Oo][Nn][Tt][Rr][Oo][Ll];
KW_ADMIN: [Aa][Dd][Mm][Ii][Nn];
KW_MODIFY: [Mm][Oo][Dd][Ii][Ff][Yy];
// KW_SELECT: 已在第 10 行定义
// KW_INSERT: 已在第 7 行定义
// KW_UPDATE: 已在前面定义
// KW_DELETE: 已在前面定义
// KW_DROP: 已在前面定义
// KW_QUERY: 已在前面定义
// KW_CREATE: 已在第 27 行定义
// KW_ALTER: 已在第 64 行定义
KW_USAGE: [Uu][Ss][Aa][Gg][Ee];
KW_TEMPORARY: [Tt][Ee][Mm][Pp][Oo][Rr][Aa][Rr][Yy];
KW_TEMP: [Tt][Ee][Mm][Pp];
KW_UNLOGGED: [Uu][Nn][Ll][Oo][Gg][Gg][Ee][Dd];
KW_SECURE: [Ss][Ee][Cc][Uu][Rr][Ee];
KW_INVOKER: [Ii][Nn][Vv][Oo][Kk][Ee][Rr];
KW_DEFINER: [Dd][Ee][Ff][Ii][Nn][Ee][Rr];
KW_RESTRICT: [Rr][Ee][Ss][Tt][Rr][Ii][Cc][Tt];
KW_CASCADE: [Cc][Aa][Ss][Cc][Aa][Dd][Ee];
KW_FK: [Ff][Kk];
KW_PK: [Pp][Kk];
KW_UQ: [Uu][Qq];
KW_NK: [Nn][Kk];
// KW_UNIQUE: 已在第 46 行定义
KW_CHECK: [Cc][Hh][Ee][Cc][Kk];
KW_CLUSTERED: [Cc][Ll][Uu][Ss][Tt][Ee][Rr][Ee][Dd];
KW_NONCLUSTERED: [Nn][Oo][Nn][Cc][Ll][Uu][Ss][Tt][Ee][Rr][Ee][Dd];
KW_INCLUDE: [Ii][Nn][Cc][Ll][Uu][Dd][Ee];
KW_FILLFACTOR: [Ff][Ii][Ll][Ll][Ff][Aa][Cc][Tt][Oo][Rr];
KW_PCTFREE: [Pp][Cc][Tt][Ff][Rr][Ee][Ee];
KW_PCTUSED: [Pp][Cc][Tt][Uu][Ss][Ee][Dd];
KW_THRESHOLD: [Tt][Hh][Rr][Ee][Ss][Hh][Oo][Ll][Dd];
KW_OVERRIDING: [Oo][Vv][Ee][Rr][Rr][Ii][Dd][Ii][Nn][Gg];
KW_SYSTEM: [Ss][Yy][Ss][Tt][Ee][Mm];
// KW_USER: 已在第 202 行定义
KW_OBJECT: [Oo][Bb][Jj][Ee][Cc][Tt];
KW_IDENTIFIER: [Ii][Dd][Ee][Nn][Tt][Ii][Ff][Ii][Ee][Rr];
KW_LITERAL: [Ll][Ii][Tt][Ee][Rr][Aa][Ll];
KW_PARAMETER: [Pp][Aa][Rr][Aa][Mm][Ee][Tt][Ee][Rr];
KW_EXPRESSION: [Ee][Xx][Pp][Rr][Ee][Ss][Ss][Ii][Oo][Nn];
KW_CONDITION: [Cc][Oo][Nn][Dd][Ii][Tt][Ii][Oo][Nn];
KW_STATEMENT: [Ss][Tt][Aa][Tt][Ee][Mm][Ee][Nn][Tt];
KW_BLOCK: [Bb][Ll][Oo][Cc][Kk];
KW_SEQUENCE: [Ss][Ee][Qq][Uu][Ee][Nn][Cc][Ee];
KW_TRIGGER: [Tt][Rr][Ii][Gg][Gg][Ee][Rr];
KW_EVENT: [Ee][Vv][Ee][Nn][Tt];
KW_SCHEDULER: [Ss][Cc][Hh][Ee][Dd][Uu][Ll][Ee][Rr];
KW_JOB: [Jj][Oo][Bb];
KW_TASK: [Tt][Aa][Ss][Kk];
KW_STEP: [Ss][Tt][Ee][Pp];
KW_PIPELINE: [Pp][Ii][Pp][Ee][Ll][Ii][Nn][Ee];
KW_STAGE: [Ss][Tt][Aa][Gg][Ee];
KW_PROCESSOR: [Pp][Rr][Oo][Cc][Ee][Ss][Ss][Oo][Rr];
KW_CONSUMER: [Cc][Oo][Nn][Ss][Uu][Mm][Ee][Rr];
KW_PRODUCER: [Pp][Rr][Oo][Dd][Uu][Cc][Ee][Rr];
KW_SUBSCRIBER: [Ss][Uu][Bb][Ss][Cc][Rr][Ii][Bb][Ee][Rr];
KW_PUBLISHER: [Pp][Uu][Bb][Ll][Ii][Ss][Hh][Ee][Rr];
KW_TOPIC: [Tt][Oo][Pp][Ii][Cc];
KW_CHANNEL: [Cc][Hh][Aa][Nn][Nn][Ee][Ll];
KW_QUEUE: [Qq][Uu][Ee][Uu][Ee];
KW_STREAM: [Ss][Tt][Rr][Ee][Aa][Mm];
KW_BATCH: [Bb][Aa][Tt][Cc][Hh];
KW_MICROBATCH: [Mm][Ii][Cc][Rr][Oo][Bb][Aa][Tt][Cc][Hh];
KW_WINDOW: [Ww][Ii][Nn][Dd][Oo][Ww];
KW_SLIDING: [Ss][Ll][Ii][Dd][Ii][Nn][Gg];
KW_TUMBLING: [Tt][Uu][Mm][Bb][Ll][Ii][Nn][Gg];
// KW_SESSION: 已在第 99 行定义
KW_TRIGGERED: [Tt][Rr][Ii][Gg][Gg][Ee][Rr][Ee][Dd];
KW_WATERMARK: [Ww][Aa][Tt][Ee][Rr][Mm][Aa][Rr][Kk];
KW_LATENCY: [Ll][Aa][Tt][Ee][Nn][Cc][Yy];
KW_THROUGHPUT: [Tt][Hh][Rr][Oo][Uu][Gg][Hh][Pp][Uu][Tt];
KW_BACKPRESSURE: [Bb][Aa][Cc][Kk][Pp][Rr][Ee][Ss][Ss][Uu][Rr][Ee];
KW_FLOW: [Ff][Ll][Oo][Ww];
KW_RATE: [Rr][Aa][Tt][Ee];
// KW_QUOTA: 'QUOTA';
// KW_LIMIT: 已在第 24 行定义
KW_MAX: [Mm][Aa][Xx];
KW_MIN: [Mm][Ii][Nn];
KW_AVG: [Aa][Vv][Gg];
KW_SUM: [Ss][Uu][Mm];
KW_COUNT: [Cc][Oo][Uu][Nn][Tt];
KW_FIRST: [Ff][Ii][Rr][Ss][Tt];
KW_LAST: [Ll][Aa][Ss][Tt];
KW_HEAD: [Hh][Ee][Aa][Dd];
KW_TAIL: [Tt][Aa][Ii][Ll];
KW_SAMPLE: [Ss][Aa][Mm][Pp][Ll][Ee];
KW_RANDOM: [Rr][Aa][Nn][Dd][Oo][Mm];
KW_DISTRIBUTE: [Dd][Ii][Ss][Tt][Rr][Ii][Bb][Uu][Tt][Ee];
KW_SORT: [Ss][Oo][Rr][Tt];
// KW_CLUSTER: 已在第 116 行定义
KW_SHARD: [Ss][Hh][Aa][Rr][Dd];
// KW_PARTITION: 已在第 42 行定义
KW_BUCKET: [Bb][Uu][Cc][Kk][Ee][Tt];
KW_ZONE: [Zz][Oo][Nn][Ee];
KW_REGION: [Rr][Ee][Gg][Ii][Oo][Nn];
KW_AZURE: [Aa][Zz][Uu][Rr][Ee];
KW_GCP: [Gg][Cc][Pp];
KW_ALIYUN: [Aa][Ll][Ii][Yy][Uu][Nn];
KW_TENCENT: [Tt][Ee][Nn][Cc][Ee][Nn][Tt];
// KW_HUAWEI: 已在第 308 行定义
KW_YANDEX: [Yy][Aa][Nn][Dd][Ee][Xx];
KW_AWS: [Aa][Ww][Ss];
KW_GOOGLE: [Gg][Oo][Oo][Gg][Ll][Ee];
KW_MICROSOFT: [Mm][Ii][Cc][Rr][Oo][Ss][Oo][Ff][Tt];
KW_ALIBABA: [Aa][Ll][Ii][Bb][Aa][Bb][Aa];
KW_TENCCENT: [Tt][Ee][Nn][Cc][Ee][Nn][Tt];
KW_HUAWEI: [Hh][Uu][Aa][Ww][Ee][Ii];

// ============================================
// 数据类型关键字
// ============================================

// KW_INT: 已在第 146 行定义
// KW_BIGINT: 已在第 147 行定义
// KW_SMALLINT: 已在第 144 行定义
// KW_TINYINT: 已在第 143 行定义
// KW_BOOLEAN: 已在第 142 行定义
// KW_DATE: 已在第 156 行定义
// KW_TIME: 已在前面定义
// KW_TIMESTAMP: 已在前面定义
// KW_BINARY: 已在前面定义
// KW_VARBINARY: 已在前面定义
// KW_ARRAY: 已在前面定义
// KW_MAP: 已在前面定义
// KW_ROW: 已在前面定义
KW_ANY: [Aa][Nn][Yy];
// KW_JSON: 已在第 151 行定义
// KW_IPADDRESS: 已在第 152 行定义
// KW_UUID: 已在前面定义

// ============================================
// TVF (Table-valued Functions) - Presto 特有
// ============================================

KW_EXPLODE: [Ee][Xx][Pp][Ll][Oo][Dd][Ee];
KW_POSEXPLODE: [Pp][Oo][Ss][Ee][Xx][Pp][Ll][Oo][Dd][Ee];
KW_GENERATE_SUBSCRIPTS: 'GENERATE_SUBSCRIPTS';
KW_UNNEST: [Uu][Nn][Nn][Ee][Ss][Tt];
KW_CARTESIAN_PRODUCT: 'CARTESIAN_PRODUCT';
KW_SIDWAYS: [Ss][Ii][Dd][Ww][Aa][Yy][Ss];
KW_LATERAL: [Ll][Aa][Tt][Ee][Rr][Aa][Ll];
KW_FLATTEN: [Ff][Ll][Aa][Tt][Tt][Ee][Nn];
KW_TRANSFORM: [Tt][Rr][Aa][Nn][Ss][Ff][Oo][Rr][Mm];
KW_APPLY: [Aa][Pp][Pp][Ll][Yy];
KW_CALL: [Cc][Aa][Ll][Ll];
KW_RUN: [Rr][Uu][Nn];
KW_EXEC: [Ee][Xx][Ee][Cc];
KW_DO: [Dd][Oo];
KW_CASE: [Cc][Aa][Ss][Ee];
KW_WHEN: [Ww][Hh][Ee][Nn];
KW_THEN: [Tt][Hh][Ee][Nn];
KW_ELSE: [Ee][Ll][Ss][Ee];
KW_END: [Ee][Nn][Dd];
KW_START: [Ss][Tt][Aa][Rr][Tt];
KW_STOP: [Ss][Tt][Oo][Pp];
KW_PAUSE: [Pp][Aa][Uu][Ss][Ee];
KW_RESUME: [Rr][Ee][Ss][Uu][Mm][Ee];
KW_SUSPEND: [Ss][Uu][Ss][Pp][Ee][Nn][Dd];
KW_ACTIVE: [Aa][Cc][Tt][Ii][Vv][Ee];
KW_INACTIVE: [Ii][Nn][Aa][Cc][Tt][Ii][Vv][Ee];
KW_FAILED: [Ff][Aa][Ii][Ll][Ee][Dd];
KW_SUCCESS: [Ss][Uu][Cc][Cc][Ee][Ss][Ss];
KW_PENDING: [Pp][Ee][Nn][Dd][Ii][Nn][Gg];
KW_RUNNING: [Rr][Uu][Nn][Nn][Ii][Nn][Gg];
KW_COMPLETED: [Cc][Oo][Mm][Pp][Ll][Ee][Tt][Ee][Dd];
KW_ABORTED: [Aa][Bb][Oo][Rr][Tt][Ee][Dd];
KW_CANCELLED: [Cc][Aa][Nn][Cc][Ee][Ll][Ll][Ee][Dd];
KW_SKIPPED: [Ss][Kk][Ii][Pp][Pp][Ee][Dd];
KW_RETRY: [Rr][Ee][Tt][Rr][Yy];
KW_RECOVER: [Rr][Ee][Cc][Oo][Vv][Ee][Rr];
KW_FAIL: [Ff][Aa][Ii][Ll];
KW_SUCCEED: [Ss][Uu][Cc][Cc][Ee][Ee][Dd];
KW_OK: [Oo][Kk];
KW_ERR: [Ee][Rr][Rr];
KW_WARN: [Ww][Aa][Rr][Nn];
KW_INFO: [Ii][Nn][Ff][Oo];
KW_DEBUG: [Dd][Ee][Bb][Uu][Gg];
KW_TRACE: [Tt][Rr][Aa][Cc][Ee];
KW_LOG: [Ll][Oo][Gg];
KW_AUDIT: [Aa][Uu][Dd][Ii][Tt];
KW_SECURITY: [Ss][Ee][Cc][Uu][Rr][Ii][Tt][Yy];
KW_COMPLIANCE: [Cc][Oo][Mm][Pp][Ll][Ii][Aa][Nn][Cc][Ee];
KW_GOVERNANCE: [Gg][Oo][Vv][Ee][Rr][Nn][Aa][Nn][Cc][Ee];
KW_QUALITY: [Qq][Uu][Aa][Ll][Ii][Tt][Yy];
KW_PERFORMANCE: [Pp][Ee][Rr][Ff][Oo][Rr][Mm][Aa][Nn][Cc][Ee];
KW_MONITORING: [Mm][Oo][Nn][Ii][Tt][Oo][Rr][Ii][Nn][Gg];
KW_ALERTING: [Aa][Ll][Ee][Rr][Tt][Ii][Nn][Gg];
KW_NOTIFICATION: [Nn][Oo][Tt][Ii][Ff][Ii][Cc][Aa][Tt][Ii][Oo][Nn];
KW_SUBSCRIPTION: [Ss][Uu][Bb][Ss][Cc][Rr][Ii][Pp][Tt][Ii][Oo][Nn];
KW_FEEDBACK: [Ff][Ee][Ee][Dd][Bb][Aa][Cc][Kk];
KW_REVIEW: [Rr][Ee][Vv][Ii][Ee][Ww];
KW_APPROVAL: [Aa][Pp][Pp][Rr][Oo][Vv][Aa][Ll];
KW_AUTHORIZATION: [Aa][Uu][Tt][Hh][Oo][Rr][Ii][Zz][Aa][Tt][Ii][Oo][Nn];
KW_AUTHENTICATION: [Aa][Uu][Tt][Hh][Ee][Nn][Tt][Ii][Cc][Aa][Tt][Ii][Oo][Nn];
KW_ENCRYPTION: [Ee][Nn][Cc][Rr][Yy][Pp][Tt][Ii][Oo][Nn];
KW_DECRYPTION: [Dd][Ee][Cc][Rr][Yy][Pp][Tt][Ii][Oo][Nn];
KW_SIGNING: [Ss][Ii][Gg][Nn][Ii][Nn][Gg];
KW_VERIFICATION: [Vv][Ee][Rr][Ii][Ff][Ii][Cc][Aa][Tt][Ii][Oo][Nn];
KW_VALIDATION: [Vv][Aa][Ll][Ii][Dd][Aa][Tt][Ii][Oo][Nn];
KW_CERTIFICATION: [Cc][Ee][Rr][Tt][Ii][Ff][Ii][Cc][Aa][Tt][Ii][Oo][Nn];
KW_ACCREDITATION: [Aa][Cc][Cc][Rr][Ee][Dd][Ii][Tt][Aa][Tt][Ii][Oo][Nn];
KW_LICENSE: [Ll][Ii][Cc][Ee][Nn][Ss][Ee];
KW_PERMIT: [Pp][Ee][Rr][Mm][Ii][Tt];
KW_CERTIFICATE: [Cc][Ee][Rr][Tt][Ii][Ff][Ii][Cc][Aa][Tt][Ee];
// KW_KEY: 已在第 44 行定义
KW_SECRET: [Ss][Ee][Cc][Rr][Ee][Tt];
KW_TOKEN: [Tt][Oo][Kk][Ee][Nn];
KW_CREDENTIAL: [Cc][Rr][Ee][Dd][Ee][Nn][Tt][Ii][Aa][Ll];
KW_PASSWORD: [Pp][Aa][Ss][Ss][Ww][Oo][Rr][Dd];
KW_PIN: [Pp][Ii][Nn];
KW_CODE: [Cc][Oo][Dd][Ee];
KW_CAPTCHA: [Cc][Aa][Pp][Tt][Cc][Hh][Aa];
KW_MFA: [Mm][Ff][Aa];
KW_2FA: '2FA';
KW_OTP: [Oo][Tt][Pp];
KW_SSO: [Ss][Ss][Oo];
KW_OIDC: [Oo][Ii][Dd][Cc];
KW_SAML: [Ss][Aa][Mm][Ll];
KW_LDAP: [Ll][Dd][Aa][Pp];
KW_AD: [Aa][Dd];
KW_NIS: [Nn][Ii][Ss];
KW_KERBEROS: [Kk][Ee][Rr][Bb][Ee][Rr][Oo][Ss];
// KW_RADIUS: 已在第 431 行定义
KW_TACACS: [Tt][Aa][Cc][Aa][Cc][Ss];

// ============================================
// 标识符
// ============================================

// missing keywords referenced by parser grammar
DOUBLE_QUOTED_ID: '"' (~['"])+ '"';
DOUBLE_QUOTED_STRING: '"' (~['"])+ '"';
KW_ADD: [Aa][Dd][Dd];
KW_ASC: [Aa][Ss][Cc];
KW_BINARY: [Bb][Ii][Nn][Aa][Rr][Yy];
KW_COST: [Cc][Oo][Ss][Tt];
KW_CROSS: [Cc][Rr][Oo][Ss][Ss];
KW_CTAS: [Cc][Tt][Aa][Ss];
KW_DELETE: [Dd][Ee][Ll][Ee][Tt][Ee];
KW_DESC: [Dd][Ee][Ss][Cc];
KW_EXISTS: [Ee][Xx][Ii][Ss][Tt][Ss];
KW_IF: [Ii][Ff];
KW_NATURAL: [Nn][Aa][Tt][Uu][Rr][Aa][Ll];
KW_OVER: [Oo][Vv][Ee][Rr];
KW_QUERY: [Qq][Uu][Ee][Rr][Yy];
KW_RENAME: [Rr][Ee][Nn][Aa][Mm][Ee];
KW_STRING: [Ss][Tt][Rr][Ii][Nn][Gg];
KW_TO: [Tt][Oo];
KW_UPDATE: [Uu][Pp][Dd][Aa][Tt][Ee];
UID: [a-zA-Z_][a-zA-Z0-9_]*;
QUOTED_UID: '`' (~[`])+ '`';

// ============================================
// 字符串和数字
// ============================================

STRING: '\'' (~'\'')+ '\'';

NUMBER: [0-9]+;
DECIMAL_NUMBER: [0-9]+'.'[0-9]+;
EXP_NUMBER: [0-9]+[eE][+-]?[0-9]+;
HEX_NUMBER: '0[xX]' [0-9a-fA-F]+;
BIN_NUMBER: '0[bB]' [01]+;

// ============================================
// 运算符
// ============================================

PLUS: '+';
MINUS: '-';
MULT: '*';
DIV: '/';
MOD: '%';
EQ: '=';
NEQ: '<>' | '!=';
LT: '<';
GT: '>';
LTE: '<=';
GTE: '>=';
CONCAT: '||';
ARROW: '->';
LONG_ARROW: '=>';
COLON_ASSIGN: ':=';

// ============================================
// 标点符号
// ============================================

LPAREN: '(';
RPAREN: ')';
LBRACKET: '[';
RBRACKET: ']';
LBRACE: '{';
RBRACE: '}';
COMMA: ',';
COLON: ':';
DOT: '.';
SEMICOLON: ';';
AT: '@';
EXCLAMATION: '!';
QUESTION: '?';
STAR: '*';
AMPERSAND: '&';
BAR: '|';
CARET: '^';
TILDE: '~';
SLASH: '/';
BACKSLASH: '\\';
BACKTICK: '`';

// ============================================
// 空白字符（跳过）
// ============================================

WS: [ \t\r\n]+ -> skip;

// ============================================
// 注释
// ============================================

LINE_COMMENT: '--' ~[\r\n]* -> skip;
BLOCK_COMMENT: '/*' .+? '*/' -> skip;
DOC_COMMENT: '/**' .+? '*/' -> skip;
