lexer grammar SparkSqlLexer;

// ============================================
// 关键字定义（继承 Flink 基础）
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
// Spark 特有关键字
// ============================================

KW_READ: [Rr][Ee][Aa][Dd];
KW_FORMAT: [Ff][Oo][Rr][Mm][Aa][Tt];
KW_OPTIONS: [Oo][Pp][Tt][Ii][Oo][Nn][Ss];
KW_OPTION: [Oo][Pp][Tt][Ii][Oo][Nn];
KW_CSV: [Cc][Ss][Vv];
KW_JSON: [Jj][Ss][Oo][Nn];
KW_PARQUET: [Pp][Aa][Rr][Qq][Uu][Ee][Tt];
KW_JDBC: [Jj][Dd][Bb][Cc];
KW_BIGQUERY: [Bb][Ii][Gg][Qq][Uu][Ee][Rr][Yy];
KW_COSMOS: [Cc][Oo][Ss][Mm][Oo][Ss];
KW_DYNAMODB: [Dd][Yy][Nn][Aa][Mm][Oo][Dd][Bb];
KW_ELASTICSEARCH: [Ee][Ll][Aa][Ss][Tt][Ii][Cc][Ss][Ee][Aa][Rr][Cc][Hh];
KW_MONGODB: [Mm][Oo][Nn][Gg][Oo][Dd][Bb];
KW_RDBMS: [Rr][Dd][Bb][Mm][Ss];
KW_SQLSERVER: [Ss][Qq][Ll][Ss][Ee][Rr][Vv][Ee][Rr];
KW_SAVEMODE: [Ss][Aa][Vv][Ee][Mm][Oo][Dd][Ee];
KW_APPEND: [Aa][Pp][Pp][Ee][Nn][Dd];
// KW_OVERWRITE: 已在第 9 行定义
KW_IGNORE: [Ii][Gg][Nn][Oo][Rr][Ee];
KW_ERRORIFNOTEXISTS: [Ee][Rr][Rr][Oo][Rr][Ii][Ff][Nn][Oo][Tt][Ee][Xx][Ii][Ss][Tt][Ss];
KW_TRUNCATE: [Tt][Rr][Uu][Nn][Cc][Aa][Tt][Ee];
KW_UPDATE: [Uu][Pp][Dd][Aa][Tt][Ee];
KW_DELETE: [Dd][Ee][Ll][Ee][Tt][Ee];
KW_MERGE: [Mm][Ee][Rr][Gg][Ee];
KW_USING: [Uu][Ss][Ii][Nn][Gg];
KW_STREAM: [Ss][Tt][Rr][Ee][Aa][Mm];
KW_TABLES: [Tt][Aa][Bb][Ll][Ee][Ss];
KW_TEMPORARY: [Tt][Ee][Mm][Pp][Oo][Rr][Aa][Rr][Yy];
KW_GLOBAL: [Gg][Ll][Oo][Bb][Aa][Ll];
KW_LOCAL: [Ll][Oo][Cc][Aa][Ll];
KW_SESSION: [Ss][Ee][Ss][Ss][Ii][Oo][Nn];
KW_APPLICATION: [Aa][Pp][Pp][Ll][Ii][Cc][Aa][Tt][Ii][Oo][Nn];
KW_TRANSACTION: [Tt][Rr][Aa][Nn][Ss][Aa][Cc][Tt][Ii][Oo][Nn];
KW_BEGIN: [Bb][Ee][Gg][Ii][Nn];
KW_COMMIT: [Cc][Oo][Mm][Mm][Ii][Tt];
KW_ROLLBACK: [Rr][Oo][Ll][Ll][Bb][Aa][Cc][Kk];
KW_ANALYZE: [Aa][Nn][Aa][Ll][Yy][Zz][Ee];
KW_COLLECT: [Cc][Oo][Ll][Ll][Ee][Cc][Tt];
KW_STATS: [Ss][Tt][Aa][Tt][Ss];
KW_DESCRIBE: [Dd][Ee][Ss][Cc][Rr][Ii][Bb][Ee];
KW_EXTENDED: [Ee][Xx][Tt][Ee][Nn][Dd][Ee][Dd];
KW_FORMATTED: [Ff][Oo][Rr][Mm][Aa][Tt][Tt][Ee][Dd];
KW_REFRESH: [Rr][Ee][Ff][Rr][Ee][Ss][Hh];
KW_INVALIDATE: [Ii][Nn][Vv][Aa][Ll][Ii][Dd][Aa][Tt][Ee];
KW_CACHE: [Cc][Aa][Cc][Hh][Ee];
KW_UNCACHE: [Uu][Nn][Cc][Aa][Cc][Hh][Ee];
KW_LIST: [Ll][Ii][Ss][Tt];
KW_FUNCTIONS: [Ff][Uu][Nn][Cc][Tt][Ii][Oo][Nn][Ss];
KW_DATABASES: [Dd][Aa][Tt][Aa][Bb][Aa][Ss][Ee][Ss];
KW_SCHEMAS: [Ss][Cc][Hh][Ee][Mm][Aa][Ss];
KW_CATALOGS: [Cc][Aa][Tt][Aa][Ll][Oo][Gg][Ss];
KW_PROCEDURE: [Pp][Rr][Oo][Cc][Ee][Dd][Uu][Rr][Ee];
KW_CALL: [Cc][Aa][Ll][Ll];
KW_CANCEL: [Cc][Aa][Nn][Cc][Ee][Ll];
KW_KILL: [Kk][Ii][Ll][Ll];
KW_SHOW: [Ss][Hh][Oo][Ww];
KW_USE: [Uu][Ss][Ee];
KW_UNNEST: [Uu][Nn][Nn][Ee][Ss][Tt];
KW_CASE: [Cc][Aa][Ss][Ee];
KW_WHEN: [Ww][Hh][Ee][Nn];
KW_THEN: [Tt][Hh][Ee][Nn];
KW_ELSE: [Ee][Ll][Ss][Ee];
KW_END: [Ee][Nn][Dd];
KW_SET: [Ss][Ee][Tt];
KW_UNSET: [Uu][Nn][Ss][Ee][Tt];
KW_CONF: [Cc][Oo][Nn][Ff];
KW_ADD: [Aa][Dd][Dd];
KW_REMOVE: [Rr][Ee][Mm][Oo][Vv][Ee];
KW_FILE: [Ff][Ii][Ll][Ee];
KW_DIRS: [Dd][Ii][Rr][Ss];
KW_DATA: [Dd][Aa][Tt][Aa];
KW_LOG: [Ll][Oo][Gg];
KW_MANIFEST: [Mm][Aa][Nn][Ii][Ff][Ee][Ss][Tt];
KW_CHECKPOINT: [Cc][Hh][Ee][Cc][Kk][Pp][Oo][Ii][Nn][Tt];
KW_RECOVER: [Rr][Ee][Cc][Oo][Vv][Ee][Rr];
KW_BACKUP: [Bb][Aa][Cc][Kk][Uu][Pp];
KW_REGISTER: [Rr][Ee][Gg][Ii][Ss][Tt][Ee][Rr];
KW_UNREGISTER: [Uu][Nn][Rr][Ee][Gg][Ii][Ss][Tt][Ee][Rr];
KW_UDF: [Uu][Dd][Ff];
KW_UADF: [Uu][Aa][Dd][Ff];
KW_AGGREGATE: [Aa][Gg][Gg][Rr][Ee][Gg][Aa][Tt][Ee];
KW_COUNT: [Cc][Oo][Uu][Nn][Tt];
KW_MIN: [Mm][Ii][Nn];
KW_MAX: [Mm][Aa][Xx];
KW_SUM: [Ss][Uu][Mm];
KW_AVG: [Aa][Vv][Gg];
KW_FIRST: [Ff][Ii][Rr][Ss][Tt];
KW_LAST: [Ll][Aa][Ss][Tt];
KW_SKEW: [Ss][Kk][Ee][Ww];
KW_BUCKET: [Bb][Uu][Cc][Kk][Ee][Tt];
KW_TOWINDOW: [Tt][Oo][Ww][Ii][Nn][Dd][Oo][Ww];
KW_WINDOW: [Ww][Ii][Nn][Dd][Oo][Ww];
KW_TIMESTAMPTZ: [Tt][Ii][Mm][Ee][Ss][Tt][Aa][Mm][Pp][Tt][Zz];
KW_NTZ: [Nn][Tt][Zz];
KW_ZORDER: [Zz][Oo][Rr][Dd][Ee][Rr];
KW_BUCKETED: [Bb][Uu][Cc][Kk][Ee][Tt][Ee][Dd];
KW_CLUSTERED: [Cc][Ll][Uu][Ss][Tt][Ee][Rr][Ee][Dd];
KW_SORTED: [Ss][Oo][Rr][Tt][Ee][Dd];
KW_SERDE: [Ss][Ee][Rr][Dd][Ee];
KW_PROPERTIES: [Pp][Rr][Oo][Pp][Ee][Rr][Tt][Ii][Ee][Ss];
KW_LOCATION: [Ll][Oo][Cc][Aa][Tt][Ii][Oo][Nn];
KW_INPUTFORMAT: [Ii][Nn][Pp][Uu][Tt][Ff][Oo][Rr][Mm][Aa][Tt];
KW_OUTPUTFORMAT: [Oo][Uu][Tt][Pp][Uu][Tt][Ff][Oo][Rr][Mm][Aa][Tt];
KW_SERDEPROPERTIES: [Ss][Ee][Rr][Dd][Ee][Pp][Rr][Oo][Pp][Ee][Rr][Tt][Ii][Ee][Ss];
KW_TBLPROPERTIES: [Tt][Bb][Ll][Pp][Rr][Oo][Pp][Ee][Rr][Tt][Ii][Ee][Ss];
KW_STORED: [Ss][Tt][Oo][Rr][Ee][Dd];
KW_INTERNAL: [Ii][Nn][Tt][Ee][Rr][Nn][Aa][Ll];
KW_MANAGED: [Mm][Aa][Nn][Aa][Gg][Ee][Dd];
KW_EXTERNAL: [Ee][Xx][Tt][Ee][Rr][Nn][Aa][Ll];
KW_IDENTITY: [Ii][Dd][Ee][Nn][Tt][Ii][Tt][Yy];
KW_GENERATED: [Gg][Ee][Nn][Ee][Rr][Aa][Tt][Ee][Dd];
KW_ALWAYS: [Aa][Ll][Ww][Aa][Yy][Ss];
KW_DETERMINISTIC: [Dd][Ee][Tt][Ee][Rr][Mm][Ii][Nn][Ii][Ss][Tt][Ii][Cc];
KW_EXECUTION: [Ee][Xx][Ee][Cc][Uu][Tt][Ii][Oo][Nn];
KW_PROCESSED: [Pp][Rr][Oo][Cc][Ee][Ss][Ss][Ee][Dd];
KW_PIVOT: [Pp][Ii][Vv][Oo][Tt];
KW_UNPIVOT: [Uu][Nn][Pp][Ii][Vv][Oo][Tt];
KW_SAMPLE: [Ss][Aa][Mm][Pp][Ll][Ee];
KW_CLUSTER: [Cc][Ll][Uu][Ss][Tt][Ee][Rr];
KW_DISTRIBUTE: [Dd][Ii][Ss][Tt][Rr][Ii][Bb][Uu][Tt][Ee];
KW_SORT: [Ss][Oo][Rr][Tt];
KW_LATERAL: [Ll][Aa][Tt][Ee][Rr][Aa][Ll];
KW_SEMI: [Ss][Ee][Mm][Ii];
KW_ANTI: [Aa][Nn][Tt][Ii];
KW_BROADCAST: [Bb][Rr][Oo][Aa][Dd][Cc][Aa][Ss][Tt];
KW_HASH: [Hh][Aa][Ss][Hh];
KW_NATURAL: [Nn][Aa][Tt][Uu][Rr][Aa][Ll];
KW_CROSS: [Cc][Rr][Oo][Ss][Ss];
KW_QUALIFY: [Qq][Uu][Aa][Ll][Ii][Ff][Yy];
KW_RANKING: [Rr][Aa][Nn][Kk][Ii][Nn][Gg];
KW_RETURNS: [Rr][Ee][Tt][Uu][Rr][Nn][Ss];
KW_LANGUAGE: [Ll][Aa][Nn][Gg][Uu][Aa][Gg][Ee];
// KW_DETERMINISTIC: 已在第 176 行定义
// KW_NOT: 已在第 38 行定义
KW_EXPLAIN: [Ee][Xx][Pp][Ll][Aa][Ii][Nn];
KW_COST: [Cc][Oo][Ss][Tt];
KW_VERBOSE: [Vv][Ee][Rr][Bb][Oo][Ss][Ee];
KW_PREDICT: [Pp][Rr][Ee][Dd][Ii][Cc][Tt];
KW_TRAIN: [Tt][Rr][Aa][Ii][Nn];
KW_MODEL: [Mm][Oo][Dd][Ee][Ll];
KW_PREDICTION: [Pp][Rr][Ee][Dd][Ii][Cc][Tt][Ii][Oo][Nn];
KW_FEATURES: [Ff][Ee][Aa][Tt][Uu][Rr][Ee][Ss];
KW_LABELS: [Ll][Aa][Bb][Ee][Ll][Ss];
KW_METRICS: [Mm][Ee][Tt][Rr][Ii][Cc][Ss];
KW_PARAMETERS: [Pp][Aa][Rr][Aa][Mm][Ee][Tt][Ee][Rr][Ss];
KW_SETTINGS: [Ss][Ee][Tt][Tt][Ii][Nn][Gg][Ss];
KW_CONFIG: [Cc][Oo][Nn][Ff][Ii][Gg];
KW_REGISTRY: [Rr][Ee][Gg][Ii][Ss][Tt][Rr][Yy];
KW_ARTIFACT: [Aa][Rr][Tt][Ii][Ff][Aa][Cc][Tt];
KW_VERSION: [Vv][Ee][Rr][Ss][Ii][Oo][Nn];
KW_DESCRIPTION: [Dd][Ee][Ss][Cc][Rr][Ii][Pp][Tt][Ii][Oo][Nn];
KW_CATEGORY: [Cc][Aa][Tt][Ee][Gg][Oo][Rr][Yy];
KW_TAGS: [Tt][Aa][Gg][Ss];
KW_OWNER: [Oo][Ww][Nn][Ee][Rr];
KW_CREATED_AT: 'CREATED_AT';
KW_MODIFIED_AT: 'MODIFIED_AT';
KW_TYPE: [Tt][Yy][Pp][Ee];
KW_ENUM: [Ee][Nn][Uu][Mm];
KW_STRUCT: [Ss][Tt][Rr][Uu][Cc][Tt];
KW_VARIANT: [Vv][Aa][Rr][Ii][Aa][Nn][Tt];
KW_BYTEARRAY: [Bb][Yy][Tt][Ee][Aa][Rr][Rr][Aa][Yy];
KW_DYNAMIC: [Dd][Yy][Nn][Aa][Mm][Ii][Cc];
KW_PRIMITIVE: [Pp][Rr][Ii][Mm][Ii][Tt][Ii][Vv][Ee];
KW_BINARY: [Bb][Ii][Nn][Aa][Rr][Yy];
KW_VARBINARY: [Vv][Aa][Rr][Bb][Ii][Nn][Aa][Rr][Yy];
KW_DECIMAL: [Dd][Ee][Cc][Ii][Mm][Aa][Ll];
KW_NUMERIC: [Nn][Uu][Mm][Ee][Rr][Ii][Cc];
KW_CHAR: [Cc][Hh][Aa][Rr];
KW_NCHAR: [Nn][Cc][Hh][Aa][Rr];
KW_VARCHAR: [Vv][Aa][Rr][Cc][Hh][Aa][Rr];
KW_NVARCHAR: [Nn][Vv][Aa][Rr][Cc][Hh][Aa][Rr];
KW_TEXT: [Tt][Ee][Xx][Tt];
KW_BLOB: [Bb][Ll][Oo][Bb];
KW_CLOB: [Cc][Ll][Oo][Bb];
KW_INTERVAL: [Ii][Nn][Tt][Ee][Rr][Vv][Aa][Ll];
KW_YEAR: [Yy][Ee][Aa][Rr];
KW_MONTH: [Mm][Oo][Nn][Tt][Hh];
KW_DAY: [Dd][Aa][Yy];
KW_HOUR: [Hh][Oo][Uu][Rr];
KW_MINUTE: [Mm][Ii][Nn][Uu][Tt][Ee];
KW_SECOND: [Ss][Ee][Cc][Oo][Nn][Dd];
KW_TIMEZONE: [Tt][Ii][Mm][Ee][Zz][Oo][Nn][Ee];
KW_REGION: [Rr][Ee][Gg][Ii][Oo][Nn];
KW_COUNTRY: [Cc][Oo][Uu][Nn][Tt][Rr][Yy];
KW_GEOMETRY: [Gg][Ee][Oo][Mm][Ee][Tt][Rr][Yy];
KW_GEOGRAPHY: [Gg][Ee][Oo][Gg][Rr][Aa][Pp][Hh][Yy];
KW_POINT: [Pp][Oo][Ii][Nn][Tt];
KW_LINESTRING: [Ll][Ii][Nn][Ee][Ss][Tt][Rr][Ii][Nn][Gg];
KW_POLYGON: [Pp][Oo][Ll][Yy][Gg][Oo][Nn];
KW_MULTIPOINT: [Mm][Uu][Ll][Tt][Ii][Pp][Oo][Ii][Nn][Tt];
KW_MULTILINESTRING: [Mm][Uu][Ll][Tt][Ii][Ll][Ii][Nn][Ee][Ss][Tt][Rr][Ii][Nn][Gg];
KW_MULTIPOLYGON: [Mm][Uu][Ll][Tt][Ii][Pp][Oo][Ll][Yy][Gg][Oo][Nn];
KW_GEOMETRYCOLLECTION: [Gg][Ee][Oo][Mm][Ee][Tt][Rr][Yy][Cc][Oo][Ll][Ll][Ee][Cc][Tt][Ii][Oo][Nn];

// ============================================
// 数据类型关键字
// ============================================

KW_INT: [Ii][Nn][Tt] | [Ii][Nn][Tt][Ee][Gg][Ee][Rr];
KW_BIGINT: [Bb][Ii][Gg][Ii][Nn][Tt];
KW_SMALLINT: [Ss][Mm][Aa][Ll][Ll][Ii][Nn][Tt];
KW_TINYINT: [Tt][Ii][Nn][Yy][Ii][Nn][Tt];
// KW_DECIMAL: 已在第 229 行定义
// KW_STRING: 已在第 230 行定义
// KW_CHAR: 已在第 231 行定义
KW_BOOLEAN: [Bb][Oo][Oo][Ll][Ee][Aa][Nn] | [Bb][Oo][Oo][Ll];
KW_DATE: [Dd][Aa][Tt][Ee];
KW_TIME: [Tt][Ii][Mm][Ee];
KW_TIMESTAMP: [Tt][Ii][Mm][Ee][Ss][Tt][Aa][Mm][Pp];
KW_ARRAY: [Aa][Rr][Rr][Aa][Yy];
KW_MAP: [Mm][Aa][Pp];
// KW_ROW: 已在第 55 行定义
KW_ANY: [Aa][Nn][Yy];

// ============================================
// TVF (Table-valued Functions) - Spark 特有
// ============================================

KW_EXPLODE: [Ee][Xx][Pp][Ll][Oo][Dd][Ee];
KW_INLINE: [Ii][Nn][Ll][Ii][Nn][Ee];
KW_POUD: [Pp][Oo][Uu][Dd];
KW_STACK: [Ss][Tt][Aa][Cc][Kk];
KW_LATERAL_VIEW: [Ll][Aa][Tt][Ee][Rr][Aa][Ll] [Vv][Ii][Ee][Ww];
KW_POSPOSE: [Pp][Oo][Ss][Ee][Xx][Pp][Ll][Oo][Dd][Ee];
KW_TRY_EVALUATE: 'TRY_EVALUATE';
KW_JSON_TUPLE: 'JSON_TUPLE';
KW_XPATH: [Xx][Pp][Aa][Tt][Hh];
KW_XPATH_STRING: 'XPATH_STRING';
KW_ASYMMETRIC: [Aa][Ss][Yy][Mm][Mm][Ee][Tt][Rr][Ii][Cc];
KW_ONLY: [Oo][Nn][Ll][Yy];
KW_FREE: [Ff][Rr][Ee][Ee];
KW_FILL: [Ff][Ii][Ll][Ll];
// KW_RESPECT: 已在前面定义
// KW_IGNORE: 已在第 88 行定义
KW_NULLS: [Nn][Uu][Ll][Ll][Ss];
KW_FIRST_VALUE: 'FIRST_VALUE';
KW_LAST_VALUE: 'LAST_VALUE';
KW_CUME_DIST: 'CUME_DIST';
KW_PERCENTILE: [Pp][Ee][Rr][Cc][Ee][Nn][Tt][Ii][Ll][Ee];
KW_PERCENTILE_CONT: 'PERCENTILE_CONT';
KW_PERCENTILE_DISC: 'PERCENTILE_DISC';
KW_MODE: [Mm][Oo][Dd][Ee];
KW_STDDEV: [Ss][Tt][Dd][Dd][Ee][Vv];
KW_STDDEV_POP: 'STDDEV_POP';
KW_STDDEV_SAMP: 'STDDEV_SAMP';
KW_VARIANCE: [Vv][Aa][Rr][Ii][Aa][Nn][Cc][Ee];
KW_VARIANCE_POP: 'VARIANCE_POP';
KW_VARIANCE_SAMP: 'VARIANCE_SAMP';
KW_CORRELATION: [Cc][Oo][Rr][Rr][Ee][Ll][Aa][Tt][Ii][Oo][Nn];
KW_COVARIANCE: [Cc][Oo][Vv][Aa][Rr][Ii][Aa][Nn][Cc][Ee];
KW_SKEWNESS: [Ss][Kk][Ee][Ww][Nn][Ee][Ss][Ss];
KW_KURTOSIS: [Kk][Uu][Rr][Tt][Oo][Ss][Ii][Ss];
KW_APPROX_COUNT_DISTINCT: 'APPROX_COUNT_DISTINCT';
KW_APPROX_PERCENTILE: 'APPROX_PERCENTILE';
KW_HISTOGRAM: [Hh][Ii][Ss][Tt][Oo][Gg][Rr][Aa][Mm];
KW_LINEAR_REGRESSION: 'LINEAR_REGRESSION';
KW_LOGISTIC_REGRESSION: 'LOGISTIC_REGRESSION';
KW_CLUSTERING: [Cc][Ll][Uu][Ss][Tt][Ee][Rr][Ii][Nn][Gg];
KW_ASSOCIATION_RULES: 'ASSOCIATION_RULES';
KW_FREQUENCY_ITEMSET: 'FREQUENCY_ITEMSET';
KW_TOPN: [Tt][Oo][Pp][Nn];
KW_SIMILARITY: [Ss][Ii][Mm][Ii][Ll][Aa][Rr][Ii][Tt][Yy];
KW_DISTANCE: [Dd][Ii][Ss][Tt][Aa][Nn][Cc][Ee];
KW_SIMILAR: [Ss][Ii][Mm][Ii][Ll][Aa][Rr];
KW_CONTAINS: [Cc][Oo][Nn][Tt][Aa][Ii][Nn][Ss];
KW_STARTSWITH: [Ss][Tt][Aa][Rr][Tt][Ss][Ww][Ii][Tt][Hh];
KW_ENDSWITH: [Ee][Nn][Dd][Ss][Ww][Ii][Tt][Hh];
KW_REPLACE: [Rr][Ee][Pp][Ll][Aa][Cc][Ee];
KW_SUBSTR: [Ss][Uu][Bb][Ss][Tt][Rr];
KW_SUBSTRING: [Ss][Uu][Bb][Ss][Tt][Rr][Ii][Nn][Gg];
KW_STRPOS: [Ss][Tt][Rr][Pp][Oo][Ss];
KW_LEVENSHTEIN: [Ll][Ee][Vv][Ee][Nn][Ss][Hh][Tt][Ee][Ii][Nn];
KW_MD5: 'MD5';
KW_SHA1: 'SHA1';
KW_SHA2: 'SHA2';
KW_UNBASE64: 'UNBASE64';
KW_BASE64: 'BASE64';
KW_TO_UNIX_TIMESTAMP: 'TO_UNIX_TIMESTAMP';
KW_FROM_UNIXTIME: 'FROM_UNIXTIME';
KW_UNIX_TIMESTAMP: 'UNIX_TIMESTAMP';
KW_DATE_ADD: 'DATE_ADD';
KW_DATE_SUB: 'DATE_SUB';
KW_NEXT_DAY: 'NEXT_DAY';
KW_LAST_DAY: 'LAST_DAY';
KW_MONTHS_BETWEEN: 'MONTHS_BETWEEN';
KW_DATE_DIFF: 'DATE_DIFF';
// KW_TRUNCATE: 已在第 90 行定义
KW_TO_DATE: 'TO_DATE';
KW_TO_TIMESTAMP: 'TO_TIMESTAMP';
// KW_YEAR: 已在第 239 行定义
KW_QUARTER: [Qq][Uu][Aa][Rr][Tt][Ee][Rr];
// KW_MONTH: 已在第 240 行定义
KW_DAYOFWEEK: [Dd][Aa][Yy][Oo][Ff][Ww][Ee][Ee][Kk];
KW_DAYOFMONTH: [Dd][Aa][Yy][Oo][Ff][Mm][Oo][Nn][Tt][Hh];
KW_DAYOFYEAR: [Dd][Aa][Yy][Oo][Ff][Yy][Ee][Aa][Rr];
// KW_WEEKOFYEAR: 已在前面定义
// KW_HOUR: 已在第 242 行定义
// KW_MINUTE: 已在第 243 行定义
// KW_SECOND: 已在第 244 行定义
KW_EXTRACT: [Ee][Xx][Tt][Rr][Aa][Cc][Tt];
KW_DATE_TRUNC: 'DATE_TRUNC';
KW_MAKE_DT: 'MAKE_DT';
KW_TO_UNIX_TS: 'TO_UNIX_TS';
KW_FROM_UNIX_TS: 'FROM_UNIX_TS';
KW_CHOMP: [Cc][Hh][Oo][Mm][Pp];
KW_ENCODE: [Ee][Nn][Cc][Oo][Dd][Ee];
KW_DECODE: [Dd][Ee][Cc][Oo][Dd][Ee];
KW_ASCII: [Aa][Ss][Cc][Ii][Ii];
KW_CONCAT_WS: 'CONCAT_WS';
KW_INITCAP: [Ii][Nn][Ii][Tt][Cc][Aa][Pp];
KW_LOWER: [Ll][Oo][Ww][Ee][Rr];
KW_LOWERCASE: [Ll][Oo][Ww][Ee][Rr][Cc][Aa][Ss][Ee];
KW_UPPER: [Uu][Pp][Pp][Ee][Rr];
KW_UPPERCASE: [Uu][Pp][Pp][Ee][Rr][Cc][Aa][Ss][Ee];
KW_LENGTH: [Ll][Ee][Nn][Gg][Tt][Hh];
KW_SIZE: [Ss][Ii][Zz][Ee];
KW_BIT_LENGTH: 'BIT_LENGTH';
KW_CHAR_LENGTH: 'CHAR_LENGTH';
KW_CHARACTER_LENGTH: 'CHARACTER_LENGTH';
KW_REPEAT: [Rr][Ee][Pp][Ee][Aa][Tt];
KW_SPACE: [Ss][Pp][Aa][Cc][Ee];
KW_TRIM: [Tt][Rr][Ii][Mm];
KW_LTRIM: [Ll][Tt][Rr][Ii][Mm];
KW_RTRIM: [Rr][Tt][Rr][Ii][Mm];
// KW_REPLACE: 已在第 331 行定义
KW_SPLIT: [Ss][Pp][Ll][Ii][Tt];
KW_REGEXP_REPLACE: 'REGEXP_REPLACE';
KW_REGEXP_EXTRACT: 'REGEXP_EXTRACT';
KW_REGEXP_LIKE: 'REGEXP_LIKE';
KW_REGEXP_SUBSTR: 'REGEXP_SUBSTR';
KW_GET_JSON_OBJECT: 'GET_JSON_OBJECT';
KW_TO_JSON: 'TO_JSON';
KW_FROM_JSON: 'FROM_JSON';
KW_SCHEMA_OF_JSON: 'SCHEMA_OF_JSON';
KW_PARSE_URL: 'PARSE_URL';
KW_INET_ATON: 'INET_ATON';
KW_INET_NTOA: 'INET_NTOA';
KW_IP: [Ii][Pp];
KW_CIDR_MATCH: 'CIDR_MATCH';
KW_MACADDR: [Mm][Aa][Cc][Aa][Dd][Dd][Rr];
KW_IS_IPV4: 'IS_IPV4';
KW_IS_IPV6: 'IS_IPV6';

// ============================================
// 标识符
// ============================================

// missing keywords referenced by parser grammar
DOUBLE_QUOTED_STRING: '"' (~["])+ '"';
KW_ASC: [Aa][Ss][Cc];
KW_COLUMN: [Cc][Oo][Ll][Uu][Mm][Nn];
KW_COLUMNS: [Cc][Oo][Ll][Uu][Mm][Nn][Ss];
KW_COMPUTE: [Cc][Oo][Mm][Pp][Uu][Tt][Ee];
KW_CTAS: [Cc][Tt][Aa][Ss];
KW_DESC: [Dd][Ee][Ss][Cc];
KW_EXISTS: [Ee][Xx][Ii][Ss][Tt][Ss];
KW_IF: [Ii][Ff];
KW_LIKE_STRING: 'LIKE_STRING';
KW_OVER: [Oo][Vv][Ee][Rr];
KW_PARTITIONS: [Pp][Aa][Rr][Tt][Ii][Tt][Ii][Oo][Nn][Ss];
KW_RENAME: [Rr][Ee][Nn][Aa][Mm][Ee];
KW_STATISTICS: [Ss][Tt][Aa][Tt][Ii][Ss][Tt][Ii][Cc][Ss];
KW_STRING: [Ss][Tt][Rr][Ii][Nn][Gg];
KW_TEMP: [Tt][Ee][Mm][Pp];
KW_TO: [Tt][Oo];
UID: [a-zA-Z_][a-zA-Z0-9_]*;
QUOTED_UID: '`' ~[`]+ '`';

// ============================================
// 字符串和数字
// ============================================

STRING: '\'' ( '\'\'' | ~['] )* '\'';

NUMBER: [0-9]+;
DECIMAL_NUMBER: [0-9]+'.'[0-9]+;
EXP_NUMBER: [0-9]+[eE][+-]?[0-9]+;

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

// ============================================
// 空白字符（跳过）
// ============================================

WS: [ \t\r\n]+ -> skip;

// ============================================
// 注释
// ============================================

LINE_COMMENT: '--' ~[\r\n]* -> skip;
BLOCK_COMMENT: '/*' .+? '*/' -> skip;
