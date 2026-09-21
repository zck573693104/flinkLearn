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
// try_cast(x AS int)：带下划线的整体关键字，必须定义在 UID 之前才能抢过长标识符匹配
KW_TRY_CAST: [Tt][Rr][Yy] '_' [Cc][Aa][Ss][Tt];
KW_DISTINCT: [Dd][Ii][Ss][Tt][Ii][Nn][Cc][Tt];
KW_ALL: [Aa][Ll][Ll];
KW_TRUE: [Tt][Rr][Uu][Ee];
KW_FALSE: [Ff][Aa][Ll][Ss][Ee];
KW_NULL: [Nn][Uu][Ll][Ll];
KW_NOT: [Nn][Oo][Tt];
KW_AND: [Aa][Nn][Dd];
KW_OR: [Oo][Rr];
KW_IS: [Ii][Ss];
KW_PARTITION: [Pp][Aa][Rr][Tt][Ii][Tt][Ii][Oo][Nn];
KW_PARTITIONED: [Pp][Aa][Rr][Tt][Ii][Tt][Ii][Oo][Nn][Ee][Dd];
KW_PRIMARY: [Pp][Rr][Ii][Mm][Aa][Rr][Yy];
KW_KEY: [Kk][Ee][Yy];
KW_DEFAULT: [Dd][Ee][Ff][Aa][Uu][Ll][Tt];
KW_COMMENT: [Cc][Oo][Mm][Mm][Ee][Nn][Tt];
KW_UNBOUNDED: [Uu][Nn][Bb][Oo][Uu][Nn][Dd][Ee][Dd];
KW_PRECEDING: [Pp][Rr][Ee][Cc][Ee][Dd][Ii][Nn][Gg];
KW_FOLLOWING: [Ff][Oo][Ll][Ll][Oo][Ww][Ii][Nn][Gg];
KW_CURRENT: [Cc][Uu][Rr][Rr][Ee][Nn][Tt];
KW_ROW: [Rr][Oo][Ww];
KW_ROWS: [Rr][Oo][Ww][Ss];
KW_RANGE: [Rr][Aa][Nn][Gg][Ee];
KW_BETWEEN: [Bb][Ee][Tt][Ww][Ee][Ee][Nn];
KW_IN: [Ii][Nn];
KW_FOR: [Ff][Oo][Rr];
KW_UNION: [Uu][Nn][Ii][Oo][Nn];
KW_INTERSECT: [Ii][Nn][Tt][Ee][Rr][Ss][Ee][Cc][Tt];
KW_EXCEPT: [Ee][Xx][Cc][Ee][Pp][Tt] | [Mm][Ii][Nn][Uu][Ss];
KW_DROP: [Dd][Rr][Oo][Pp];
KW_TRUNCATE: [Tt][Rr][Uu][Nn][Cc][Aa][Tt][Ee];
KW_ALTER: [Aa][Ll][Tt][Ee][Rr];

// ============================================
// Spark 特有关键字
// ============================================

// KW_OVERWRITE: 已在第 9 行定义
KW_UPDATE: [Uu][Pp][Dd][Aa][Tt][Ee];
KW_DELETE: [Dd][Ee][Ll][Ee][Tt][Ee];
KW_TABLES: [Tt][Aa][Bb][Ll][Ee][Ss];
KW_TEMPORARY: [Tt][Ee][Mm][Pp][Oo][Rr][Aa][Rr][Yy];
KW_ANALYZE: [Aa][Nn][Aa][Ll][Yy][Zz][Ee];
KW_CACHE: [Cc][Aa][Cc][Hh][Ee];
KW_UNCACHE: [Uu][Nn][Cc][Aa][Cc][Hh][Ee];
KW_FUNCTIONS: [Ff][Uu][Nn][Cc][Tt][Ii][Oo][Nn][Ss];
KW_DATABASES: [Dd][Aa][Tt][Aa][Bb][Aa][Ss][Ee][Ss];
KW_SCHEMAS: [Ss][Cc][Hh][Ee][Mm][Aa][Ss];
KW_CATALOGS: [Cc][Aa][Tt][Aa][Ll][Oo][Gg][Ss];
KW_SHOW: [Ss][Hh][Oo][Ww];
KW_USE: [Uu][Ss][Ee];
KW_CASE: [Cc][Aa][Ss][Ee];
KW_WHEN: [Ww][Hh][Ee][Nn];
KW_THEN: [Tt][Hh][Ee][Nn];
KW_ELSE: [Ee][Ll][Ss][Ee];
KW_END: [Ee][Nn][Dd];
KW_SET: [Ss][Ee][Tt];
KW_UNSET: [Uu][Nn][Ss][Ee][Tt];
KW_ADD: [Aa][Dd][Dd];
KW_FIRST: [Ff][Ii][Rr][Ss][Tt];
KW_LAST: [Ll][Aa][Ss][Tt];
KW_WINDOW: [Ww][Ii][Nn][Dd][Oo][Ww];
KW_TBLPROPERTIES: [Tt][Bb][Ll][Pp][Rr][Oo][Pp][Ee][Rr][Tt][Ii][Ee][Ss];
KW_PIVOT: [Pp][Ii][Vv][Oo][Tt];
KW_UNPIVOT: [Uu][Nn][Pp][Ii][Vv][Oo][Tt];
KW_CLUSTER: [Cc][Ll][Uu][Ss][Tt][Ee][Rr];
KW_DISTRIBUTE: [Dd][Ii][Ss][Tt][Rr][Ii][Bb][Uu][Tt][Ee];
KW_SORT: [Ss][Oo][Rr][Tt];
KW_SEMI: [Ss][Ee][Mm][Ii];
KW_ANTI: [Aa][Nn][Tt][Ii];
KW_CROSS: [Cc][Rr][Oo][Ss][Ss];
// KW_DETERMINISTIC: 已在第 176 行定义
// KW_NOT: 已在第 38 行定义
KW_STRUCT: [Ss][Tt][Rr][Uu][Cc][Tt];
KW_VARIANT: [Vv][Aa][Rr][Ii][Aa][Nn][Tt];
KW_BYTEARRAY: [Bb][Yy][Tt][Ee][Aa][Rr][Rr][Aa][Yy];
KW_BINARY: [Bb][Ii][Nn][Aa][Rr][Yy];
KW_VARBINARY: [Vv][Aa][Rr][Bb][Ii][Nn][Aa][Rr][Yy];
KW_DECIMAL: [Dd][Ee][Cc][Ii][Mm][Aa][Ll];
KW_CHAR: [Cc][Hh][Aa][Rr];

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
KW_DOUBLE: [Dd][Oo][Uu][Bb][Ll][Ee];
KW_FLOAT: [Ff][Ll][Oo][Aa][Tt];
// KW_ROW: 已在第 55 行定义

// ============================================
// INTERVAL 字面量与时间单位
// ============================================

KW_INTERVAL: [Ii][Nn][Tt][Ee][Rr][Vv][Aa][Ll];
KW_SECOND: [Ss][Ee][Cc][Oo][Nn][Dd];
KW_MINUTE: [Mm][Ii][Nn][Uu][Tt][Ee];
KW_HOUR: [Hh][Oo][Uu][Rr];
KW_DAY: [Dd][Aa][Yy];
KW_MONTH: [Mm][Oo][Nn][Tt][Hh];
KW_QUARTER: [Qq][Uu][Aa][Rr][Tt][Ee][Rr];
KW_YEAR: [Yy][Ee][Aa][Rr];

// ============================================
// TVF (Table-valued Functions) - Spark 特有
// ============================================

// LATERAL VIEW 是两个词：ANTLR 词法规则会忽略规则内的空白，
// 写成 `KW_LATERAL_VIEW: LATERAL VIEW` 实际匹配的是 LATERALVIEW，必须拆成两个 token
KW_LATERAL: [Ll][Aa][Tt][Ee][Rr][Aa][Ll];
KW_RESPECT: [Rr][Ee][Ss][Pp][Ee][Cc][Tt];
KW_IGNORE: [Ii][Gg][Nn][Oo][Rr][Ee];
KW_NULLS: [Nn][Uu][Ll][Ll][Ss];
KW_RECURSIVE: [Rr][Ee][Cc][Uu][Rr][Ss][Ii][Vv][Ee];
KW_QUALIFY: [Qq][Uu][Aa][Ll][Ii][Ff][Yy];
KW_DIRECTORY: [Dd][Ii][Rr][Ee][Cc][Tt][Oo][Rr][Yy];
KW_LOCAL: [Ll][Oo][Cc][Aa][Ll];
KW_STORED: [Ss][Tt][Oo][Rr][Ee][Dd];
KW_USING: [Uu][Ss][Ii][Nn][Gg];
KW_VALUES: [Vv][Aa][Ll][Uu][Ee][Ss];
KW_GROUPING: [Gg][Rr][Oo][Uu][Pp][Ii][Nn][Gg];
KW_SETS: [Ss][Ee][Tt][Ss];
KW_CUBE: [Cc][Uu][Bb][Ee];
KW_ROLLUP: [Rr][Oo][Ll][Ll][Uu][Pp];
KW_TABLESAMPLE: [Tt][Aa][Bb][Ll][Ee][Ss][Aa][Mm][Pp][Ll][Ee];
KW_PERCENT: [Pp][Ee][Rr][Cc][Ee][Nn][Tt];
// KW_TRUNCATE: 已在第 90 行定义
// KW_YEAR: 已在第 239 行定义
// KW_MONTH: 已在第 240 行定义
// KW_WEEKOFYEAR: 已在前面定义
// KW_HOUR: 已在第 242 行定义
// KW_MINUTE: 已在第 243 行定义
// KW_SECOND: 已在第 244 行定义
// KW_REPLACE: 已在第 331 行定义

// ============================================
// 标识符
// ============================================

// missing keywords referenced by parser grammar
DOUBLE_QUOTED_STRING: '"' (~["])+ '"';
KW_ASC: [Aa][Ss][Cc];
KW_COLUMN: [Cc][Oo][Ll][Uu][Mm][Nn];
KW_COLUMNS: [Cc][Oo][Ll][Uu][Mm][Nn][Ss];
KW_COMPUTE: [Cc][Oo][Mm][Pp][Uu][Tt][Ee];
KW_DESC: [Dd][Ee][Ss][Cc];
KW_EXISTS: [Ee][Xx][Ii][Ss][Tt][Ss];
KW_IF: [Ii][Ff];
KW_LIKE: [Ll][Ii][Kk][Ee];
KW_RLIKE: [Rr][Ll][Ii][Kk][Ee];
KW_REGEXP: [Rr][Ee][Gg][Ee][Xx][Pp];
KW_LIKE_STRING: 'LIKE_STRING';
KW_OVER: [Oo][Vv][Ee][Rr];
KW_PARTITIONS: [Pp][Aa][Rr][Tt][Ii][Tt][Ii][Oo][Nn][Ss];
KW_RENAME: [Rr][Ee][Nn][Aa][Mm][Ee];
KW_STATISTICS: [Ss][Tt][Aa][Tt][Ii][Ss][Tt][Ii][Cc][Ss];
KW_STRING: [Ss][Tt][Rr][Ii][Nn][Gg] | [Vv][Aa][Rr][Cc][Hh][Aa][Rr];
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

// ============================================
// 空白字符（跳过）
// ============================================

WS: [ \t\r\n]+ -> skip;

// ============================================
// 注释
// ============================================

LINE_COMMENT: '--' ~[\r\n]* -> skip;
BLOCK_COMMENT: '/*' .+? '*/' -> skip;
