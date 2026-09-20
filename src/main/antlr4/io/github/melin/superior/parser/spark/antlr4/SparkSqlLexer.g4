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
KW_UNION: [Uu][Nn][Ii][Oo][Nn];
KW_INTERSECT: [Ii][Nn][Tt][Ee][Rr][Ss][Ee][Cc][Tt];
KW_EXCEPT: [Ee][Xx][Cc][Ee][Pp][Tt] | [Mm][Ii][Nn][Uu][Ss];
KW_DROP: [Dd][Rr][Oo][Pp];
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
// KW_ROW: 已在第 55 行定义

// ============================================
// TVF (Table-valued Functions) - Spark 特有
// ============================================

KW_LATERAL_VIEW: [Ll][Aa][Tt][Ee][Rr][Aa][Ll] [Vv][Ii][Ee][Ww];
// KW_RESPECT: 已在前面定义
// KW_IGNORE: 已在第 88 行定义
KW_NULLS: [Nn][Uu][Ll][Ll][Ss];
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
