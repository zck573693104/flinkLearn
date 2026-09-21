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
// Presto 特有关键字
// ============================================

KW_EXPLAIN: [Ee][Xx][Pp][Ll][Aa][Ii][Nn];
KW_ANALYZE: [Aa][Nn][Aa][Ll][Yy][Zz][Ee];
KW_SET: [Ss][Ee][Tt];
KW_COLUMN: [Cc][Oo][Ll][Uu][Mm][Nn];
// KW_SCHEMA: 已在第 112 行定义
KW_MAP: [Mm][Aa][Pp];
KW_ARRAY: [Aa][Rr][Rr][Aa][Yy];
// KW_ROW: 已在第 55 行定义
KW_DECIMAL: [Dd][Ee][Cc][Ii][Mm][Aa][Ll];
KW_BOOLEAN: [Bb][Oo][Oo][Ll][Ee][Aa][Nn];
KW_TINYINT: [Tt][Ii][Nn][Yy][Ii][Nn][Tt];
KW_DOUBLE: [Dd][Oo][Uu][Bb][Ll][Ee];
KW_FLOAT: [Ff][Ll][Oo][Aa][Tt];
KW_REAL: [Rr][Ee][Aa][Ll];
KW_SMALLINT: [Ss][Mm][Aa][Ll][Ll][Ii][Nn][Tt];
KW_INT: [Ii][Nn][Tt] | [Ii][Nn][Tt][Ee][Gg][Ee][Rr];
KW_BIGINT: [Bb][Ii][Gg][Ii][Nn][Tt];
KW_VARBINARY: [Vv][Aa][Rr][Bb][Ii][Nn][Aa][Rr][Yy];
KW_CHAR: [Cc][Hh][Aa][Rr];
// CHARACTER VARYING 是两个词：词法规则内的空白会被忽略，必须拆成两个 token 在 Parser 侧组合
KW_CHARACTER: [Cc][Hh][Aa][Rr][Aa][Cc][Tt][Ee][Rr];
KW_VARYING: [Vv][Aa][Rr][Yy][Ii][Nn][Gg];
KW_JSON: [Jj][Ss][Oo][Nn];
KW_IPADDRESS: [Ii][Pp][Aa][Dd][Dd][Rr][Ee][Ss][Ss];
KW_UUID: [Uu][Uu][Ii][Dd];
KW_TIME: [Tt][Ii][Mm][Ee];
KW_TIMESTAMP: [Tt][Ii][Mm][Ee][Ss][Tt][Aa][Mm][Pp];
KW_DATE: [Dd][Aa][Tt][Ee];
KW_DISTRIBUTION: [Dd][Ii][Ss][Tt][Rr][Ii][Bb][Uu][Tt][Ii][Oo][Nn];
// KW_GROUP: 已在第 20 行定义
// KW_SELECT: 已在第 10 行定义
// KW_INSERT: 已在第 7 行定义
// KW_UPDATE: 已在前面定义
// KW_DELETE: 已在前面定义
// KW_DROP: 已在前面定义
// KW_QUERY: 已在前面定义
// KW_CREATE: 已在第 27 行定义
// KW_ALTER: 已在第 64 行定义
KW_TEMPORARY: [Tt][Ee][Mm][Pp][Oo][Rr][Aa][Rr][Yy];
KW_TEMP: [Tt][Ee][Mm][Pp];
// KW_UNIQUE: 已在第 46 行定义
// KW_USER: 已在第 202 行定义
KW_WINDOW: [Ww][Ii][Nn][Dd][Oo][Ww];
// KW_SESSION: 已在第 99 行定义
// KW_QUOTA: 'QUOTA';
// KW_LIMIT: 已在第 24 行定义
// KW_CLUSTER: 已在第 116 行定义
// KW_PARTITION: 已在第 42 行定义
// KW_HUAWEI: 已在第 308 行定义

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
// KW_JSON: 已在第 151 行定义
// KW_IPADDRESS: 已在第 152 行定义
// KW_UUID: 已在前面定义

// ============================================
// TVF (Table-valued Functions) - Presto 特有
// ============================================

KW_UNNEST: [Uu][Nn][Nn][Ee][Ss][Tt];
KW_USE: [Uu][Ss][Ee];
KW_CASE: [Cc][Aa][Ss][Ee];
KW_WHEN: [Ww][Hh][Ee][Nn];
KW_THEN: [Tt][Hh][Ee][Nn];
KW_ELSE: [Ee][Ll][Ss][Ee];
KW_END: [Ee][Nn][Dd];
// KW_KEY: 已在第 44 行定义
// KW_RADIUS: 已在第 431 行定义

// ============================================
// 标识符
// ============================================

// missing keywords referenced by parser grammar
DOUBLE_QUOTED_ID: '"' (~['"])+ '"';
DOUBLE_QUOTED_STRING: '"' (~["])+ '"';
KW_ADD: [Aa][Dd][Dd];
KW_ASC: [Aa][Ss][Cc];
KW_BINARY: [Bb][Ii][Nn][Aa][Rr][Yy];
KW_COST: [Cc][Oo][Ss][Tt];
KW_CROSS: [Cc][Rr][Oo][Ss][Ss];
KW_DELETE: [Dd][Ee][Ll][Ee][Tt][Ee];
KW_DESC: [Dd][Ee][Ss][Cc];
KW_EXISTS: [Ee][Xx][Ii][Ss][Tt][Ss];
KW_IF: [Ii][Ff];
KW_NATURAL: [Nn][Aa][Tt][Uu][Rr][Aa][Ll];
KW_OVER: [Oo][Vv][Ee][Rr];
KW_LIKE: [Ll][Ii][Kk][Ee];
KW_QUERY: [Qq][Uu][Ee][Rr][Yy];
KW_RENAME: [Rr][Ee][Nn][Aa][Mm][Ee];
KW_STRING: [Ss][Tt][Rr][Ii][Nn][Gg] | [Vv][Aa][Rr][Cc][Hh][Aa][Rr];
KW_TO: [Tt][Oo];
KW_UPDATE: [Uu][Pp][Dd][Aa][Tt][Ee];
// 时间间隔 / EXTRACT / 采样 / UNNEST ORDINALITY 等 Presto 原生语法词
KW_INTERVAL: [Ii][Nn][Tt][Ee][Rr][Vv][Aa][Ll];
KW_EXTRACT: [Ee][Xx][Tt][Rr][Aa][Cc][Tt];
KW_ORDINALITY: [Oo][Rr][Dd][Ii][Nn][Aa][Ll][Ii][Tt][Yy];
KW_VALUES: [Vv][Aa][Ll][Uu][Ee][Ss];
KW_GROUPING: [Gg][Rr][Oo][Uu][Pp][Ii][Nn][Gg];
KW_SETS: [Ss][Ee][Tt][Ss];
KW_CUBE: [Cc][Uu][Bb][Ee];
KW_ROLLUP: [Rr][Oo][Ll][Ll][Uu][Pp];
KW_TABLESAMPLE: [Tt][Aa][Bb][Ll][Ee][Ss][Aa][Mm][Pp][Ll][Ee];
KW_BERNOULLI: [Bb][Ee][Rr][Nn][Oo][Uu][Ll][Ll][Ii];
KW_SYSTEM: [Ss][Yy][Ss][Tt][Ee][Mm];
KW_SECOND: [Ss][Ee][Cc][Oo][Nn][Dd];
KW_MINUTE: [Mm][Ii][Nn][Uu][Tt][Ee];
KW_HOUR: [Hh][Oo][Uu][Rr];
KW_DAY: [Dd][Aa][Yy];
KW_WEEK: [Ww][Ee][Ee][Kk];
KW_MONTH: [Mm][Oo][Nn][Tt][Hh];
KW_QUARTER: [Qq][Uu][Aa][Rr][Tt][Ee][Rr];
KW_YEAR: [Yy][Ee][Aa][Rr];
UID: [a-zA-Z_][a-zA-Z0-9_]*;
QUOTED_UID: '`' (~[`])+ '`';

// ============================================
// 字符串和数字
// ============================================

STRING: '\'' ( '\'\'' | ~['] )* '\'';

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
AMPERSAND: '&';
BAR: '|';
CARET: '^';
TILDE: '~';
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
