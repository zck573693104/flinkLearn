lexer grammar FlinkSqlLexer;

// ============================================
// 关键字定义
// ============================================

KW_INSERT: 'INSERT';
KW_INTO: 'INTO';
KW_OVERWRITE: 'OVERWRITE';
KW_SELECT: 'SELECT';
KW_FROM: 'FROM';
KW_JOIN: 'JOIN';
KW_LEFT: 'LEFT';
KW_RIGHT: 'RIGHT';
KW_FULL: 'FULL';
KW_INNER: 'INNER';
KW_OUTER: 'OUTER';
KW_ON: 'ON';
KW_WHERE: 'WHERE';
KW_GROUP: 'GROUP';
KW_BY: 'BY';
KW_HAVING: 'HAVING';
KW_ORDER: 'ORDER';
KW_LIMIT: 'LIMIT';
KW_AS: 'AS';
KW_WITH: 'WITH';
KW_CREATE: 'CREATE';
KW_TABLE: 'TABLE';
KW_VIEW: 'VIEW';
KW_CAST: 'CAST';
KW_FUNCTION: 'FUNCTION';
KW_WINDOW: 'WINDOW';
KW_DISTINCT: 'DISTINCT';
KW_ALL: 'ALL';
KW_TRUE: 'TRUE';
KW_FALSE: 'FALSE';
KW_NULL: 'NULL';
KW_NOT: 'NOT';
KW_AND: 'AND';
KW_OR: 'OR';
KW_IS: 'IS';
KW_FOR: 'FOR';
KW_PARTITION: 'PARTITION';
KW_PRIMARY: 'PRIMARY';
KW_KEY: 'KEY';
KW_CONSTRAINT: 'CONSTRAINT';
KW_UNIQUE: 'UNIQUE';
KW_REFERENCES: 'REFERENCES';
KW_DEFAULT: 'DEFAULT';
KW_COMMENT: 'COMMENT';
KW_PRIMARY_KEY: 'PRIMARY KEY';
KW_UNBOUNDED: 'UNBOUNDED';
KW_PRECEDING: 'PRECEDING';
KW_FOLLOWING: 'FOLLOWING';
KW_CURRENT: 'CURRENT';
KW_ROW: 'ROW';
KW_ROWS: 'ROWS';
KW_RANGE: 'RANGE';
KW_BETWEEN: 'BETWEEN';
KW_IN: 'IN';

// ============================================
// 数据类型关键字
// ============================================

KW_INT: 'INT' | 'INTEGER';
KW_BIGINT: 'BIGINT';
KW_SMALLINT: 'SMALLINT';
KW_TINYINT: 'TINYINT';
KW_DECIMAL: 'DECIMAL' | 'NUMERIC';
KW_STRING: 'STRING' | 'VARCHAR' | 'CHARACTER VARYING';
KW_CHAR: 'CHARACTER' | 'CHAR';
KW_BOOLEAN: 'BOOLEAN' | 'BOOL';
KW_DATE: 'DATE';
KW_TIME: 'TIME';
KW_TIMESTAMP: 'TIMESTAMP';
KW_BINARY: 'BINARY';
KW_VARBINARY: 'VARBINARY';
KW_ARRAY: 'ARRAY';
KW_MAP: 'MAP';
KW_ROW: 'ROW';
KW_ANY: 'ANY';

// ============================================
// TVF (Table-valued Functions) - Flink 特有
// ============================================

KW_TUMBLE: 'TUMBLE';
KW_HOP: 'HOP';
KW_SESSION: 'SESSION';
KW_CUMULATE: 'CUMULATE';
KW_FLOOR: 'FLOOR';
KW_TRIGGER: 'TRIGGER';

// ============================================
// 函数关键字
// ============================================

KW_CURRENT_TIMESTAMP: 'CURRENT_TIMESTAMP';
KW_CURRENT_DATE: 'CURRENT_DATE';
KW_CURRENT_TIME: 'CURRENT_TIME';
KW_LOCALTIME: 'LOCALTIME';
KW_LOCALTIMESTAMP: 'LOCALTIMESTAMP';

// ============================================
// 标识符
// ============================================

UID: [a-zA-Z_][a-zA-Z0-9_]*;
QUOTED_UID: '`' (~`)+ '`';

// ============================================
// 字符串和数字
// ============================================

STRING: '\'' (~'\')* '\'';
DOUBLE_QUOTED_STRING: '"' (~")* '"';

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

// ============================================
// 空白字符（跳过）
// ============================================

WS: [ \t\r\n]+ -> skip;

// ============================================
// 注释
// ============================================

LINE_COMMENT: '--' ~[\r\n]* -> skip;
BLOCK_COMMENT: '/*' .+? '*/' -> skip;
