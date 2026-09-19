parser grammar FlinkSqlParser;

options {
    superClass = BaseFlinkSqlParser;
}

// ============================================
// 入口规则
// ============================================

sqlStatements
    : statement* EOF
    ;

statement
    : insertStatement SEMICOLON?
    | selectStatement SEMICOLON?
    | createTableStatement SEMICOLON?
    | cteStatement SEMICOLON?
    ;

// ============================================
// INSERT 语句 - 血缘提取核心
// ============================================

insertStatement
    : KW_INSERT (KW_INTO | KW_OVERWRITE)? tablePath 
      (LPAREN columnNameList RPAREN)? queryExpression
    ;

// ============================================
// SELECT 语句
// ============================================

selectStatement
    : queryExpression
    ;

// ============================================
// CTE 语句 (WITH) - 血缘提取核心
// ============================================

cteStatement
    : KW_WITH cteDefinition+ queryExpression
    ;

cteDefinition
    : cteName KW_AS LPAREN queryExpression RPAREN
    ;

// ============================================
// 查询表达式 - 血缘提取核心
// ============================================

queryExpression
    : selectClause fromClause? whereClause? groupByClause? havingClause? orderByClause? limitClause?
    ;

selectClause
    : KW_SELECT (DISTINCT | ALL)? columnList
    ;

columnList
    : columnDef (COMMA columnDef)*
    ;

columnDef
    : STAR
    | expression (alias)?
    ;

// ============================================
// FROM 子句 - 表引用提取核心
// ============================================

fromClause
    : KW_FROM tableReference (COMMA tableReference)*
    ;

// 表引用 - 支持单表、子查询、JOIN
// 这是血缘提取最关键的部分
// ============================================

tableReference
    : tablePath (alias)?
    | LPAREN queryExpression RPAREN (alias)?
    | tableReference joinType? JOIN tablePath (alias)? ON expression
    | tableReference joinType? JOIN LPAREN queryExpression RPAREN (alias)? ON expression
    ;

// ============================================
// JOIN 类型
// ============================================

joinType
    : KW_LEFT? KW_OUTER?
    | KW_RIGHT? KW_OUTER?
    | KW_FULL? KW_OUTER?
    | KW_INNER
    | KW_CROSS
    ;

// ============================================
// WHERE/GROUP BY/HAVING/ORDER BY/LIMIT
// ============================================

whereClause
    : KW_WHERE expression
    ;

groupByClause
    : KW_GROUP BY columnList
    ;

havingClause
    : KW_HAVING expression
    ;

orderByClause
    : KW_ORDER BY orderByItem (COMMA orderByItem)*
    ;

orderByItem
    : expression (ASC | DESC)?
    ;

limitClause
    : KW_LIMIT NUMBER
    ;

// ============================================
// 表路径和列名列表
// ============================================

tablePath
    : uid (DOT uid)*
    ;

columnNameList
    : uid (COMMA uid)*
    ;

// ============================================
// 表达式 - 用于 WHERE、JOIN 条件等
// ============================================

expression
    : primaryExpression
    | functionCall
    | castExpression
    | binaryExpression
    | betweenExpression
    | inExpression
    ;

primaryExpression
    : columnRef
    | literal
    | LPAREN expression RPAREN
    ;

columnRef
    : tablePath DOT uid
    | uid
    ;

functionCall
    : functionName LPAREN (DISTINCT? expression (COMMA expression)*)? RPAREN
    | functionName LPAREN DISTINCT? * RPAREN
    ;

castExpression
    : KW_CAST LPAREN expression AS dataType RPAREN
    ;

binaryExpression
    : expression operator expression
    ;

// BETWEEN 表达式
betweenExpression
    : expression KW_NOT? BETWEEN expression AND expression
    ;

// IN 表达式
inExpression
    : expression (KW_NOT? IN LPAREN expression (COMMA expression)* RPAREN 
                 | LPAREN queryExpression RPAREN)
    ;

operator
    : PLUS | MINUS | MULT | DIV | MOD
    | EQ | NEQ | LT | GT | LTE | GTE
    | KW_AND | KW_OR
    ;

literal
    : STRING
    | DOUBLE_QUOTED_STRING
    | NUMBER
    | DECIMAL_NUMBER
    | EXP_NUMBER
    | KW_TRUE
    | KW_FALSE
    | KW_NULL
    ;

dataType
    : typeName
    | typeName LPAREN NUMBER (COMMA NUMBER)? RPAREN
    ;

typeName
    : KW_INT | KW_BIGINT | KW_SMALLINT | KW_TINYINT
    | KW_DECIMAL | KW_STRING | KW_CHAR | KW_BOOLEAN
    | KW_DATE | KW_TIME | KW_TIMESTAMP
    | KW_BINARY | KW_VARBINARY
    | KW_ARRAY | KW_MAP | KW_ROW
    ;

// ============================================
// 辅助规则
// ============================================

uid
    : UID
    | QUOTED_UID
    ;

alias
    : KW_AS? uid
    ;

functionName
    : uid
    ;

cteName
    : uid
    ;

// ============================================
// TVF (Table-valued Functions) - Flink 特有
// ============================================

// TVF 函数调用（TUMBLE, HOP, SESSION, CUMULATE）
tvfFunction
    : functionName LPAREN tablePath (COMMA expression)+ RPAREN
    ;

// 窗口定义（用于 ROW_NUMBER, RANK 等窗口函数）
windowSpecification
    : windowName OVER LPAREN windowDefinition RPAREN
    ;

windowDefinition
    : partitionByClause? orderByClause? windowFrame?
    ;

partitionByClause
    : KW_PARTITION BY columnList
    ;

windowFrame
    : (ROWS | RANGE) frameBound
    | (ROWS | RANGE) BETWEEN frameBound AND frameBound
    ;

frameBound
    : UNBOUNDED PRECEDING
    | UNBOUNDED FOLLOWING
    | CURRENT ROW
    | INTEGER_VALUE PRECEDING
    | INTEGER_VALUE FOLLOWING
    ;

// ============================================
// CREATE TABLE 语句（CTAS 支持）
// ============================================

createTableStatement
    : KW_CREATE TABLE (IF NOT EXISTS)? tablePath 
      LPAREN columnDefinition (COMMA columnDefinition)* RPAREN tableProperties?
    | KW_CREATE TABLE (IF NOT EXISTS)? tablePath AS queryExpression
    ;

columnDefinition
    : uid dataType columnConstraint*
    ;

columnConstraint
    : KW_PRIMARY KEY
    | KW_NOT NULL
    | KW_NULL
    | KW_DEFAULT expression
    | KW_COMMENT STRING
    ;

tableProperties
    : KW_WITH LPAREN tableProperty (COMMA tableProperty)* RPAREN
    ;

tableProperty
    : uid EQ expression
    ;
