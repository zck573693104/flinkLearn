parser grammar FlinkSqlParser;

options { tokenVocab=FlinkSqlLexer; }

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
    | dropTableStatement SEMICOLON?
    | alterTableStatement SEMICOLON?
    | useStatement SEMICOLON?
    ;

useStatement
    : KW_USE tablePath
    ;

// ============================================
// INSERT 语句 - 血缘提取核心
// ============================================

insertStatement
    : KW_INSERT (KW_INTO | KW_OVERWRITE)? KW_TABLE? tablePath 
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
    : KW_WITH cteDefinition (COMMA cteDefinition)* (insertStatement | queryExpression)
    ;

cteDefinition
    : cteName KW_AS LPAREN queryExpression RPAREN
    ;

// ============================================
// 查询表达式 - 血缘提取核心
// 支持集合操作：UNION/INTERSECT/EXCEPT
// ============================================

queryExpression
    : selectClause fromClause? whereClause? groupByClause? havingClause? orderByClause? limitClause?
      (KW_UNION (KW_DISTINCT | KW_ALL)? queryExpression
       | KW_INTERSECT (KW_DISTINCT | KW_ALL)? queryExpression
       | KW_EXCEPT (KW_DISTINCT | KW_ALL)? queryExpression)*
    ;

selectClause
    : KW_SELECT (KW_DISTINCT | KW_ALL)? columnList
    ;

columnList
    : columnDef (COMMA columnDef)*
    ;

columnDef
    : MULT
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
    | KW_UNNEST LPAREN expression RPAREN (aliasWithColumns | alias)?
    | tableReference joinType? KW_JOIN tablePath (alias)? KW_ON expression
    | tableReference joinType? KW_JOIN LPAREN queryExpression RPAREN (alias)? KW_ON expression
    | tableReference joinType? KW_JOIN KW_UNNEST LPAREN expression RPAREN (aliasWithColumns | alias)?
    ;

aliasWithColumns
    : KW_AS? uid LPAREN uid (COMMA uid)* RPAREN
    ;

// ============================================
// JOIN 类型
// ============================================

joinType
    : KW_LEFT KW_OUTER?
    | KW_RIGHT KW_OUTER?
    | KW_FULL KW_OUTER?
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
    : KW_GROUP KW_BY columnList
    ;

havingClause
    : KW_HAVING expression
    ;

orderByClause
    : KW_ORDER KW_BY orderByItem (COMMA orderByItem)*
    ;

orderByItem
    : expression (KW_ASC | KW_DESC)?
    ;

limitClause
    : KW_LIMIT NUMBER
    ;

// ============================================
// 表路径和列名列表
// 支持三级命名空间：catalog.schema.table
// ============================================

tablePath
    : uid (DOT uid)? (DOT uid)?
    ;

columnNameList
    : uid (COMMA uid)*
    ;

// ============================================
// 表达式 - 用于 WHERE、JOIN 条件等
// ============================================

expression
    : expression LBRACKET expression RBRACKET
    | expression DOT uid
    | (PLUS | MINUS) expression
    | expression (PLUS | MINUS | MULT | DIV | MOD) expression
    | expression (EQ | NEQ | LT | GT | LTE | GTE | CONCAT) expression
    | expression KW_AND expression
    | expression KW_OR expression
    | KW_NOT expression
    | expression KW_IS KW_NOT? (KW_NULL | KW_TRUE | KW_FALSE)
    | expression KW_NOT? KW_IN LPAREN expression (COMMA expression)* RPAREN
    | expression KW_NOT? KW_IN LPAREN queryExpression RPAREN
    | expression KW_NOT? KW_BETWEEN expression KW_AND expression
    | LPAREN queryExpression RPAREN
    | caseExpression
    | primaryExpression
    | functionCall
    | castExpression
    | MULT
    ;

caseExpression
    : KW_CASE expression? (KW_WHEN expression KW_THEN expression)+ (KW_ELSE expression)? KW_END
    ;

primaryExpression
    : tablePath DOT MULT
    | columnRef
    | literal
    | LPAREN expression RPAREN
    ;

columnRef
    : tablePath DOT uid
    | uid
    ;

functionCall
    : functionName LPAREN (KW_DISTINCT? expression (COMMA expression)*)? RPAREN
    | functionName LPAREN KW_DISTINCT? expression (COMMA expression)* RPAREN
    ;

castExpression
    : KW_CAST LPAREN expression KW_AS dataType RPAREN
    ;

binaryExpression
    : expression operator expression
    ;

// BETWEEN 表达式
betweenExpression
    : expression KW_NOT? KW_BETWEEN expression KW_AND expression
    ;

// IN 表达式
inExpression
    : expression (KW_NOT? KW_IN LPAREN expression (COMMA expression)* RPAREN 
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
    | KW_DEFAULT   // default 数据库名等场景下关键字可作标识符
    ;

alias
    : KW_AS? uid
    ;

functionName
    : uid
    | KW_IF
    | KW_LEFT
    | KW_RIGHT
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
    : uid KW_OVER LPAREN windowDefinition RPAREN
    ;

windowDefinition
    : partitionByClause? orderByClause? windowFrame?
    ;

partitionByClause
    : KW_PARTITION KW_BY columnList
    ;

windowFrame
    : (KW_ROWS | KW_RANGE) frameBound
    | (KW_ROWS | KW_RANGE) KW_BETWEEN frameBound KW_AND frameBound
    ;

frameBound
    : KW_UNBOUNDED KW_PRECEDING
    | KW_UNBOUNDED KW_FOLLOWING
    | KW_CURRENT KW_ROW
    | NUMBER KW_PRECEDING
    | NUMBER KW_FOLLOWING
    ;

// ============================================
// CREATE TABLE/VIEW 语句（含临时表、CTAS 支持）
// ============================================

createTableStatement
    : KW_CREATE (KW_TEMPORARY | KW_TEMP)? KW_TABLE (KW_IF KW_NOT KW_EXISTS)? tablePath 
      LPAREN columnDefinition (COMMA columnDefinition)* RPAREN tableProperties?
    | KW_CREATE (KW_TEMPORARY | KW_TEMP)? KW_TABLE (KW_IF KW_NOT KW_EXISTS)? tablePath KW_AS queryExpression
    | KW_CREATE (KW_TEMPORARY | KW_TEMP)? KW_VIEW (KW_IF KW_NOT KW_EXISTS)? tablePath KW_AS queryExpression
    ;

columnDefinition
    : uid dataType columnConstraint*
    ;

columnConstraint
    : KW_PRIMARY_KEY
    | KW_PRIMARY KW_KEY
    | KW_NOT KW_NULL
    | KW_NULL
    | KW_DEFAULT expression
    | KW_COMMENT STRING
    ;

tableProperties
    : KW_WITH LPAREN tableProperty (COMMA tableProperty)* RPAREN
    ;

tableProperty
    : (uid | STRING) EQ expression
    ;

// ============================================
// DROP TABLE 语句
// ============================================

dropTableStatement
    : KW_DROP KW_TABLE (KW_IF KW_EXISTS)? tablePath
    ;

// ============================================
// ALTER TABLE 语句
// ============================================

alterTableStatement
    : KW_ALTER KW_TABLE tablePath alterTableClause
    ;

alterTableClause
    : KW_RENAME KW_TO tablePath
    | KW_ADD KW_COLUMN columnDefinition
    | KW_DROP KW_COLUMN uid
    | KW_SET tableProperties
    ;
