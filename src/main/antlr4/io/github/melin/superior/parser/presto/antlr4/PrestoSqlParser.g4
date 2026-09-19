parser grammar PrestoSqlParser;

// options { }

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
    | explainStatement SEMICOLON?
    | dropTableStatement SEMICOLON?
    | alterTableStatement SEMICOLON?
    | updateStatement SEMICOLON?
    | deleteStatement SEMICOLON?
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
// 支持集合操作：UNION/INTERSECT/EXCEPT
// ============================================

queryExpression
    : selectClause fromClause? whereClause? groupByClause? havingClause? 
      orderByClause? limitClause? windowClause?
      (KW_UNION (DISTINCT | ALL)? queryExpression
       | KW_INTERSECT (DISTINCT | ALL)? queryExpression
       | KW_EXCEPT (DISTINCT | ALL)? queryExpression)*
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

tableReference
    : tablePath (alias)?
    | LPAREN queryExpression RPAREN (alias)?
    | tableReference joinType? JOIN tablePath (alias)? ON expression
    | tableReference joinType? JOIN LPAREN queryExpression RPAREN (alias)? ON expression
    | UNNEST LPAREN expression RPAREN (alias)?
    ;

joinType
    : KW_LEFT? KW_OUTER?
    | KW_RIGHT? KW_OUTER?
    | KW_FULL? KW_OUTER?
    | KW_INNER
    | KW_CROSS
    | KW_NATURAL
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

windowClause
    : KW_WINDOW windowSpecification (COMMA windowSpecification)*
    ;

windowSpecification
    : uid AS windowName OVER LPAREN windowDefinition RPAREN
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
// 表达式
// ============================================

expression
    : primaryExpression
    | functionCall
    | castExpression
    | caseExpression
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
    | functionName LPAREN DISTINCT? expression (COMMA expression)* RPAREN
    ;

castExpression
    : KW_CAST LPAREN expression AS dataType RPAREN
    ;

binaryExpression
    : expression operator expression
    ;

betweenExpression
    : expression KW_NOT? BETWEEN expression AND expression
    ;

inExpression
    : expression (KW_NOT? IN LPAREN expression (COMMA expression)* RPAREN 
                 | LPAREN queryExpression RPAREN)
    ;

caseExpression
    : KW_CASE expression? whenClause+ (KW_ELSE elseClause)? KW_END
    | KW_CASE KW_WHEN condition THEN result (KW_WHEN condition THEN result)+ (KW_ELSE elseClause)? KW_END
    ;

whenClause
    : KW_WHEN condition KW_THEN result
    ;

condition
    : expression
    ;

result
    : expression
    ;

elseClause
    : expression
    ;

operator
    : PLUS | MINUS | MULT | DIV | MOD
    | EQ | NEQ | LT | GT | LTE | GTE
    | KW_AND | KW_OR
    | ARROW
    ;

literal
    : STRING
    | DOUBLE_QUOTED_STRING
    | NUMBER
    | DECIMAL_NUMBER
    | EXP_NUMBER
    | HEX_NUMBER
    | BIN_NUMBER
    | KW_TRUE
    | KW_FALSE
    | KW_NULL
    ;

dataType
    : typeName
    | typeName LPAREN NUMBER (COMMA NUMBER)? RPAREN
    | ARRAY LT dataType GT
    | MAP LT dataType COMMA dataType GT
    | ROW LT rowType GT
    ;

rowType
    : field (COMMA field)*
    ;

field
    : uid COLON dataType
    ;

typeName
    : KW_INT | KW_BIGINT | KW_SMALLINT | KW_TINYINT
    | KW_DECIMAL | KW_STRING | KW_CHAR | KW_BOOLEAN
    | KW_DATE | KW_TIME | KW_TIMESTAMP
    | KW_BINARY | KW_VARBINARY
    | KW_ARRAY | KW_MAP | KW_ROW
    | KW_JSON | KW_IPADDRESS | KW_UUID
    ;

// ============================================
// 辅助规则
// ============================================

uid
    : UID
    | QUOTED_UID
    | DOUBLE_QUOTED_ID
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

windowName
    : uid
    ;

// ============================================
// CREATE TABLE/VIEW 语句（含临时表、CTAS 支持）
// ============================================

createTableStatement
    : KW_CREATE (TEMPORARY | TEMP)? TABLE (IF NOT EXISTS)? tablePath 
      LPAREN columnDefinition (COMMA columnDefinition)* RPAREN tableProperties?
    | KW_CREATE (TEMPORARY | TEMP)? TABLE (IF NOT EXISTS)? tablePath AS queryExpression
    | KW_CREATE (TEMPORARY | TEMP)? VIEW (IF NOT EXISTS)? tablePath AS queryExpression
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

// ============================================
// EXPLAIN 语句
// ============================================

explainStatement
    : KW_EXPLAIN (ANALYZE | COST | DISTRIBUTION)? statementType
    ;

// ============================================
// DROP TABLE 语句
// ============================================

dropTableStatement
    : KW_DROP TABLE (IF EXISTS)? tablePath
    ;

// ============================================
// ALTER TABLE 语句
// ============================================

alterTableStatement
    : KW_ALTER TABLE tablePath alterTableClause
    ;

alterTableClause
    : RENAME TO tablePath
    | ADD COLUMN columnDefinition
    | DROP COLUMN uid
    | SET tableProperties
    ;

// ============================================
// UPDATE 语句（Presto）
// ============================================

updateStatement
    : KW_UPDATE tablePath (KW_AS alias)?
      SET assignmentList
      (KW_WHERE condition)?
    ;

assignmentList
    : assignment (COMMA assignment)*
    ;

assignment
    : uid EQ expression
    ;

// ============================================
// DELETE 语句（Presto）
// ============================================

deleteStatement
    : KW_DELETE FROM tablePath (KW_AS alias)?
      (KW_WHERE condition)?
    ;

statementType
    : KW_QUERY
    | KW_DELETE
    | KW_UPDATE
    | KW_INSERT
    ;
