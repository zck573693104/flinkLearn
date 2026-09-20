parser grammar SparkSqlParser;

options { tokenVocab=SparkSqlLexer; }

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
    | analyzeStatement SEMICOLON?
    | cacheStatement SEMICOLON?
    | showStatement SEMICOLON?
    | setStatement SEMICOLON?
    | dropTableStatement SEMICOLON?
    | alterTableStatement SEMICOLON?
    | updateStatement SEMICOLON?
    | deleteStatement SEMICOLON?
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
    : KW_WITH cteDefinition (COMMA cteDefinition)* queryExpression
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
      orderByClause? limitClause? windowClause? pivotClause?
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
    : STAR
    | expression (alias)?
    ;

// ============================================
// FROM 子句 - 表引用提取核心
// ============================================

fromClause
    : KW_FROM tableReference (COMMA tableReference)*
    ;

// 表引用 - 支持单表、子查询、JOIN、LATERAL VIEW
// ============================================

tableReference
    : tablePath (alias)?
    | LPAREN queryExpression RPAREN (alias)?
    | tableReference joinType? KW_JOIN tablePath (alias)? KW_ON expression
    | tableReference joinType? KW_JOIN LPAREN queryExpression RPAREN (alias)? KW_ON expression
    | KW_LATERAL_VIEW lateralFunction
    ;

lateralFunction
    : functionName LPAREN columnRef (COMMA uid)* RPAREN
    ;

joinType
    : KW_LEFT KW_OUTER?
    | KW_RIGHT KW_OUTER?
    | KW_FULL KW_OUTER?
    | KW_INNER
    | KW_CROSS
    | KW_SEMI
    | KW_ANTI
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
    : expression (KW_ASC | KW_DESC)? (KW_NULLS (KW_FIRST | KW_LAST))?
    ;

limitClause
    : KW_LIMIT NUMBER
    | KW_LIMIT NUMBER COMMA NUMBER
    ;

windowClause
    : KW_WINDOW windowSpecification (COMMA windowSpecification)*
    ;

windowSpecification
    : uid KW_AS windowName KW_OVER LPAREN windowDefinition RPAREN
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

pivotClause
    : KW_PIVOT LPAREN functionName KW_IN columnList (alias)? RPAREN
    | KW_UNPIVOT LPAREN uid KW_IN columnList RPAREN
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
    : expression (PLUS | MINUS | MULT | DIV | MOD) expression
    | expression (EQ | NEQ | LT | GT | LTE | GTE | CONCAT | ARROW) expression
    | expression KW_AND expression
    | expression KW_OR expression
    | KW_NOT expression
    | expression KW_IS KW_NOT? (KW_NULL | KW_TRUE | KW_FALSE)
    | expression KW_NOT? KW_IN LPAREN expression (COMMA expression)* RPAREN
    | expression KW_NOT? KW_IN LPAREN queryExpression RPAREN
    | expression KW_NOT? KW_BETWEEN expression KW_AND expression
    | primaryExpression
    | functionCall
    | castExpression
    | arrayAccessExpression
    | MULT
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

betweenExpression
    : expression KW_NOT? KW_BETWEEN expression KW_AND expression
    ;

inExpression
    : expression (KW_NOT? KW_IN LPAREN expression (COMMA expression)* RPAREN 
                 | LPAREN queryExpression RPAREN)
    ;

arrayAccessExpression
    : columnRef LBRACKET expression RBRACKET
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
    | KW_TRUE
    | KW_FALSE
    | KW_NULL
    ;

dataType
    : typeName
    | typeName LPAREN NUMBER (COMMA NUMBER)? RPAREN
    | KW_ARRAY LT dataType GT
    | KW_MAP LT dataType COMMA dataType GT
    | KW_STRUCT LT structField (COMMA structField)* GT
    ;

structField
    : uid COLON dataType
    ;

typeName
    : KW_INT | KW_BIGINT | KW_SMALLINT | KW_TINYINT
    | KW_DECIMAL | KW_STRING | KW_CHAR | KW_BOOLEAN
    | KW_DATE | KW_TIME | KW_TIMESTAMP
    | KW_BINARY | KW_VARBINARY
    | KW_ARRAY | KW_MAP | KW_ROW
    | KW_STRUCT | KW_VARIANT | KW_BYTEARRAY
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
    : KW_CREATE (KW_TEMPORARY | KW_TEMP)? KW_TABLE (KW_IF KW_NOT KW_EXISTS)? tablePath 
      LPAREN columnDefinition (COMMA columnDefinition)* RPAREN tableProperties?
    | KW_CREATE (KW_TEMPORARY | KW_TEMP)? KW_TABLE (KW_IF KW_NOT KW_EXISTS)? tablePath KW_AS queryExpression
    | KW_CREATE KW_TEMPORARY KW_VIEW tablePath KW_AS queryExpression
    ;

columnDefinition
    : uid dataType columnConstraint*
    ;

columnConstraint
    : KW_PRIMARY KW_KEY
    | KW_NOT KW_NULL
    | KW_NULL
    | KW_DEFAULT expression
    | KW_COMMENT STRING
    ;

tableProperties
    : KW_WITH LPAREN tableProperty (COMMA tableProperty)* RPAREN
    | KW_TBLPROPERTIES LPAREN tableProperty (COMMA tableProperty)* RPAREN
    ;

tableProperty
    : uid EQ expression
    ;

// ============================================
// ANALYZE 语句
// ============================================

analyzeStatement
    : KW_ANALYZE KW_TABLE tablePath KW_COMPUTE KW_STATISTICS
    | KW_ANALYZE KW_TABLE tablePath KW_COLUMN columnList KW_COMPUTE KW_STATISTICS
    ;

// ============================================
// CACHE/UNCACHE 语句
// ============================================

cacheStatement
    : KW_CACHE KW_TABLE tablePath
    | KW_UNCACHE KW_TABLE tablePath
    ;

// ============================================
// SHOW 语句
// ============================================

showStatement
    : KW_SHOW KW_TABLES
    | KW_SHOW KW_TABLE tableNamePattern?
    | KW_SHOW KW_PARTITIONS tablePath
    | KW_SHOW KW_COLUMNS KW_IN tablePath
    | KW_SHOW KW_FUNCTIONS pattern?
    | KW_SHOW KW_DATABASES pattern?
    | KW_SHOW KW_SCHEMAS pattern?
    | KW_SHOW KW_CATALOGS
    ;

tableNamePattern
    : KW_LIKE_STRING pattern?
    ;

// pattern 已在上文定义
pattern
    : STRING
    ;

// ============================================
// SET/UNSET 语句
// ============================================

setStatement
    : KW_SET propertyExpression
    | KW_UNSET propertyExpression
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

// ============================================
// UPDATE 语句（Spark 3.x+）
// ============================================

updateStatement
    : KW_UPDATE tablePath (KW_AS alias)?
      KW_SET assignmentList
      (KW_WHERE expression)?
    ;

assignmentList
    : assignment (COMMA assignment)*
    ;

assignment
    : uid EQ expression
    ;

// ============================================
// DELETE 语句（Spark 3.x+）
// ============================================

deleteStatement
    : KW_DELETE KW_FROM tablePath (KW_AS alias)?
      (KW_WHERE expression)?
    ;

propertyExpression
    : uid (DOT uid)* EQ expression
    ;
