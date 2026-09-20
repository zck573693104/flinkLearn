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
    | truncateStatement SEMICOLON?
    | alterTableStatement SEMICOLON?
    | updateStatement SEMICOLON?
    | deleteStatement SEMICOLON?
    | useStatement SEMICOLON?
    ;

useStatement
    : KW_USE tablePath
    ;

// ============================================
// INSERT 语句 - 血缘提取核心
// ============================================

insertStatement
    // INSERT OVERWRITE [LOCAL] DIRECTORY '/path' [STORED AS fmt | USING fmt] SELECT ...
    // 无目标表，但 SELECT 的输入表仍是真实上游
    : KW_INSERT KW_OVERWRITE KW_LOCAL? KW_DIRECTORY STRING
      (KW_STORED KW_AS uid | KW_USING uid)? queryExpression
    | KW_INSERT (KW_INTO | KW_OVERWRITE)? KW_TABLE? tablePath
      (KW_PARTITION LPAREN partitionSpec (COMMA partitionSpec)* RPAREN)?
      (LPAREN columnNameList RPAREN)? (KW_VALUES valuesTuple (COMMA valuesTuple)* | queryExpression)
    ;

valuesTuple
    : LPAREN (expression (COMMA expression)*)? RPAREN
    ;

// Hive 风格分区说明：PARTITION (dt='2024-01-01') 或 PARTITION (dt)
partitionSpec
    : uid (EQ (STRING | NUMBER | DOUBLE_QUOTED_STRING | uid))?
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
    : KW_WITH KW_RECURSIVE? cteDefinition (COMMA cteDefinition)* (insertStatement | queryExpression)
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
      qualifyClause? distributeClause? orderByClause? distributeClause? limitClause? windowClause? pivotClause?
      (KW_UNION (KW_DISTINCT | KW_ALL)? queryExpression
       | KW_INTERSECT (KW_DISTINCT | KW_ALL)? queryExpression
       | KW_EXCEPT (KW_DISTINCT | KW_ALL)? queryExpression)*
    ;

// Spark 3.5+: QUALIFY 按窗口函数结果过滤行
qualifyClause
    : KW_QUALIFY expression
    ;

// Hive/Spark 特有：DISTRIBUTE BY / CLUSTER BY / SORT BY 控制数据分发与排序
distributeClause
    : (KW_DISTRIBUTE | KW_CLUSTER | KW_SORT) KW_BY columnList
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

// 表引用 - 支持单表、子查询、JOIN、LATERAL VIEW
// ============================================

tableReference
    : tablePath (alias)?
    | LPAREN queryExpression RPAREN (alias)?
    | tableReference joinType? KW_JOIN tablePath (alias)? KW_ON expression
    | tableReference joinType? KW_JOIN LPAREN queryExpression RPAREN (alias)? KW_ON expression
    | tableReference lateralView
    | tableReference KW_TABLESAMPLE LPAREN NUMBER (KW_PERCENT | KW_ROWS)? RPAREN (alias)?
    ;

// Hive/Spark 特有：LATERAL VIEW [OUTER] udf(...) [表别名] AS 列别名[, 列别名...]
lateralView
    : KW_LATERAL KW_VIEW KW_OUTER? functionName LPAREN (expression (COMMA expression)*)? RPAREN
      uid? KW_AS uid (COMMA uid)*
    ;

joinType
    : KW_LEFT KW_OUTER?
    | KW_LEFT KW_SEMI
    | KW_LEFT KW_ANTI
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
    : KW_GROUP KW_BY groupingElement (COMMA groupingElement)*
    ;

// GROUP BY a, GROUPING SETS ((a,b), ()) / CUBE(...) / ROLLUP(...)
groupingElement
    : KW_GROUPING KW_SETS LPAREN groupingSet (COMMA groupingSet)* RPAREN
    | (KW_CUBE | KW_ROLLUP) LPAREN expression (COMMA expression)* RPAREN
    | expression (alias)?
    ;

groupingSet
    : LPAREN (expression (COMMA expression)*)? RPAREN
    | expression
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
    : KW_PIVOT LPAREN expression (COMMA expression)* KW_FOR uid KW_IN LPAREN pivotValue (COMMA pivotValue)* RPAREN RPAREN (alias)?
    | KW_UNPIVOT LPAREN uid (COMMA uid)* KW_IN columnList KW_FOR uid KW_IN LPAREN pivotValue (COMMA pivotValue)* RPAREN RPAREN (alias)?
    ;

pivotValue
    : (literal | uid | MULT) (KW_AS uid)?
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
    // first_value(v) IGNORE NULLS OVER (...)：先绑定 IGNORE/RESPECT，再套窗口
    | expression (KW_IGNORE | KW_RESPECT) KW_NULLS
    | expression KW_OVER LPAREN windowDefinition RPAREN
    | expression KW_NOT? KW_LIKE expression
    | (PLUS | MINUS) expression
    | expression (PLUS | MINUS | MULT | DIV | MOD) expression
    | expression (EQ | NEQ | LT | GT | LTE | GTE | CONCAT | ARROW) expression
    | expression KW_AND expression
    | expression KW_OR expression
    | KW_NOT expression
    | expression KW_IS KW_NOT? (KW_NULL | KW_TRUE | KW_FALSE)
    | expression KW_NOT? KW_IN LPAREN expression (COMMA expression)* RPAREN
    | expression KW_NOT? KW_IN LPAREN queryExpression RPAREN
    | expression KW_NOT? KW_BETWEEN expression KW_AND expression
    // INTERVAL '1' DAY / INTERVAL '5' MINUTE
    | KW_INTERVAL expression timeUnit
    | KW_EXISTS LPAREN queryExpression RPAREN
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
    | KW_BINARY | KW_VARBINARY | KW_DOUBLE | KW_FLOAT
    | KW_ARRAY | KW_MAP | KW_ROW
    | KW_STRUCT | KW_VARIANT | KW_BYTEARRAY
    ;

// INTERVAL 与日期函数的时间单位
timeUnit
    : KW_SECOND | KW_MINUTE | KW_HOUR | KW_DAY | KW_MONTH | KW_QUARTER | KW_YEAR
    ;

// ============================================
// 辅助规则
// ============================================

uid
    : UID
    | QUOTED_UID
    | KW_DEFAULT   // default 数据库名等场景下关键字可作标识符
    | timeUnit     // day/hour/second 等常作列名，不作保留字
    ;

alias
    : KW_AS? uid
    ;

// 函数名一律走 uid；只有同时充当子句关键字的词（IF/LEFT/RIGHT/FIRST/LAST）需显式放行
functionName
    : uid
    | KW_IF
    | KW_LEFT
    | KW_RIGHT
    | KW_FIRST
    | KW_LAST
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
    : (uid | STRING) EQ expression
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

// TRUNCATE TABLE x [PARTITION (dt='...')]：只清空数据，不产生血缘
truncateStatement
    : KW_TRUNCATE KW_TABLE tablePath
      (KW_PARTITION LPAREN partitionSpec (COMMA partitionSpec)* RPAREN)?
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
