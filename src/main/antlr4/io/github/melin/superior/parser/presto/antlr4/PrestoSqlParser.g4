parser grammar PrestoSqlParser;

options { tokenVocab=PrestoSqlLexer; }

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
      (LPAREN columnNameList RPAREN)? (KW_VALUES valuesTuple (COMMA valuesTuple)* | queryExpression)
    ;

valuesTuple
    : LPAREN (expression (COMMA expression)*)? RPAREN
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
    : selectClause fromClause? whereClause? groupByClause? havingClause? 
      orderByClause? limitClause? windowClause?
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

tableReference
    : tablePath (alias)?
    | LPAREN queryExpression RPAREN (alias)?
    | tableReference joinType? KW_JOIN tablePath (alias)? KW_ON expression
    | tableReference joinType? KW_JOIN LPAREN queryExpression RPAREN (alias)? KW_ON expression
    | KW_UNNEST LPAREN expression (COMMA expression)* RPAREN (KW_WITH KW_ORDINALITY)? (aliasWithColumns | alias)?
    | tableReference joinType? KW_JOIN KW_UNNEST LPAREN expression (COMMA expression)* RPAREN
      (KW_WITH KW_ORDINALITY)? (aliasWithColumns | alias)? (KW_ON KW_TRUE)?
    | tableReference KW_TABLESAMPLE (KW_BERNOULLI | KW_SYSTEM) LPAREN NUMBER RPAREN (alias)?
    ;

aliasWithColumns
    : KW_AS? uid LPAREN uid (COMMA uid)* RPAREN
    ;

joinType
    : KW_LEFT KW_OUTER?
    | KW_RIGHT KW_OUTER?
    | KW_FULL KW_OUTER?
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
    : expression (KW_ASC | KW_DESC)?
    ;

limitClause
    : KW_LIMIT NUMBER
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
    : expression LBRACKET expression RBRACKET
    | expression DOT uid
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
    | KW_EXISTS LPAREN queryExpression RPAREN
    | LPAREN queryExpression RPAREN
    | KW_INTERVAL expression timeUnit (KW_TO timeUnit)?
    | KW_EXTRACT LPAREN timeUnit KW_FROM expression RPAREN
    | KW_ARRAY LBRACKET expression (COMMA expression)* RBRACKET
    | KW_ROW LPAREN (expression (COMMA expression)*)? RPAREN
    | primaryExpression
    | functionCall
    | castExpression
    | caseExpression
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

caseExpression
    : KW_CASE expression? whenClause+ (KW_ELSE elseClause)? KW_END
    | KW_CASE KW_WHEN condition KW_THEN result (KW_WHEN condition KW_THEN result)+ (KW_ELSE elseClause)? KW_END
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
    | KW_ARRAY LT dataType GT
    | KW_MAP LT dataType COMMA dataType GT
    | KW_ROW LT rowType GT
    // Trino 的行类型写作 ROW(a INTEGER, b VARCHAR)，圆号形式才是标准写法
    | KW_ROW LPAREN rowType RPAREN
    ;

// Presto 的具名字段写作 ROW(a INTEGER, b VARCHAR)，也允许纯位置 ROW(INTEGER, VARCHAR)
rowType
    : rowField (COMMA rowField)*
    ;

rowField
    : uid dataType
    | dataType
    ;

// INTERVAL '1' DAY / EXTRACT(DAY FROM ts) 的时间单位
timeUnit
    : KW_SECOND | KW_MINUTE | KW_HOUR | KW_DAY | KW_WEEK | KW_MONTH | KW_QUARTER | KW_YEAR
    ;

typeName
    : KW_INT | KW_BIGINT | KW_SMALLINT | KW_TINYINT
    | KW_DECIMAL | KW_STRING | KW_BOOLEAN
    | KW_CHAR | KW_CHARACTER KW_VARYING
    | KW_DATE | KW_TIME | KW_TIMESTAMP
    | KW_BINARY | KW_VARBINARY
    | KW_DOUBLE | KW_FLOAT | KW_REAL
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
    | KW_DEFAULT   // default 数据库名等场景下关键字可作标识符
    | timeUnit     // day/month/year 等在 Presto 里非保留字，仍可作列名
    | KW_GROUPING
    | KW_SETS
    | KW_ORDINALITY
    | KW_EXTRACT
    ;

alias
    : KW_AS? uid
    ;

// 函数名一律走 uid；只有同时充当子句关键字的词（IF/LEFT/RIGHT）需显式放行
functionName
    : uid
    | KW_IF
    | KW_LEFT
    | KW_RIGHT
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
    | KW_CREATE (KW_TEMPORARY | KW_TEMP)? KW_VIEW (KW_IF KW_NOT KW_EXISTS)? tablePath KW_AS queryExpression
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
    ;

tableProperty
    : (uid | STRING) EQ expression
    ;

// ============================================
// EXPLAIN 语句
// ============================================

explainStatement
    : KW_EXPLAIN (KW_ANALYZE | KW_COST | KW_DISTRIBUTION)? statementType
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
// UPDATE 语句（Presto）
// ============================================

updateStatement
    : KW_UPDATE tablePath (KW_AS alias)?
      KW_SET assignmentList
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
    : KW_DELETE KW_FROM tablePath (KW_AS alias)?
      (KW_WHERE condition)?
    ;

statementType
    : KW_QUERY
    | KW_DELETE
    | KW_UPDATE
    | KW_INSERT
    ;
