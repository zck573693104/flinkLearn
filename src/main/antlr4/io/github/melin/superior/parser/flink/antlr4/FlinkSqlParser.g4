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
    | truncateStatement SEMICOLON?
    | alterTableStatement SEMICOLON?
    | setStatement SEMICOLON?
    | useStatement SEMICOLON?
    ;

useStatement
    : KW_USE tablePath
    ;

// SQL Client 的会话参数语句：SET 'k' = 'v' / SET k = 'v' / SET ('k'='v')
// 属性键可能含点号和短横线（table.sql-dialect），且首段可以是关键字（table）
setStatement
    : KW_SET (STRING (EQ)? expression | propertyKey EQ expression
             | LPAREN tableProperty (COMMA tableProperty)* RPAREN)?
    ;

propertyKey
    : (uid | KW_TABLE) ((DOT | MINUS) (uid | KW_TABLE))*
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
    | tvfFunction (alias)?
    | KW_TABLE LPAREN tvfFunction RPAREN (alias)?
    | tableReference joinType? KW_JOIN tablePath temporalClause? (alias)? KW_ON expression
    | tableReference joinType? KW_JOIN LPAREN queryExpression RPAREN (alias)? KW_ON expression
    | tableReference joinType? KW_JOIN KW_UNNEST LPAREN expression RPAREN (aliasWithColumns | alias)?
    ;

// 处理时间/事件时间时态表 JOIN：JOIN dim FOR SYSTEM_TIME AS OF probe.proctime
temporalClause
    : KW_FOR KW_SYSTEM_TIME KW_AS KW_OF expression
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
    | expression KW_OVER LPAREN windowDefinition RPAREN
    | expression KW_NOT? KW_LIKE expression
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
    | KW_EXISTS LPAREN queryExpression RPAREN
    // 数组/行构造器：ARRAY[1, 2]、ROW(1, 'a')
    | KW_ARRAY LBRACKET expression (COMMA expression)* RBRACKET
    | KW_ROW LPAREN expression (COMMA expression)+ RPAREN
    | LPAREN queryExpression RPAREN
    | KW_INTERVAL expression timeUnit
    | caseExpression
    | primaryExpression
    | functionCall
    | castExpression
    | MULT
    ;

// INTERVAL '5' MINUTE 的时间单位（TVF 窗口大小、WATERMARK 延迟）
timeUnit
    : KW_DAY | KW_HOUR | KW_MINUTE | KW_MONTH | KW_QUARTER | KW_SECOND | KW_YEAR
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
    | KW_ROW LT rowField (COMMA rowField)* GT
    ;

// Flink 具名字段写作 ROW<a TIMESTAMP, b STRING>（名字与类型之间没有冒号）
rowField
    : uid dataType
    ;

typeName
    : KW_INT | KW_BIGINT | KW_SMALLINT | KW_TINYINT
    | KW_DECIMAL | KW_STRING | KW_BOOLEAN
    | KW_CHAR | KW_CHARACTER KW_VARYING
    | KW_DATE | KW_TIME | KW_TIMESTAMP
    | KW_BINARY | KW_VARBINARY
    | KW_DOUBLE | KW_FLOAT
    | KW_ARRAY | KW_MAP | KW_ROW
    ;

// ============================================
// 辅助规则
// ============================================

uid
    : UID
    | QUOTED_UID
    | KW_DEFAULT   // default 数据库名等场景下关键字可作标识符
    | timeUnit     // year/month/day 等在 Flink 里非保留字，仍可作列名
    | KW_OF
    | KW_SYSTEM_TIME
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
// 首个实参是输入表：旧式 TUMBLE(t, ts, INTERVAL '5' MINUTE)
// 与新式 TUMBLE(TABLE t, DESCRIPTOR(ts), INTERVAL '5' MINUTE)
tvfFunction
    : functionName LPAREN KW_TABLE? tablePath (COMMA expression)* RPAREN
    ;

// 窗口定义（用于 ROW_NUMBER, RANK 等窗口函数）
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
      LPAREN tableElement (COMMA tableElement)* RPAREN tableOption*
    | KW_CREATE (KW_TEMPORARY | KW_TEMP)? KW_TABLE (KW_IF KW_NOT KW_EXISTS)? tablePath
      (LPAREN tableElement (COMMA tableElement)* RPAREN)? KW_AS queryExpression
    | KW_CREATE (KW_TEMPORARY | KW_TEMP)? KW_VIEW (KW_IF KW_NOT KW_EXISTS)? tablePath KW_AS queryExpression
    ;

// 列定义与表级约束（WATERMARK / PRIMARY KEY）混写在同一个括号列表里
tableElement
    : columnDefinition
    | watermarkSpec
    | constraintSpec
    ;

// WATERMARK FOR rowtime_col AS rowtime_col - INTERVAL '5' SECOND
watermarkSpec
    : KW_WATERMARK KW_FOR uid KW_AS expression
    ;

// PRIMARY KEY (id, dt) NOT ENFORCED：Flink upsert 表的声明式主键
constraintSpec
    : KW_PRIMARY KW_KEY LPAREN uid (COMMA uid)* RPAREN (KW_NOT KW_ENFORCED)?
    ;

// Hive 表选项：Flink 的 Hive catalog 与 SQL Client 脚本会直接写这些子句
tableOption
    : tableProperties
    | KW_PARTITIONED KW_BY LPAREN partitionField (COMMA partitionField)* RPAREN
    | KW_STORED KW_AS uid
    ;

// PARTITIONED BY (dt STRING) 或仅写列名 PARTITIONED BY (dt)
partitionField
    : uid dataType?
    ;

columnDefinition
    : uid dataType columnConstraint*
    | uid KW_AS expression   // 计算列：proc_ts AS PROCTIME()
    ;

columnConstraint
    : KW_PRIMARY KW_KEY (KW_NOT KW_ENFORCED)?
    | KW_NOT KW_NULL
    | KW_NULL
    | KW_DEFAULT expression
    | KW_COMMENT STRING
    | KW_METADATA (KW_FROM STRING)? KW_VIRTUAL?
    ;

tableProperties
    : KW_WITH LPAREN tableProperty (COMMA tableProperty)* RPAREN
    | KW_TBLPROPERTIES LPAREN tableProperty (COMMA tableProperty)* RPAREN
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

// TRUNCATE TABLE 只清空数据，无上下游数据流，因此不产生血缘
truncateStatement
    : KW_TRUNCATE KW_TABLE tablePath
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
