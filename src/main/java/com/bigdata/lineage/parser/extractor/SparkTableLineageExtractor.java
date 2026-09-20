package com.bigdata.lineage.parser.extractor;

import com.bigdata.lineage.parser.model.TableLineage;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;
import io.github.melin.superior.parser.spark.antlr4.SparkSqlLexer;
import io.github.melin.superior.parser.spark.antlr4.SparkSqlParser;
import io.github.melin.superior.parser.spark.antlr4.SparkSqlParserBaseVisitor;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Spark SQL 表级血缘提取器 - 基于 ANTLR4
 * 
 * 完全独立的实现，支持 Spark SQL 特有语法
 */
public class SparkTableLineageExtractor {
    
    private static final double DEFAULT_CONFIDENCE = 0.95;
    
    private static final Set<String> WINDOW_FUNCTIONS = new HashSet<>(Arrays.asList(
            "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE", "PERCENT_RANK",
            "CUME_DIST", "FIRST_VALUE", "LAST_VALUE", "LAG", "LEAD",
            "NTH_VALUE"
    ));
    
    /**
     * 从 SQL 语句提取表级血缘
     */
    public TableLineage extractFromSql(String sql) {
        try {
            // 创建词法分析器（静默错误）
            CharStream charStream = CharStreams.fromString(sql);
            SparkSqlLexer lexer = new SparkSqlLexer(charStream);
            lexer.removeErrorListeners();
            
            // 创建解析器（容错模式：存在语法错误时仍使用部分解析结果）
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            SparkSqlParser parser = new SparkSqlParser(tokens);
            parser.removeErrorListeners();
            
            SparkSqlParser.SqlStatementsContext stmtCtx = parser.sqlStatements();
            
            // 创建血缘提取访问者
            LineageVisitor visitor = new LineageVisitor();
            visitor.visit(stmtCtx);
            
            // 构建血缘关系
            return TableLineage.builder()
                    .targetTable(visitor.getTargetTable())
                    .sourceTables(visitor.getSourceTables())
                    .processType(visitor.getProcessType())
                    .insertMode(visitor.getInsertMode())
                    .hasCte(visitor.hasCte())
                    .hasTemporalJoin(visitor.hasTemporalJoin())
                    .hasWindowFunc(visitor.hasWindowFunc())
                    .originalSql(sql)
                    .confidence(DEFAULT_CONFIDENCE)
                    .build();
                    
        } catch (Exception e) {
            throw new RuntimeException("Spark SQL 血缘提取失败", e);
        }
    }
    
    /**
     * ANTLR4 Visitor 实现 - 遍历 AST 并提取血缘信息
     */
    private static class LineageVisitor extends SparkSqlParserBaseVisitor<Void> {
        
        private String targetTable;
        private Set<String> sourceTables = new HashSet<>();
        private Set<String> cteNames = new HashSet<>();
        private String processType = "SELECT";
        private String insertMode = "INTO";
        private boolean hasCte = false;
        private boolean hasTemporalJoin = false;
        private boolean hasWindowFunc = false;
        
        @Override
        public Void visitInsertStatement(SparkSqlParser.InsertStatementContext ctx) {
            // 提取目标表
            if (ctx.tablePath() != null) {
                targetTable = extractTableName(ctx.tablePath());
            }
            
            // 确定插入模式
            if (ctx.KW_INTO() != null) {
                insertMode = "INTO";
            } else if (ctx.KW_OVERWRITE() != null) {
                insertMode = "OVERWRITE";
            }
            
            processType = "INSERT";
            
            // 继续访问查询表达式以提取源表
            if (ctx.queryExpression() != null) {
                visit(ctx.queryExpression());
            }
            
            return null;
        }
        
        @Override
        public Void visitCteStatement(SparkSqlParser.CteStatementContext ctx) {
            hasCte = true;
            
            // 先注册所有 CTE 名称（CTE 是临时结果集，不计入物理源表）
            for (SparkSqlParser.CteDefinitionContext cteCtx : ctx.cteDefinition()) {
                if (cteCtx.cteName() != null) {
                    cteNames.add(cteCtx.cteName().getText());
                }
            }
            
            // 访问所有 CTE 定义
            for (SparkSqlParser.CteDefinitionContext cteCtx : ctx.cteDefinition()) {
                visit(cteCtx);
            }
            
            // 访问主查询（WITH ... INSERT INTO 或 WITH ... SELECT）
            if (ctx.insertStatement() != null) {
                visit(ctx.insertStatement());
            } else if (ctx.queryExpression() != null) {
                visit(ctx.queryExpression());
            }
            
            return null;
        }
        
        @Override
        public Void visitCreateTableStatement(SparkSqlParser.CreateTableStatementContext ctx) {
            // CTAS: CREATE TABLE target AS SELECT ... FROM source
            if (ctx.tablePath() != null && ctx.queryExpression() != null) {
                targetTable = extractTableName(ctx.tablePath());
                processType = "CTAS";
                
                // 提取源表
                visit(ctx.queryExpression());
            }
            // CREATE VIEW
            else if (ctx.KW_TEMPORARY() != null && ctx.KW_VIEW() != null && ctx.tablePath() != null && ctx.queryExpression() != null) {
                targetTable = extractTableName(ctx.tablePath());
                processType = "CREATE_VIEW";
                
                // 提取源表
                visit(ctx.queryExpression());
            }
            // CREATE TABLE DDL（无血缘）
            else if (ctx.tablePath() != null) {
                targetTable = extractTableName(ctx.tablePath());
                processType = "CREATE_TABLE";
            }
            
            return null;
        }
        
        @Override
        public Void visitQueryExpression(SparkSqlParser.QueryExpressionContext ctx) {
            // 访问 FROM 子句以提取表
            if (ctx.fromClause() != null) {
                visit(ctx.fromClause());
            }
            
            // 访问集合操作（UNION/INTERSECT/EXCEPT）右侧子查询
            for (SparkSqlParser.QueryExpressionContext childCtx : ctx.queryExpression()) {
                visit(childCtx);
            }
            
            return null;
        }
        
        @Override
        public Void visitFromClause(SparkSqlParser.FromClauseContext ctx) {
            // 访问所有表引用
            for (SparkSqlParser.TableReferenceContext refCtx : ctx.tableReference()) {
                visitTableReference(refCtx);
            }
            
            return null;
        }
        
        @Override
        public Void visitTableReference(SparkSqlParser.TableReferenceContext ctx) {
            // 处理 LATERAL VIEW
            if (ctx.lateralFunction() != null) {
                visitLateralFunction(ctx.lateralFunction());
                return null;
            }
            
            // JOIN 分支：先递归处理左侧表引用
            if (ctx.tableReference() != null) {
                visit(ctx.tableReference());
            }
            
            // 处理表路径
            if (ctx.tablePath() != null) {
                addSourceTable(extractTableName(ctx.tablePath()));
            }
            
            // 处理嵌套子查询
            if (ctx.queryExpression() != null) {
                visit(ctx.queryExpression());
            }
            
            return null;
        }
        
        /**
         * 添加源表（排除 CTE 临时名称）
         */
        private void addSourceTable(String tableName) {
            if (tableName == null || tableName.isEmpty() || cteNames.contains(tableName)) {
                return;
            }
            sourceTables.add(tableName);
        }
        
        @Override
        public Void visitUpdateStatement(SparkSqlParser.UpdateStatementContext ctx) {
            // UPDATE target SET ... WHERE ...：目标表即被更新表
            if (ctx.tablePath() != null) {
                targetTable = extractTableName(ctx.tablePath());
            }
            processType = "UPDATE";
            
            // WHERE 条件中的子查询（如 IN (SELECT ...)）产生源表依赖
            if (ctx.expression() != null) {
                collectSubqueryTables(ctx.expression());
            }
            return null;
        }
        
        @Override
        public Void visitDeleteStatement(SparkSqlParser.DeleteStatementContext ctx) {
            // DELETE FROM target WHERE ...：目标表即被删除表
            if (ctx.tablePath() != null) {
                targetTable = extractTableName(ctx.tablePath());
            }
            processType = "DELETE";
            
            // WHERE 条件中的子查询产生源表依赖
            if (ctx.expression() != null) {
                collectSubqueryTables(ctx.expression());
            }
            return null;
        }
        
        /**
         * 递归查找子树中的查询表达式并提取源表（用于 WHERE 子查询）
         */
        private void collectSubqueryTables(ParseTree node) {
            if (node instanceof SparkSqlParser.QueryExpressionContext) {
                visit((SparkSqlParser.QueryExpressionContext) node);
                return;
            }
            for (int i = 0; i < node.getChildCount(); i++) {
                ParseTree child = node.getChild(i);
                if (child != null) {
                    collectSubqueryTables(child);
                }
            }
        }
        
        @Override
        public Void visitLateralFunction(SparkSqlParser.LateralFunctionContext ctx) {
            // LATERAL VIEW EXPLODE(...) - 展开数组/Map
            // 源表是前面的表，不产生新的表依赖
            if (ctx.columnRef() != null) {
                String columnName = extractColumnName(ctx.columnRef());
                // 记录列血缘（可选）
            }
            return null;
        }
        
        @Override
        public Void visitFunctionCall(SparkSqlParser.FunctionCallContext ctx) {
            // 检查是否为窗口函数或 TVF
            String funcName = extractFunctionName(ctx);
            if (isWindowFunction(funcName) || isTvfFunction(funcName)) {
                hasWindowFunc = true;
            }
            
            return super.visitFunctionCall(ctx);
        }
        
        /**
         * 提取表名
         */
        private String extractTableName(SparkSqlParser.TablePathContext ctx) {
            if (ctx == null) {
                return null;
            }
            
            List<String> parts = new ArrayList<>();
            for (SparkSqlParser.UidContext uidCtx : ctx.uid()) {
                parts.add(uidCtx.getText());
            }
            
            return String.join(".", parts);
        }
        
        /**
         * 提取列名
         */
        private String extractColumnName(SparkSqlParser.ColumnRefContext ctx) {
            if (ctx == null) {
                return null;
            }
            return ctx.uid().getText();
        }
        
        /**
         * 提取函数名
         */
        private String extractFunctionName(SparkSqlParser.FunctionCallContext ctx) {
            if (ctx.functionName() == null) {
                return null;
            }
            return ctx.functionName().getText().toUpperCase();
        }
        
        /**
         * 判断是否为窗口函数
         */
        private boolean isWindowFunction(String funcName) {
            return funcName != null && WINDOW_FUNCTIONS.contains(funcName);
        }
        
        /**
         * 判断是否为 TVF（Table-valued Functions）
         */
        private boolean isTvfFunction(String funcName) {
            if (funcName == null) {
                return false;
            }
            
            Set<String> tvfFuncs = new HashSet<>(Arrays.asList(
                    "EXPLODE", "INLINE", "POSEXPLODE", "JSON_TUPLE",
                    "STACK", "SLICE", "TRANSFORM"
            ));
            
            return tvfFuncs.contains(funcName.toUpperCase());
        }
        
        // Getters
        public String getTargetTable() { return targetTable; }
        public Set<String> getSourceTables() { return sourceTables; }
        public String getProcessType() { return processType; }
        public String getInsertMode() { return insertMode; }
        public boolean hasCte() { return hasCte; }
        public boolean hasTemporalJoin() { return hasTemporalJoin; }
        public boolean hasWindowFunc() { return hasWindowFunc; }
    }
}
