package com.bigdata.lineage.parser.extractor;

import com.bigdata.lineage.parser.model.TableLineage;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;
import io.github.melin.superior.parser.presto.antlr4.PrestoSqlLexer;
import io.github.melin.superior.parser.presto.antlr4.PrestoSqlParser;
import io.github.melin.superior.parser.presto.antlr4.PrestoSqlParserBaseVisitor;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Presto SQL 表级血缘提取器 - 基于 ANTLR4
 * 
 * 完全独立的实现，支持 Presto SQL 特有语法
 */
public class PrestoTableLineageExtractor {
    
    private static final double DEFAULT_CONFIDENCE = 0.95;
    
    /**
     * 从 SQL 语句提取表级血缘
     */
    public TableLineage extractFromSql(String sql) {
        try {
            // 创建词法分析器
            CharStream charStream = CharStreams.fromString(sql);
            PrestoSqlLexer lexer = new PrestoSqlLexer(charStream);
            
            // 创建解析器
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            PrestoSqlParser parser = new PrestoSqlParser(tokens);
            
            // 移除默认错误监听器
            parser.removeErrorListeners();
            
            // 添加自定义错误处理器（静默收集，不中断解析）
            final List<String> parseErrors = new ArrayList<>();
            parser.addErrorListener(new BaseErrorListener() {
                @Override
                public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
                    parseErrors.add("行 " + line + ":" + charPositionInLine + " " + msg);
                }
            });
            
            // 解析 SQL 语句（容错模式：存在语法错误时仍使用部分解析结果）
            PrestoSqlParser.SqlStatementsContext stmtCtx = parser.sqlStatements();
            
            // 创建血缘提取访问者
            LineageVisitor visitor = new LineageVisitor(sql);
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
            throw new RuntimeException("Presto SQL 血缘提取失败", e);
        }
    }
    
    /**
     * ANTLR4 Visitor 实现 - 遍历 AST 并提取血缘信息
     */
    private static class LineageVisitor extends PrestoSqlParserBaseVisitor<Void> {
        
        private final String originalSql;
        private String targetTable;
        private Set<String> sourceTables = new HashSet<>();
        private String processType = "SELECT";
        private String insertMode = "INTO";
        private boolean hasCte = false;
        private boolean hasTemporalJoin = false;
        private boolean hasWindowFunc = false;
        
        LineageVisitor(String originalSql) {
            this.originalSql = originalSql;
        }
        
        @Override
        public Void visitInsertStatement(PrestoSqlParser.InsertStatementContext ctx) {
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
        public Void visitCteStatement(PrestoSqlParser.CteStatementContext ctx) {
            hasCte = true;
            
            // 访问所有 CTE 定义
            for (PrestoSqlParser.CteDefinitionContext cteCtx : ctx.cteDefinition()) {
                visit(cteCtx);
            }
            
            // 访问主查询
            if (ctx.queryExpression() != null) {
                visit(ctx.queryExpression());
            }
            
            return null;
        }
        
        @Override
        public Void visitCreateTableStatement(PrestoSqlParser.CreateTableStatementContext ctx) {
            // CTAS: CREATE TABLE target AS SELECT ... FROM source
            if (ctx.tablePath() != null && ctx.queryExpression() != null) {
                targetTable = extractTableName(ctx.tablePath());
                processType = "CTAS";
                
                // 提取源表
                visit(ctx.queryExpression());
            }
            // CREATE VIEW
            else if (ctx.KW_VIEW() != null && ctx.tablePath() != null && ctx.queryExpression() != null) {
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
        public Void visitQueryExpression(PrestoSqlParser.QueryExpressionContext ctx) {
            // 访问 FROM 子句以提取表
            if (ctx.fromClause() != null) {
                visit(ctx.fromClause());
            }
            
            // 访问集合操作（UNION/INTERSECT/EXCEPT）右侧子查询
            for (PrestoSqlParser.QueryExpressionContext childCtx : ctx.queryExpression()) {
                visit(childCtx);
            }
            
            // 检查窗口函数
            checkForWindowFunctions();
            
            return null;
        }
        
        @Override
        public Void visitFromClause(PrestoSqlParser.FromClauseContext ctx) {
            // 访问所有表引用
            for (PrestoSqlParser.TableReferenceContext refCtx : ctx.tableReference()) {
                visitTableReference(refCtx);
            }
            
            return null;
        }
        
        @Override
        public Void visitTableReference(PrestoSqlParser.TableReferenceContext ctx) {
            // 处理 UNNEST
            if (ctx.KW_UNNEST() != null && ctx.expression() != null) {
                // UNNEST 不产生新表依赖
                return null;
            }
            
            // JOIN 分支：先递归处理左侧表引用
            if (ctx.tableReference() != null) {
                visit(ctx.tableReference());
            }
            
            // 处理表路径
            if (ctx.tablePath() != null) {
                String tableName = extractTableName(ctx.tablePath());
                if (tableName != null) {
                    sourceTables.add(tableName);
                }
            }
            
            // 处理嵌套子查询
            if (ctx.queryExpression() != null) {
                visit(ctx.queryExpression());
            }
            
            return null;
        }
        
        @Override
        public Void visitUpdateStatement(PrestoSqlParser.UpdateStatementContext ctx) {
            // UPDATE target SET ... WHERE ...：目标表即被更新表
            if (ctx.tablePath() != null) {
                targetTable = extractTableName(ctx.tablePath());
            }
            processType = "UPDATE";
            
            // WHERE 条件中的子查询（如 IN (SELECT ...)）也产生源表依赖
            if (ctx.condition() != null) {
                collectSubqueryTables(ctx.condition());
            }
            return null;
        }
        
        @Override
        public Void visitDeleteStatement(PrestoSqlParser.DeleteStatementContext ctx) {
            // DELETE FROM target WHERE ...：目标表即被删除表
            if (ctx.tablePath() != null) {
                targetTable = extractTableName(ctx.tablePath());
            }
            processType = "DELETE";
            
            // WHERE 条件中的子查询产生源表依赖
            if (ctx.condition() != null) {
                collectSubqueryTables(ctx.condition());
            }
            return null;
        }
        
        /**
         * 递归查找子树中的查询表达式并提取源表（用于 WHERE 子查询）
         */
        private void collectSubqueryTables(ParseTree node) {
            if (node instanceof PrestoSqlParser.QueryExpressionContext) {
                visit((PrestoSqlParser.QueryExpressionContext) node);
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
        public Void visitFunctionCall(PrestoSqlParser.FunctionCallContext ctx) {
            // 检查是否为窗口函数
            String funcName = extractFunctionName(ctx);
            if (isWindowFunction(funcName)) {
                hasWindowFunc = true;
            }
            
            return super.visitFunctionCall(ctx);
        }
        
        /**
         * 提取表名
         */
        private String extractTableName(PrestoSqlParser.TablePathContext ctx) {
            if (ctx == null) {
                return null;
            }
            
            List<String> parts = new ArrayList<>();
            for (PrestoSqlParser.UidContext uidCtx : ctx.uid()) {
                parts.add(uidCtx.getText());
            }
            
            return String.join(".", parts);
        }
        
        /**
         * 提取函数名
         */
        private String extractFunctionName(PrestoSqlParser.FunctionCallContext ctx) {
            if (ctx.functionName() == null) {
                return null;
            }
            return ctx.functionName().getText().toUpperCase();
        }
        
        /**
         * 检查是否有窗口函数
         */
        private void checkForWindowFunctions() {
            // 检查 WINDOW 子句
        }
        
        /**
         * 判断是否为窗口函数
         */
        private boolean isWindowFunction(String funcName) {
            if (funcName == null) {
                return false;
            }
            
            Set<String> windowFuncs = new HashSet<>(Arrays.asList(
                    "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE", "PERCENT_RANK",
                    "CUME_DIST", "FIRST_VALUE", "LAST_VALUE", "LAG", "LEAD",
                    "NTH_VALUE"
            ));
            
            return windowFuncs.contains(funcName.toUpperCase());
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
