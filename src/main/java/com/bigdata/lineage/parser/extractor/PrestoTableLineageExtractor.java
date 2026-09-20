package com.bigdata.lineage.parser.extractor;

import com.bigdata.lineage.parser.model.TableLineage;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;
import io.github.melin.superior.parser.presto.antlr4.PrestoSqlLexer;
import io.github.melin.superior.parser.presto.antlr4.PrestoSqlParser;
import io.github.melin.superior.parser.presto.antlr4.PrestoSqlParserBaseVisitor;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Presto SQL 表级血缘提取器 - 基于 ANTLR4
 * 
 * 完全独立的实现，支持 Presto SQL 特有语法
 */
public class PrestoTableLineageExtractor {
    
    private static final double DEFAULT_CONFIDENCE = 0.95;
    
    /** 语法有错误时血缘只是错误恢复的副产品，必须让下游能区分出来 */
    private static final double PARSE_ERROR_CONFIDENCE = 0.5;
    
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
            // 语法错误计数：ANTLR 容错模式会产生部分解析树，必须标记结果不可信
            final AtomicInteger syntaxErrors = new AtomicInteger();
            BaseErrorListener errorListener = new BaseErrorListener() {
                @Override
                public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                                        int line, int charPositionInLine, String msg, RecognitionException e) {
                    syntaxErrors.incrementAndGet();
                }
            };
            
            // 创建词法分析器（错误进入计数器而非控制台）
            CharStream charStream = CharStreams.fromString(sql);
            PrestoSqlLexer lexer = new PrestoSqlLexer(charStream);
            lexer.removeErrorListeners();
            lexer.addErrorListener(errorListener);
            
            // 创建解析器（容错模式：存在语法错误时仍使用部分解析结果）
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            PrestoSqlParser parser = new PrestoSqlParser(tokens);
            parser.removeErrorListeners();
            parser.addErrorListener(errorListener);
            
            PrestoSqlParser.SqlStatementsContext stmtCtx = parser.sqlStatements();
            
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
                    .parseError(syntaxErrors.get() > 0)
                    .originalSql(sql)
                    .confidence(syntaxErrors.get() > 0 ? PARSE_ERROR_CONFIDENCE : DEFAULT_CONFIDENCE)
                    .build();
                    
        } catch (Exception e) {
            throw new RuntimeException("Presto SQL 血缘提取失败", e);
        }
    }
    
    /**
     * ANTLR4 Visitor 实现 - 遍历 AST 并提取血缘信息
     */
    private static class LineageVisitor extends PrestoSqlParserBaseVisitor<Void> {
        
        private String targetTable;
        private Set<String> sourceTables = new HashSet<>();
        private Set<String> cteNames = new HashSet<>();
        private String processType = "SELECT";
        private String insertMode = "INTO";
        private boolean hasCte = false;
        private boolean hasTemporalJoin = false;
        private boolean hasWindowFunc = false;
        
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
            
            // 先注册所有 CTE 名称（CTE 是临时结果集，不计入物理源表）
            for (PrestoSqlParser.CteDefinitionContext cteCtx : ctx.cteDefinition()) {
                if (cteCtx.cteName() != null) {
                    cteNames.add(cteCtx.cteName().getText().toLowerCase());
                }
            }
            
            // 访问所有 CTE 定义
            for (PrestoSqlParser.CteDefinitionContext cteCtx : ctx.cteDefinition()) {
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
        public Void visitCreateTableStatement(PrestoSqlParser.CreateTableStatementContext ctx) {
            if (ctx.tablePath() == null) {
                return null;
            }
            targetTable = extractTableName(ctx.tablePath());
            
            if (ctx.queryExpression() != null) {
                // CREATE TABLE / VIEW ... AS SELECT：源表来自查询
                processType = ctx.KW_VIEW() != null ? "CREATE_VIEW" : "CTAS";
                visit(ctx.queryExpression());
            } else {
                // 纯 DDL（无血缘）
                processType = "CREATE_TABLE";
            }
            
            return null;
        }
        
        // visitQueryExpression 不覆写：使用基类 visitChildren 完整下钻
        // SELECT/WHERE 中的子查询（IN、标量子查询）同样产生表依赖
        
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
            // JOIN 分支：先递归处理左侧表引用（含左侧 JOIN ... UNNEST 场景）
            if (ctx.tableReference() != null) {
                visit(ctx.tableReference());
            }
            
            // 处理 UNNEST：不产生新表依赖
            if (ctx.KW_UNNEST() != null) {
                return null;
            }
            
            // 处理表路径
            if (ctx.tablePath() != null) {
                addSourceTable(extractTableName(ctx.tablePath()));
            }
            
            // 处理嵌套子查询
            if (ctx.queryExpression() != null) {
                visit(ctx.queryExpression());
            }
            
            // JOIN ON 条件中的子查询（如 ON x.id IN (SELECT ...)）同样产生表依赖
            for (PrestoSqlParser.ExpressionContext exprCtx : ctx.expression()) {
                visit(exprCtx);
            }
            
            return null;
        }
        
        /**
         * 添加源表（排除 CTE 临时名称，SQL 标识符大小写不敏感）
         */
        private void addSourceTable(String tableName) {
            if (tableName == null || tableName.isEmpty()
                    || cteNames.contains(tableName.toLowerCase())) {
                return;
            }
            sourceTables.add(tableName);
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
         * 判断是否为窗口函数
         */
        private boolean isWindowFunction(String funcName) {
            return funcName != null && WINDOW_FUNCTIONS.contains(funcName);
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
