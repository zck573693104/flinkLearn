package com.bigdata.lineage.parser.extractor;

import com.bigdata.lineage.parser.model.TableLineage;
import org.antlr.v4.runtime.*;
import io.github.melin.superior.parser.flink.antlr4.FlinkSqlLexer;
import io.github.melin.superior.parser.flink.antlr4.FlinkSqlParser;
import io.github.melin.superior.parser.flink.antlr4.FlinkSqlParserBaseVisitor;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 表级血缘提取器 - 基于 ANTLR4
 * 
 * 完全独立的实现，不依赖 Superior SQL Parser 的其他代码
 */
public class TableLineageExtractor {
    
    private static final double DEFAULT_CONFIDENCE = 0.95;
    
    private static final Set<String> WINDOW_FUNCTIONS = new HashSet<>(Arrays.asList(
            "TUMBLE", "HOP", "CUMULATE", "SESSION",
            "ROW_NUMBER", "RANK", "DENSE_RANK", "ROWNUM"
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
            FlinkSqlLexer lexer = new FlinkSqlLexer(charStream);
            lexer.removeErrorListeners();
            lexer.addErrorListener(errorListener);
            
            // 创建解析器（容错模式：存在语法错误时仍使用部分解析结果）
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            FlinkSqlParser parser = new FlinkSqlParser(tokens);
            parser.removeErrorListeners();
            parser.addErrorListener(errorListener);
            
            FlinkSqlParser.SqlStatementsContext stmtCtx = parser.sqlStatements();
            
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
                    .confidence(DEFAULT_CONFIDENCE)
                    .build();
                    
        } catch (Exception e) {
            throw new RuntimeException("血缘提取失败", e);
        }
    }
    
    /**
     * ANTLR4 Visitor 实现 - 遍历 AST 并提取血缘信息
     */
    private static class LineageVisitor extends FlinkSqlParserBaseVisitor<Void> {
        
        private String targetTable;
        private Set<String> sourceTables = new HashSet<>();
        private Set<String> cteNames = new HashSet<>();
        private String processType = "SELECT";
        private String insertMode = "INTO";
        private boolean hasCte = false;
        private boolean hasTemporalJoin = false;
        private boolean hasWindowFunc = false;
        
        @Override
        public Void visitInsertStatement(FlinkSqlParser.InsertStatementContext ctx) {
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
        public Void visitCteStatement(FlinkSqlParser.CteStatementContext ctx) {
            hasCte = true;
            
            // 先注册所有 CTE 名称（CTE 是临时结果集，不计入物理源表）
            for (FlinkSqlParser.CteDefinitionContext cteCtx : ctx.cteDefinition()) {
                if (cteCtx.cteName() != null) {
                    cteNames.add(cteCtx.cteName().getText());
                }
            }
            
            // 访问所有 CTE 定义，提取其内部查询的物理源表
            for (FlinkSqlParser.CteDefinitionContext cteCtx : ctx.cteDefinition()) {
                visit(cteCtx);
            }
            
            // 主语句：WITH ... INSERT INTO ... 或 WITH ... SELECT ...
            if (ctx.insertStatement() != null) {
                visit(ctx.insertStatement());
            } else if (ctx.queryExpression() != null) {
                visit(ctx.queryExpression());
            }
            
            return null;
        }
        
        @Override
        public Void visitCreateTableStatement(FlinkSqlParser.CreateTableStatementContext ctx) {
            // CTAS: CREATE TABLE target AS SELECT ... FROM source
            if (ctx.tablePath() != null && ctx.queryExpression() != null) {
                targetTable = extractTableName(ctx.tablePath());
                processType = "CTAS";
                
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
        public Void visitCteDefinition(FlinkSqlParser.CteDefinitionContext ctx) {
            // CTE 名称已在外层注册，仅访问内部查询提取物理源表
            if (ctx.queryExpression() != null) {
                visit(ctx.queryExpression());
            }
            
            return null;
        }
        
        // visitQueryExpression 不覆写：使用基类 visitChildren 完整下钻
        // SELECT/WHERE 中的子查询（IN、标量子查询）同样产生表依赖
        
        @Override
        public Void visitFromClause(FlinkSqlParser.FromClauseContext ctx) {
            // 访问所有表引用
            for (FlinkSqlParser.TableReferenceContext refCtx : ctx.tableReference()) {
                visitTableReference(refCtx);
            }
            
            return null;
        }
        
        @Override
        public Void visitTableReference(FlinkSqlParser.TableReferenceContext ctx) {
            // JOIN 分支：先递归处理左侧表引用
            if (ctx.tableReference() != null) {
                visit(ctx.tableReference());
            }
            
            // 处理表路径
            if (ctx.tablePath() != null) {
                addSourceTable(extractTableName(ctx.tablePath()));
            }
            
            // 处理子查询
            if (ctx.queryExpression() != null) {
                visit(ctx.queryExpression());
            }
            
            return null;
        }
        
        /**
         * 添加源表（排除 CTE 临时名称）
         */
        private void addSourceTable(String tableName) {
            if (tableName == null || tableName.isEmpty()) {
                return;
            }
            if (cteNames.contains(tableName)) {
                return;
            }
            sourceTables.add(tableName);
        }
        
        @Override
        public Void visitFunctionCall(FlinkSqlParser.FunctionCallContext ctx) {
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
        private String extractTableName(FlinkSqlParser.TablePathContext ctx) {
            if (ctx == null) {
                return null;
            }
            
            List<String> parts = new ArrayList<>();
            for (FlinkSqlParser.UidContext uidCtx : ctx.uid()) {
                parts.add(uidCtx.getText());
            }
            
            return String.join(".", parts);
        }
        
        /**
         * 提取函数名
         */
        private String extractFunctionName(FlinkSqlParser.FunctionCallContext ctx) {
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
