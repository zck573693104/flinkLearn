package com.bigdata.lineage.parser.extractor;

import com.bigdata.lineage.parser.model.TableLineage;
import org.antlr.v4.runtime.*;
import io.github.melin.superior.parser.flink.antlr4.FlinkSqlLexer;
import io.github.melin.superior.parser.flink.antlr4.FlinkSqlParser;
import io.github.melin.superior.parser.flink.antlr4.FlinkSqlParserBaseVisitor;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 表级血缘提取器 - 基于 ANTLR4
 * 
 * 完全独立的实现，不依赖 Superior SQL Parser 的其他代码
 */
public class TableLineageExtractor {
    
    private static final double DEFAULT_CONFIDENCE = 0.95;
    
    /**
     * 从 SQL 语句提取表级血缘
     */
    public TableLineage extractFromSql(String sql) {
        try {
            // 创建词法分析器
            CharStream charStream = CharStreams.fromString(sql);
            FlinkSqlLexer lexer = new FlinkSqlLexer(charStream);
            
            // 创建解析器
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            FlinkSqlParser parser = new FlinkSqlParser(tokens);
            
            // 移除默认错误监听器（避免输出到控制台）
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
            FlinkSqlParser.SqlStatementsContext stmtCtx = parser.sqlStatements();
            
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
            throw new RuntimeException("血缘提取失败", e);
        }
    }
    
    /**
     * ANTLR4 Visitor 实现 - 遍历 AST 并提取血缘信息
     */
    private static class LineageVisitor extends FlinkSqlParserBaseVisitor<Void> {
        
        private final String originalSql;
        private String targetTable;
        private Set<String> sourceTables = new HashSet<>();
        private Set<String> cteNames = new HashSet<>();
        private String processType = "SELECT";
        private String insertMode = "INTO";
        private boolean hasCte = false;
        private boolean hasTemporalJoin = false;
        private boolean hasWindowFunc = false;
        
        LineageVisitor(String originalSql) {
            this.originalSql = originalSql;
        }
        
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
        
        @Override
        public Void visitQueryExpression(FlinkSqlParser.QueryExpressionContext ctx) {
            // 访问 FROM 子句以提取表
            if (ctx.fromClause() != null) {
                visit(ctx.fromClause());
            }
            
            // 访问集合操作（UNION/INTERSECT/EXCEPT）右侧子查询
            for (FlinkSqlParser.QueryExpressionContext childCtx : ctx.queryExpression()) {
                visit(childCtx);
            }
            
            // 检查窗口函数
            checkForWindowFunctions();
            
            return null;
        }
        
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
         * 提取 CTE 名称
         */
        private String extractCteName(FlinkSqlParser.CteDefinitionContext ctx) {
            if (ctx.cteName() == null) {
                return null;
            }
            return ctx.cteName().getText();
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
         * 检查是否有窗口函数
         */
        private void checkForWindowFunctions() {
            // 检查 WINDOW 子句
            // 这里可以添加更复杂的逻辑来检测 TUMBLE, HOP, CUMULATE 等 TVF
        }
        
        /**
         * 判断是否为窗口函数
         */
        private boolean isWindowFunction(String funcName) {
            if (funcName == null) {
                return false;
            }
            
            Set<String> windowFuncs = new HashSet<>(Arrays.asList(
                    "TUMBLE", "HOP", "CUMULATE", "SESSION",
                    "ROW_NUMBER", "RANK", "DENSE_RANK", "ROWNUM"
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
