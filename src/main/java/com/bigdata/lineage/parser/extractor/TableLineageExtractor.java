package com.bigdata.lineage.parser.extractor;

import com.bigdata.lineage.parser.model.TableLineage;
import org.antlr.v4.runtime.*;
import io.github.melin.superior.parser.flink.antlr4.FlinkSqlLexer;
import io.github.melin.superior.parser.flink.antlr4.FlinkSqlParser;
import io.github.melin.superior.parser.flink.antlr4.FlinkSqlBaseVisitor;
import io.github.melin.superior.parser.flink.antlr4.BaseFlinkSqlParser;

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
            CharStream charStream = new AntlrInputStream(new ByteArrayInputStream(sql.getBytes()));
            FlinkSqlLexer lexer = new FlinkSqlLexer(charStream);
            
            // 创建解析器
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            FlinkSqlParser parser = new FlinkSqlParser(tokens);
            
            // 移除默认错误监听器（避免输出到控制台）
            parser.removeErrorListeners();
            
            // 添加自定义错误处理器
            parser.addErrorListener(new BaseErrorListener() {
                @Override
                public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
                    // 静默处理错误
                }
            });
            
            // 解析 SQL 语句
            FlinkSqlParser.SqlStatementsContext stmtCtx = parser.sqlStatements();
            
            if (parser.hasErrors()) {
                throw new RuntimeException("SQL 解析失败：" + parser.getErrors());
            }
            
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
    private static class LineageVisitor extends FlinkSqlBaseVisitor<Void> {
        
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
            
            // 访问所有 CTE 定义
            for (FlinkSqlParser.CteDefinitionContext cteCtx : ctx.cteDefinition()) {
                visit(cteCtx);
            }
            
            // 访问主查询
            if (ctx.queryExpression() != null) {
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
            // 提取 CTE 名称
            String cteName = extractCteName(ctx);
            
            // 将 CTE 视为临时源表
            sourceTables.add(cteName);
            
            // 访问内部查询
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
            // 处理嵌套查询
            if (ctx.queryExpression() != null) {
                visit(ctx.queryExpression());
                return null;
            }
            
            // 处理表路径
            if (ctx.tablePath() != null) {
                String tableName = extractTableName(ctx.tablePath());
                if (tableName != null && !sourceTables.contains(tableName)) {
                    sourceTables.add(tableName);
                }
            }
            
            // 递归处理 JOIN
            if (ctx.joinType() != null && ctx.tablePath() != null) {
                String tableName = extractTableName(ctx.tablePath());
                if (tableName != null && !sourceTables.contains(tableName)) {
                    sourceTables.add(tableName);
                }
            }
            
            return null;
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
