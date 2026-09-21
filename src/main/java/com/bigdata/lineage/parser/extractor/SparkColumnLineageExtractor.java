package com.bigdata.lineage.parser.extractor;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import com.bigdata.lineage.parser.model.ColumnEdge;
import io.github.melin.superior.parser.spark.antlr4.SparkSqlLexer;
import io.github.melin.superior.parser.spark.antlr4.SparkSqlParser;

import java.util.List;

/**
 * Spark SQL 字段级血缘提取器：语法分析用 Spark 方言，列绑定语义交给
 * {@link ColumnLineageEngine}（三套方言共用一份实现）。
 *
 * <p>与表级提取器分开而不是复用同一个 Visitor：表级靠基类 {@code visitChildren} 自然下钻，
 * 列级必须控制每层的顺序（先 WITH 注册 CTE、再 FROM 建立关系、最后 SELECT 绑定列），
 * 硬嫁接会把已有的表级回归全部变成风险面。
 */
public class SparkColumnLineageExtractor {

    /**
     * 提取字段级血缘边。语法错误不在这里上报：调用方先跑表级提取，
     * parseError 已经记在 TableLineage 上，这里再数一遍只会产生第二个口径。
     */
    public List<ColumnEdge> extractFromSql(String sql) {
        CharStream charStream = CharStreams.fromString(sql);
        SparkSqlLexer lexer = new SparkSqlLexer(charStream);
        lexer.removeErrorListeners();

        SparkSqlParser parser = new SparkSqlParser(new CommonTokenStream(lexer));
        parser.removeErrorListeners();

        return new ColumnLineageEngine(sql, parser.sqlStatements(),
                parser.getRuleNames(), parser.getVocabulary()).run();
    }
}
