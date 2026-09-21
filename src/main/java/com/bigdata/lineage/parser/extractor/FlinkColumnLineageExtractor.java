package com.bigdata.lineage.parser.extractor;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import com.bigdata.lineage.parser.model.ColumnEdge;
import io.github.melin.superior.parser.flink.antlr4.FlinkSqlLexer;
import io.github.melin.superior.parser.flink.antlr4.FlinkSqlParser;

import java.util.List;

/**
 * Flink SQL 字段级血缘提取器：语法分析用 Flink 方言（窗口 TVF、UNNEST、时态表 JOIN），
 * 列绑定语义交给 {@link ColumnLineageEngine}。
 */
public class FlinkColumnLineageExtractor {

    /** 语法错误由表级提取器计数，这里不再产生第二个口径 */
    public List<ColumnEdge> extractFromSql(String sql) {
        CharStream charStream = CharStreams.fromString(sql);
        FlinkSqlLexer lexer = new FlinkSqlLexer(charStream);
        lexer.removeErrorListeners();

        FlinkSqlParser parser = new FlinkSqlParser(new CommonTokenStream(lexer));
        parser.removeErrorListeners();

        return new ColumnLineageEngine(sql, parser.sqlStatements(),
                parser.getRuleNames(), parser.getVocabulary()).run();
    }
}
