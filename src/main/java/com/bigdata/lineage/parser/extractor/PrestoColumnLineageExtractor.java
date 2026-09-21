package com.bigdata.lineage.parser.extractor;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import com.bigdata.lineage.parser.model.ColumnEdge;
import io.github.melin.superior.parser.presto.antlr4.PrestoSqlLexer;
import io.github.melin.superior.parser.presto.antlr4.PrestoSqlParser;

import java.util.List;

/**
 * Presto/Trino SQL 字段级血缘提取器：语法分析用 Presto 方言（UNNEST、EXTRACT、TRY_CAST），
 * 列绑定语义交给 {@link ColumnLineageEngine}。
 */
public class PrestoColumnLineageExtractor {

    /** 语法错误由表级提取器计数，这里不再产生第二个口径 */
    public List<ColumnEdge> extractFromSql(String sql) {
        CharStream charStream = CharStreams.fromString(sql);
        PrestoSqlLexer lexer = new PrestoSqlLexer(charStream);
        lexer.removeErrorListeners();

        PrestoSqlParser parser = new PrestoSqlParser(new CommonTokenStream(lexer));
        parser.removeErrorListeners();

        return new ColumnLineageEngine(sql, parser.sqlStatements(),
                parser.getRuleNames(), parser.getVocabulary()).run();
    }
}
