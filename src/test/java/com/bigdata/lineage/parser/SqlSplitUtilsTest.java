package com.bigdata.lineage.parser;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SqlSplitUtils 语句切分回归测试。
 * 切错语句的后果不是报错而是血缘塌陷：多条 INSERT 会被并成一条，
 * 目标表互相覆盖、CTE 名泄漏成输入表
 */
class SqlSplitUtilsTest {

    private void assertCount(int expected, String sql) {
        List<String> parts = SqlSplitUtils.splitStatements(sql);
        assertEquals(expected, parts.size(), "切分数量不符，结果: " + parts);
    }

    @Test
    void splitsPlainStatements() {
        assertCount(2, "INSERT INTO a SELECT * FROM x; INSERT INTO b SELECT * FROM y");
    }

    @Test
    void ignoresSemicolonInsideStringLiteral() {
        assertCount(2, "INSERT INTO a SELECT ';' FROM x; INSERT INTO b SELECT 1");
    }

    @Test
    void ignoresSemicolonInsideLineComment() {
        assertCount(2, "-- 步骤; 下一步\nINSERT INTO a SELECT * FROM x;\nINSERT INTO b SELECT 1");
    }

    @Test
    void ignoresSemicolonInsideBlockComment() {
        assertCount(2, "/* 说明; 含分号 */\nINSERT INTO a SELECT * FROM x;\nINSERT INTO b SELECT 1");
    }

    /**
     * 注释里的单引号不能把后续状态带进"字符串内"，
     * 这是真实数仓作业（英文缩写 it's/don't 出现在注释里）的切分事故
     */
    @Test
    void apostropheInCommentDoesNotLeakQuoteState() {
        assertCount(2, "/* it's a test */\nINSERT INTO a SELECT * FROM x;\nINSERT INTO b SELECT * FROM y");
        assertCount(2, "-- don't stop\nINSERT INTO a SELECT * FROM x;\nINSERT INTO b SELECT * FROM y");
    }

    @Test
    void ignoresSemicolonInsideQuotedIdentifier() {
        assertCount(2, "INSERT INTO a SELECT \"x;y\" FROM t;\nINSERT INTO b SELECT 1");
        assertCount(2, "INSERT INTO a SELECT `x;y` FROM t;\nINSERT INTO b SELECT 1");
    }

    @Test
    void doubledQuoteIsEscapeNotClose() {
        assertCount(2, "INSERT INTO a SELECT 'it''s;ok' FROM t;\nINSERT INTO b SELECT 1");
    }

    @Test
    void keepsCommentTextWithStatement() {
        List<String> parts = SqlSplitUtils.splitStatements("/* h */ SELECT 1");
        assertTrue(parts.get(0).startsWith("/* h */"), "注释应随语句保留，便于回溯: " + parts);
    }

    @Test
    void blankInputYieldsSingleStatement() {
        assertCount(1, ";;;");
    }
}
