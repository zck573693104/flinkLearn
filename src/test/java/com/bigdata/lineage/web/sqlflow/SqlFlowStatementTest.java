package com.bigdata.lineage.web.sqlflow;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 语句动作是从原文开头读出来的：注释、换行、大小写都不该让它读错。
 *
 * <p>{@code TableLineage.processType} 进不了快照（快照只留图和原文），所以这里重新判一次。
 * 判错一处就会把视图写成表、把查询写成写入，UI 上的动作标签整片失真。
 */
class SqlFlowStatementTest {

    private static void expect(String sql, String effectType, boolean view) {
        SqlFlowStatement statement = SqlFlowStatement.of(sql);
        assertEquals(effectType, statement.getEffectType(), "动作判错：" + sql);
        assertEquals(view, statement.producesView(), "产出类型判错：" + sql);
    }

    @Test
    void leadingWordsDecideTheEffectType() {
        expect("INSERT INTO t SELECT a FROM s", "insert", false);
        expect("insert overwrite table t SELECT a FROM s", "insert", false);
        expect("CREATE VIEW v AS SELECT a FROM s", "create_view", true);
        expect("CREATE OR REPLACE VIEW v AS SELECT 1", "create_view", true);
        expect("CREATE TEMPORARY VIEW v AS SELECT 1", "create_view", true);
        expect("CREATE MATERIALIZED VIEW v AS SELECT 1", "create_view", true);
        expect("CREATE TABLE t AS SELECT a FROM s", "create_table", false);
        expect("MERGE INTO t USING s ON 1=1", "merge", false);
        expect("UPDATE t SET a = 1", "update", false);
        expect("DELETE FROM t", "delete", false);
        expect("SELECT a FROM s", "select", false);
    }

    /** 注释与空白在语料里是常态：跳过它们才能读到真正的开头 */
    @Test
    void commentsAndBlankLinesAreSkipped() {
        expect("-- 回刷\n\n/* 第二段 */  insert into t select a from s", "insert", false);
        expect("   \n\t CREATE   VIEW   v   AS SELECT 1", "create_view", true);
    }

    /** 大小写混着写也要认：语料里关键字没有统一的大小写口径 */
    @Test
    void keywordsAreCaseInsensitive() {
        expect("CrEaTe ViEw v AS SELECT 1", "create_view", true);
        expect("InSeRt OvErWrItE t SELECT 1", "insert", false);
    }

    /** 多写一个空格不能把 "create table" 读成 "create  table"：空白一律压成单空格再比 */
    @Test
    void whitespaceRunsCollapse() {
        expect("CREATE    TABLE t AS SELECT 1", "create_table", false);
        assertFalse(SqlFlowStatement.of("CREATE TABLE t AS SELECT 1").producesView());
    }

    /** 读不出动作就是查询：宁可标轻，也不要给一条没有目标的语句安一个写入动作 */
    @Test
    void unknownPrefixFallsBackToSelect() {
        expect("", "select", false);
        expect(null, "select", false);
        expect("USE mydb", "select", false);
        assertTrue(SqlFlowStatement.of("CREATE VIEW v AS SELECT 1").getHeading().contains("VIEW"));
    }
}
