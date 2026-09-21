package com.bigdata.lineage.graph;

import com.bigdata.lineage.parser.MultiEngineSQLLineageParser;
import com.bigdata.lineage.parser.model.TableLineage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 快照存储：读侧拿到的必须是同一份不可变图，重扫之后整体换掉。 */
class LineageStoreTest {

    private static final MultiEngineSQLLineageParser PARSER = new MultiEngineSQLLineageParser();

    private static List<TableLineage> statements(String sql) {
        return PARSER.extractTableLineages(sql, false);
    }

    private static ScanReport report(List<TableLineage> statements) {
        return ScanReport.of(1, Collections.<String>emptyList(), statements);
    }

    private static Map<String, String> sqlByJob(String jobId, String sql) {
        Map<String, String> map = new LinkedHashMap<>();
        map.put(jobId, sql);
        return map;
    }

    @Test
    void anUnscannedStoreIsEmptyButStillReadable() {
        LineageStore store = new LineageStore();
        assertTrue(store.current().isEmpty());
        assertEquals(0, store.current().getReport().getFileCount());
        assertTrue(store.graph().getNodes().isEmpty());
    }

    @Test
    void replaceSwapsTheWholeSnapshot() {
        LineageStore store = new LineageStore();
        LineageGraph first = store.graph();
        List<TableLineage> statements = statements("INSERT INTO dwd.t SELECT id FROM ods.src");
        store.replace(statements, report(statements), "sql/a.sql", sqlByJob("part.sql#1", "x"), 12L);

        assertFalse(store.current().isEmpty());
        assertEquals(1, store.current().getReport().getStatementCount());
        assertEquals("sql/a.sql", store.current().getSource());
        assertEquals(12L, store.current().getDurationMillis());
        assertNotSame(store.graph(), first);
        assertTrue(first.getNodes().isEmpty(), "旧快照不能被就地改写");
    }

    /** 读侧一次请求里读到的图不能中途换掉：换引用而不是换内容 */
    @Test
    void snapshotKeepsServingTheGraphItWasTakenWith() {
        LineageStore store = new LineageStore();
        List<TableLineage> before = statements("INSERT INTO dwd.t SELECT id FROM ods.src");
        store.replace(before, report(before), "a", Collections.<String, String>emptyMap(), 1L);
        LineageStore.Snapshot held = store.current();

        List<TableLineage> after = statements("INSERT INTO dwd.u SELECT id FROM ods.src2");
        store.replace(after, report(after), "b", Collections.<String, String>emptyMap(), 2L);

        assertFalse(held.getGraph().getNodes().containsKey("dwd.u"), "旧快照不能看见新血缘");
        assertTrue(held.getGraph().getNodes().containsKey("dwd.t"));
        assertTrue(store.graph().getNodes().containsKey("dwd.u"), "新快照要有新血缘");
        assertEquals("b", store.current().getSource());
    }

    @Test
    void nullInputProducesAnEmptySnapshotInsteadOfAnException() {
        LineageStore store = new LineageStore();
        store.replace(new ArrayList<TableLineage>(),
                ScanReport.of(0, Arrays.asList("empty.sql"), new ArrayList<TableLineage>()),
                null, null, 0L);
        assertTrue(store.current().isEmpty());
        assertEquals(1, store.current().getReport().getFilesWithoutLineage().size());
        store.replace(null, null, "x", null, 0L);
        assertTrue(store.current().isEmpty());
        assertEquals(0, store.current().getReport().getFileCount());
    }

    /** 证据面板按 jobId 取原文；快照必须自己留一份拷贝，否则调用方还能回头改 */
    @Test
    void snapshotOwnsTheSqlIndexItWasGiven() {
        LineageStore store = new LineageStore();
        List<TableLineage> statements = statements("INSERT INTO dwd.t SELECT id FROM ods.src");
        Map<String, String> given = sqlByJob("a.sql#1", "INSERT INTO dwd.t SELECT id FROM ods.src");
        store.replace(statements, report(statements), "sql", given, 1L);
        given.remove("a.sql#1");

        assertEquals("INSERT INTO dwd.t SELECT id FROM ods.src",
                store.current().sqlOf("a.sql#1"));
        assertNull(store.current().sqlOf("missing.sql#1"), "没登记过的语句不能凭空挤出 SQL");
        assertNull(store.current().sqlOf(null));
    }
}
