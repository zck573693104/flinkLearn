package com.bigdata.lineage.graph;

import com.bigdata.lineage.parser.MultiEngineSQLLineageParser;
import com.bigdata.lineage.parser.model.TableLineage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 快照存储：读侧拿到的必须是同一份不可变图，重扫之后整体换掉。 */
class LineageStoreTest {

    private static final MultiEngineSQLLineageParser PARSER = new MultiEngineSQLLineageParser();

    private static List<TableLineage> statements(String sql) {
        return PARSER.extractTableLineages(sql, false);
    }

    @Test
    void anUnscannedStoreIsEmptyButStillReadable() {
        LineageStore store = new LineageStore();
        assertTrue(store.current().isEmpty());
        assertEquals(0, store.current().getStatementCount());
        assertTrue(store.graph().getNodes().isEmpty());
    }

    @Test
    void replaceSwapsTheWholeSnapshot() {
        LineageStore store = new LineageStore();
        LineageGraph first = store.graph();
        store.replace(statements("INSERT INTO dwd.t SELECT id FROM ods.src"), "sql/a.sql", 12L);

        assertFalse(store.current().isEmpty());
        assertEquals(1, store.current().getStatementCount());
        assertEquals("sql/a.sql", store.current().getSource());
        assertEquals(12L, store.current().getDurationMillis());
        assertNotSame(store.graph(), first);
        assertTrue(first.getNodes().isEmpty(), "旧快照不能被就地改写");
    }

    /** 读侧一次请求里读到的图不能中途换掉：换引用而不是换内容 */
    @Test
    void snapshotKeepsServingTheGraphItWasTakenWith() {
        LineageStore store = new LineageStore();
        store.replace(statements("INSERT INTO dwd.t SELECT id FROM ods.src"), "a", 1L);
        LineageStore.Snapshot before = store.current();

        store.replace(statements("INSERT INTO dwd.u SELECT id FROM ods.src2"), "b", 2L);

        assertFalse(before.getGraph().getNodes().containsKey("dwd.u"),
                "旧快照不能看见新血缘");
        assertTrue(before.getGraph().getNodes().containsKey("dwd.t"));
        assertTrue(store.graph().getNodes().containsKey("dwd.u"), "新快照要有新血缘");
        assertEquals("b", store.current().getSource());
    }

    @Test
    void nullStatementsProduceAnEmptySnapshotInsteadOfAnException() {
        LineageStore store = new LineageStore();
        store.replace(new ArrayList<TableLineage>(), null, 0L);
        assertTrue(store.current().isEmpty());
        assertEquals(0, store.current().getStatementCount());
        store.replace(null, "x", 0L);
        assertTrue(store.current().isEmpty());
    }
}
