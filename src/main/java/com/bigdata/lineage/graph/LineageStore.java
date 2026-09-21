package com.bigdata.lineage.graph;

import com.bigdata.lineage.parser.model.TableLineage;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 当前血缘快照的唯一持有者：扫描线程整块替换，读侧（REST）只拿引用，不加锁。
 *
 * <p>为什么整块替换而不是增量更新：{@link LineageGraph} 内部全不可变，换引用是原子的，
 * 一次请求里读到的节点、边、分层必然来自同一次扫描；增量更新会让读侧看见半张图。
 */
public final class LineageStore {

    private final AtomicReference<Snapshot> ref = new AtomicReference<>(Snapshot.empty());

    /** 重扫完成后一次性换掉整张图与其统计口径 */
    public void replace(List<TableLineage> statements, String source, long durationMillis) {
        ref.set(new Snapshot(ColumnGraphBuilder.build(statements), statements, source,
                durationMillis));
    }

    public Snapshot current() {
        return ref.get();
    }

    public LineageGraph graph() {
        return current().getGraph();
    }

    /** 一份快照 = 一张图 + 建图的那批语句。字段全部 final，可安全跨线程发布 */
    public static final class Snapshot {

        private static final Snapshot EMPTY =
                new Snapshot(LineageGraph.empty(), java.util.Collections.<TableLineage>emptyList(),
                        "", 0L);

        private final LineageGraph graph;
        private final int statementCount;
        private final String source;
        private final long durationMillis;

        Snapshot(LineageGraph graph, List<TableLineage> statements, String source,
                 long durationMillis) {
            this.graph = graph;
            this.statementCount = statements == null ? 0 : statements.size();
            this.source = source;
            this.durationMillis = durationMillis;
        }

        static Snapshot empty() {
            return EMPTY;
        }

        public LineageGraph getGraph() {
            return graph;
        }

        public int getStatementCount() {
            return statementCount;
        }

        /** 扫描目录或其他来源标识，用于 UI 显示"这是哪儿的血缘" */
        public String getSource() {
            return source;
        }

        public long getDurationMillis() {
            return durationMillis;
        }

        public boolean isEmpty() {
            return graph.size() == 0;
        }
    }
}
