package com.bigdata.lineage.graph;

import com.bigdata.lineage.parser.model.TableLineage;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 当前血缘快照的唯一持有者：扫描线程整块替换，读侧（REST）只拿引用，不加锁。
 *
 * <p>为什么整块替换而不是增量更新：{@link LineageGraph} 内部全不可变，换引用是原子的，
 * 一次请求里读到的节点、边、分层必然来自同一次扫描；增量更新会让读侧看见半张图。
 */
public final class LineageStore {

    private final AtomicReference<Snapshot> ref = new AtomicReference<>(Snapshot.EMPTY);

    /**
     * 重扫完成后一次性换掉整张图与账本。
     *
     * @param sqlByJob 语句标识 → 原文，供边面板回显 SQL；键必须与图内 {@code ColumnLink.getJobId()} 一致
     */
    public void replace(List<TableLineage> statements, ScanReport report, String source,
                        Map<String, String> sqlByJob, long durationMillis) {
        ref.set(new Snapshot(ColumnGraphBuilder.build(statements),
                report == null ? ScanReport.empty() : report, source,
                immutable(sqlByJob), durationMillis));
    }

    private static Map<String, String> immutable(Map<String, String> source) {
        return source == null ? Collections.<String, String>emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(source));
    }

    public Snapshot current() {
        return ref.get();
    }

    public LineageGraph graph() {
        return current().getGraph();
    }

    /** 一份快照 = 一张图 + 一份账本 + 语句原文索引。字段全部 final，可安全跨线程发布 */
    public static final class Snapshot {

        private static final Snapshot EMPTY = new Snapshot(LineageGraph.empty(),
                ScanReport.empty(), "", Collections.<String, String>emptyMap(), 0L);

        private final LineageGraph graph;
        private final ScanReport report;
        private final String source;
        private final Map<String, String> sqlByJob;
        private final long durationMillis;

        Snapshot(LineageGraph graph, ScanReport report, String source,
                 Map<String, String> sqlByJob, long durationMillis) {
            this.graph = graph;
            this.report = report;
            this.source = source;
            this.sqlByJob = sqlByJob;
            this.durationMillis = durationMillis;
        }

        public LineageGraph getGraph() {
            return graph;
        }

        /** 解析质量账本：文件数、语句数、parseError/UNRESOLVED/STAR 计数与明细 */
        public ScanReport getReport() {
            return report;
        }

        /** 扫描目录或其他来源标识，用于 UI 显示"这是哪儿的血缘" */
        public String getSource() {
            return source;
        }

        /** 语句原文；扫描器没登记过这条语句时返回 null，面板据此留白而不是造一段假 SQL */
        public String sqlOf(String jobId) {
            return jobId == null ? null : sqlByJob.get(jobId);
        }

        public long getDurationMillis() {
            return durationMillis;
        }

        public boolean isEmpty() {
            return graph.size() == 0;
        }
    }
}
