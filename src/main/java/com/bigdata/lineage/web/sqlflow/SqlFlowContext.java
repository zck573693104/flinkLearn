package com.bigdata.lineage.web.sqlflow;

import com.bigdata.lineage.graph.ColumnLink;
import com.bigdata.lineage.graph.LineageGraph;
import com.bigdata.lineage.graph.LineageStore;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * 装配一份 sqlflow 响应所需的全部上下文：一张图 + 语句原文索引 + 质量问题名单。
 *
 * <p>为什么不直接收 {@link LineageStore.Snapshot}：贴 SQL 试解析那条路径拿不到快照（它不落库、
 * 不换快照），而那恰恰是最需要画字段血缘的入口。把"读快照"和"临时解析"压成同一个形状，
 * 装配器就只有一份逻辑，两条路径不会各说各话。
 */
public final class SqlFlowContext {

    private final String source;
    private final LineageGraph graph;
    private final Map<String, String> sqlByJob;
    private final Set<String> parseErrors;

    private SqlFlowContext(String source, LineageGraph graph, Map<String, String> sqlByJob,
                           Set<String> parseErrors) {
        this.source = source == null ? "" : source;
        this.graph = graph;
        this.sqlByJob = sqlByJob == null ? Collections.<String, String>emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(sqlByJob));
        this.parseErrors = parseErrors == null ? Collections.<String>emptySet()
                : Collections.unmodifiableSet(new LinkedHashSet<>(parseErrors));
    }

    /** 当前快照：图、原文、语法错误名单本来就在一起 */
    public static SqlFlowContext of(LineageStore.Snapshot snapshot) {
        Map<String, String> sql = new LinkedHashMap<String, String>();
        Set<String> errors = new LinkedHashSet<String>();
        // Snapshot 只给逐条 sqlOf(jobId)，这里要的是整张索引；语句标识以图里出现过的为准。
        for (ColumnLink link : snapshot.getGraph().getColumnLinks()) {
            String jobId = link.getJobId();
            if (jobId == null || sql.containsKey(jobId)) {
                continue;
            }
            sql.put(jobId, snapshot.sqlOf(jobId));
            if (snapshot.isParseError(jobId)) {
                errors.add(jobId);
            }
        }
        return new SqlFlowContext(snapshot.getSource(), snapshot.getGraph(), sql, errors);
    }

    /** 临时解析：没有快照可借，原文与质量名单由调用方按 {@code jobIdOf} 的口径递进来 */
    public static SqlFlowContext adhoc(String source, LineageGraph graph,
                                       Map<String, String> sqlByJob, Set<String> parseErrors) {
        return new SqlFlowContext(source, graph, sqlByJob, parseErrors);
    }

    LineageGraph getGraph() {
        return graph;
    }

    String getSource() {
        return source;
    }

    String sqlOf(String jobId) {
        return jobId == null ? null : sqlByJob.get(jobId);
    }

    boolean isParseError(String jobId) {
        return jobId != null && parseErrors.contains(jobId);
    }
}
