package com.bigdata.lineage.graph;

import java.util.Collections;
import java.util.List;

/**
 * 图里的一个关系节点：物理表，或只在一条语句内可见的派生关系（CTE、折不动的伪节点）。
 *
 * <p>标识口径见 {@link LocalRelation}：派生关系带语句前缀，物理表就是归一化全名。
 * 列清单来自折叠后的字段边端点，物理表没有字段边时为空。
 */
public final class GraphNode {

    private final String id;
    private final boolean local;
    private final String name;
    private final String namespace;
    private final List<String> columns;

    GraphNode(String id, boolean local, String name, String namespace, List<String> columns) {
        this.id = id;
        this.local = local;
        this.name = name;
        this.namespace = namespace;
        this.columns = Collections.unmodifiableList(columns);
    }

    /** 归一化后的关系标识：物理表全名，或 {@code [jobId]cteName} */
    public String getId() {
        return id;
    }

    /** true 表示它是语句内的派生关系，不是物理表 */
    public boolean isLocal() {
        return local;
    }

    /** 末段短名，UI 显示用 */
    public String getName() {
        return name;
    }

    /** 物理表是库名（可为空），派生关系是所属语句的 jobId */
    public String getNamespace() {
        return namespace;
    }

    public List<String> getColumns() {
        return columns;
    }

    public boolean isPhysical() {
        return !local;
    }
}
