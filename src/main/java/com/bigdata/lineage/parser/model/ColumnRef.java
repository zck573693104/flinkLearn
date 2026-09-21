package com.bigdata.lineage.parser.model;

import lombok.Builder;
import lombok.Data;

/**
 * 一个字段的限定引用。
 *
 * <p>{@code qualifier} 是 SQL 里写的限定前缀（表名、别名或 db.table），{@code column} 是末段列名；
 * 二者都已归一化。{@code boundTable} 是在当前作用域里解析出的真实关系名，绑定失败时为 null。
 */
@Data
@Builder
public class ColumnRef {

    /** 原始限定符，未限定则为 null */
    private String qualifier;

    /** 归一化列名 */
    private String column;

    /** 绑定到的关系（物理表全名 / CTE 名 / 子查询伪节点），未绑定为 null */
    private String boundTable;

    /** 原始文本，仅用于 UI 证据展示 */
    private String rawText;

    public boolean isResolved() {
        return boundTable != null && !boundTable.isEmpty();
    }

    /** 归一化的字段节点标识：{@code table.column}，未绑定时退化为限定符或裸列名 */
    public String nodeId() {
        String table = isResolved() ? boundTable : qualifier;
        return table == null || table.isEmpty() ? column : table + "." + column;
    }
}
