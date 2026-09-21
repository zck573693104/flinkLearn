package com.bigdata.lineage.parser.scope;

import java.util.Collections;
import java.util.List;

/**
 * 作用域里的一个关系：物理表、CTE、子查询伪节点，或 explode/unnest 生成的行。
 */
public final class Relation {

    private final String name;
    private final List<String> knownColumns;

    private Relation(String name, List<String> knownColumns) {
        this.name = name;
        this.knownColumns = knownColumns == null ? Collections.<String>emptyList() : knownColumns;
    }

    public static Relation physicalTable(String fullName) {
        return new Relation(fullName, Collections.<String>emptyList());
    }

    /** 派生关系（CTE / 子查询 / unnest 行），{@code knownColumns} 是已知的输出列，可为空表示未知 */
    public static Relation derived(String name, List<String> knownColumns) {
        return new Relation(name, knownColumns);
    }

    public String getName() {
        return name;
    }

    public List<String> getKnownColumns() {
        return knownColumns;
    }
}
