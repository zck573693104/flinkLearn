package com.bigdata.lineage.parser.scope;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 一个查询作用域：一条 SELECT（含其子查询、CTE 体）里可见的关系集合。
 *
 * <p>必须有这层栈：现有表级提取只用一个扁平的 CTE 名集合，嵌套 CTE 的名字会泄漏到
 * 兄弟和外层作用域；列绑定同样要求 {@code c.a} 只在定义 {@code c} 的那层及其子层可见。
 */
public final class QueryScope {

    private final QueryScope parent;

    private final Map<String, Relation> byKey = new LinkedHashMap<>();

    private final List<Relation> relations = new ArrayList<>();

    public QueryScope(QueryScope parent) {
        this.parent = parent;
    }

    /**
     * 登记关系。除关系名外还可用别名查找（可为 null）；物理表再用末段短名登记一次，
     * 因为 {@code FROM db.tbl} 之后写 {@code tbl.col} 是合法引用。
     */
    public void register(Relation relation, String alias) {
        relations.add(relation);
        put(relation.getName(), relation);
        put(alias, relation);
        String shortName = shortName(relation.getName());
        if (shortName != null) {
            put(shortName, relation);
        }
    }

    private void put(String key, Relation relation) {
        if (key != null && !key.isEmpty()) {
            byKey.putIfAbsent(key, relation);
        }
    }

    private static String shortName(String qualified) {
        if (qualified == null) {
            return null;
        }
        int dot = qualified.lastIndexOf('.');
        return dot < 0 ? null : qualified.substring(dot + 1);
    }

    /**
     * 按限定符查找关系；限定符在本层找不到时向外层查找（相关子查询要引用外层表）。
     */
    public Relation bind(String qualifier) {
        if (qualifier == null || qualifier.isEmpty()) {
            return null;
        }
        Relation found = byKey.get(qualifier);
        if (found != null) {
            return found;
        }
        return parent == null ? null : parent.bind(qualifier);
    }

    /**
     * 未限定列的归属：只有一张表时可直接认定；多表时先找声明了该列的派生关系，
     * 其次若未知表头的关系只剩一张，也认定（其余关系都报出了自己的列，容不下这一列）。
     * 仍然歧义时返回 null 让上层标为 UNRESOLVED——歧义时猜测比承认猜不到更糟。
     */
    public Relation bindByColumn(String column) {
        if (relations.size() == 1) {
            return relations.get(0);
        }
        Relation match = null;
        List<Relation> open = new ArrayList<>();
        for (Relation relation : relations) {
            if (relation.getKnownColumns().isEmpty()) {
                if (!containsSame(open, relation)) {
                    open.add(relation);
                }
            } else if (relation.getKnownColumns().contains(column)) {
                if (match != null && !match.getName().equals(relation.getName())) {
                    return null;
                }
                match = relation;
            }
        }
        if (match != null) {
            return match;
        }
        if (open.size() == 1) {
            return open.get(0);
        }
        return parent == null ? null : parent.bindByColumn(column);
    }

    private static boolean containsSame(List<Relation> list, Relation relation) {
        for (Relation candidate : list) {
            if (candidate.getName().equals(relation.getName())) {
                return true;
            }
        }
        return false;
    }

    public List<Relation> getRelations() {
        return relations;
    }
}
