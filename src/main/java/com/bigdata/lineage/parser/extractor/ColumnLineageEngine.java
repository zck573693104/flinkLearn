package com.bigdata.lineage.parser.extractor;

import com.bigdata.lineage.parser.NameNormalizer;
import com.bigdata.lineage.parser.model.ColumnDerivation;
import com.bigdata.lineage.parser.model.ColumnEdge;
import com.bigdata.lineage.parser.model.ColumnRef;
import com.bigdata.lineage.parser.scope.QueryScope;
import com.bigdata.lineage.parser.scope.Relation;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * 字段级血缘引擎：Flink / Spark / Presto 三套方言共用一份列绑定语义。
 *
 * <p>按**规则名与符号 token 名**分派，而不是生成的上下文类型：本项目手写的三套 grammar 刻意保持
 * 同名规则（{@code queryExpression}、{@code tableReference}、{@code columnRef}、{@code cteDefinition}…），
 * 方言独有的规则再按名字分叉（Spark 的 {@code lateralView}、Flink 的 {@code tvfFunction}、
 * Flink/Presto 的 {@code UNNEST}）。命名口径、位置映射、歧义判定因此只有一份实现——
 * 字段血缘的正确性风险远高于省下的代码量。
 *
 * <p>只产出**值来源**边：WHERE / JOIN ON / GROUP BY 限定的是行集合，不是字段取值，
 * 它们的表依赖由表级血缘负责，这里不伪造列间关系。
 */
final class ColumnLineageEngine {

    private static final Set<String> AGGREGATE_FUNCTIONS = new HashSet<>(Arrays.asList(
            "COUNT", "SUM", "AVG", "MIN", "MAX", "STDDEV", "STDDEV_POP", "STDDEV_SAMP",
            "VARIANCE", "VAR_POP", "VAR_SAMP", "COLLECT_LIST", "COLLECT_SET",
            "GROUPING", "GROUPING_ID", "APPROX_COUNT_DISTINCT", "ANY_VALUE",
            "CORR", "COVAR_POP", "COVAR_SAMP", "MAX_BY", "MIN_BY",
            "ARRAY_AGG", "STRING_AGG", "LISTAGG"
    ));

    private final String sql;
    private final ParseTree root;
    private final String[] ruleNames;
    private final Vocabulary vocabulary;
    private final String engine;

    private final List<ColumnEdge> edges = new ArrayList<>();
    private int pseudoCounter;
    /**
     * 只在这条语句里成立的中间关系名：子查询（带别名时以别名登记）与 explode/unnest 的展开行。
     * 图构建层要折掉它们，但光看名字分不出 {@code s} 是子查询还是 CTE、物理表，所以由边带着标记走。
     */
    private final Set<String> internalRelations = new LinkedHashSet<>();

    /**
     * @param engine FLINK / SPARK / PRESTO，在建边时一次写定：调用方若在缓存结果上补这个字段，
     *               就成了多线程共写同一批 {@link ColumnEdge}。
     */
    ColumnLineageEngine(String sql, ParseTree root, String[] ruleNames, Vocabulary vocabulary,
                        String engine) {
        this.sql = sql;
        this.root = root;
        this.ruleNames = ruleNames;
        this.vocabulary = vocabulary;
        this.engine = engine;
    }

    List<ColumnEdge> run() {
        if (root == null) {
            return edges;
        }
        for (ParserRuleContext stmt : childRules(root, "statement")) {
            handleStatement(stmt);
        }
        return edges;
    }

    // ============================================
    // 语句分派
    // ============================================

    private void handleStatement(ParserRuleContext stmt) {
        ParserRuleContext insert = childRule(stmt, "insertStatement");
        if (insert != null) {
            handleInsert(insert, null);
            return;
        }
        ParserRuleContext cte = childRule(stmt, "cteStatement");
        if (cte != null) {
            handleCteStatement(cte);
            return;
        }
        ParserRuleContext create = childRule(stmt, "createTableStatement");
        if (create != null) {
            handleCreateTable(create);
            return;
        }
        ParserRuleContext update = childRule(stmt, "updateStatement");
        if (update != null) {
            handleUpdate(update);
        }
    }

    /** {@code WITH c AS (...) INSERT INTO t SELECT ...}：WITH 挂在语句最前面 */
    private void handleCteStatement(ParserRuleContext ctx) {
        ParserRuleContext insert = childRule(ctx, "insertStatement");
        if (insert == null) {
            return;
        }
        QueryScope outer = new QueryScope(null);
        ParserRuleContext with = childRule(ctx, "withClause");
        if (with != null) {
            registerCtes(with, outer);
        }
        handleInsert(insert, outer);
    }

    private void handleInsert(ParserRuleContext ctx, QueryScope inherited) {
        ParserRuleContext tablePath = childRule(ctx, "tablePath");
        // INSERT OVERWRITE DIRECTORY '/path'：没有目标表，列边无处可挂
        if (tablePath == null) {
            return;
        }
        String target = tableName(tablePath);
        List<String> positional = columnNames(childRule(ctx, "columnNameList"));

        ParserRuleContext query = childRule(ctx, "queryExpression");
        if (query != null) {
            processQuery(query, inherited, target, positional);
        } else if (!positional.isEmpty()) {
            // VALUES 只有字面量，列名全靠 INSERT 列名清单
            for (int i = 0; i < positional.size(); i++) {
                edges.add(edge(target, positional.get(i),
                        Collections.<ColumnRef>emptyList(), ColumnDerivation.CONSTANT, "VALUES", i));
            }
        }
    }

    private void handleCreateTable(ParserRuleContext ctx) {
        ParserRuleContext tablePath = childRule(ctx, "tablePath");
        ParserRuleContext query = childRule(ctx, "queryExpression");
        if (tablePath == null || query == null) {
            return;
        }
        processQuery(query, null, tableName(tablePath), declaredTableColumns(ctx));
    }

    /**
     * CTAS 的显式列清单（Flink 把列包在 {@code tableElement} 里，Spark/Presto 直接挂
     * {@code columnDefinition}）：与水查询按位置对齐，否则目标列名会退回 SELECT 里的别名。
     */
    private List<String> declaredTableColumns(ParserRuleContext create) {
        List<String> names = new ArrayList<>();
        collectColumnDefinitions(create, names);
        for (ParserRuleContext element : childRules(create, "tableElement")) {
            collectColumnDefinitions(element, names);
        }
        return names;
    }

    private void collectColumnDefinitions(ParserRuleContext ctx, List<String> names) {
        for (ParserRuleContext definition : childRules(ctx, "columnDefinition")) {
            String name = uidOf(definition);
            if (name != null) {
                names.add(name);
            }
        }
    }

    private void handleUpdate(ParserRuleContext ctx) {
        ParserRuleContext tablePath = childRule(ctx, "tablePath");
        ParserRuleContext assignments = childRule(ctx, "assignmentList");
        if (tablePath == null || assignments == null) {
            return;
        }
        String target = tableName(tablePath);
        QueryScope scope = new QueryScope(null);
        scope.register(Relation.physicalTable(target), uidOf(childRule(ctx, "alias")));

        List<ParserRuleContext> items = childRules(assignments, "assignment");
        for (int i = 0; i < items.size(); i++) {
            ParserRuleContext item = items.get(i);
            ParserRuleContext expr = childRule(item, "expression");
            List<ColumnRef> sources = sourcesOf(expr, scope);
            edges.add(edge(target, uidOf(item), sources,
                    classify(expr, sources), textOf(expr), i));
        }
    }

    // ============================================
    // 查询作用域
    // ============================================

    /**
     * 处理一个查询表达式：WITH 建立本层可见的 CTE，集合操作的每个分支各自开作用域，
     * 但共用同一个目标与同一套输出列名（第一分支的列名有约束力）。
     *
     * @return 本层的输出列名，供外层把它当关系来解析
     */
    private List<String> processQuery(ParserRuleContext qe, QueryScope parent,
                                      String target, List<String> positional) {
        QueryScope cteScope = new QueryScope(parent);
        registerWith(qe, cteScope);

        // 集合操作的各分支共用同一套输出列名：没有显式列清单时由第一分支定名。
        // 空清单与"没有清单"等价——columnNames() 永不返回 null，按 null 判断会让整层失去列名。
        List<String> names = positional == null || positional.isEmpty() ? null : positional;
        List<String> head = processBranch(qe, cteScope, target, names);
        if (names == null) {
            names = head;
        }
        processSetBranches(qe, cteScope, target, names);
        return names;
    }

    /**
     * 集合操作在语法里是右递归的：{@code A UNION B INTERSECT C} 解析成
     * {@code A UNION (B INTERSECT C)}，只看直接子节点会静默丢掉末分支。
     */
    private void processSetBranches(ParserRuleContext qe, QueryScope outer,
                                    String target, List<String> names) {
        for (ParserRuleContext part : childRules(qe, "queryExpression")) {
            QueryScope partScope = new QueryScope(outer);
            registerWith(part, partScope);
            processBranch(part, partScope, target, names);
            processSetBranches(part, partScope, target, names);
        }
    }

    private void registerWith(ParserRuleContext qe, QueryScope scope) {
        ParserRuleContext with = childRule(qe, "withClause");
        if (with != null) {
            registerCtes(with, scope);
        }
    }

    private List<String> processBranch(ParserRuleContext branch, QueryScope parent,
                                       String target, List<String> positional) {
        QueryScope scope = new QueryScope(parent);
        ParserRuleContext from = childRule(branch, "fromClause");
        if (from != null) {
            registerFrom(from, scope);
        }

        List<String> out = new ArrayList<>();
        ParserRuleContext select = childRule(branch, "selectClause");
        ParserRuleContext columnList = select == null ? null : childRule(select, "columnList");
        if (columnList == null) {
            return out;
        }
        List<ParserRuleContext> items = childRules(columnList, "columnDef");
        for (int i = 0; i < items.size(); i++) {
            String name = emitSelectItem(items.get(i), i, scope, target, positional);
            if (name != null) {
                out.add(name);
            }
        }
        return out;
    }

    private String emitSelectItem(ParserRuleContext def, int ordinal, QueryScope scope,
                                  String target, List<String> positional) {
        String positionalName = positional != null && ordinal < positional.size()
                ? positional.get(ordinal) : null;

        List<String> starPrefix = qualifiedStar(def);
        if (hasStarChild(def) || starPrefix != null) {
            ParserRuleContext starText = childRule(def, "expression");
            edges.add(edge(target, "*", starSources(starPrefix, scope),
                    ColumnDerivation.STAR, textOf(starText != null ? starText : def), ordinal));
            return "*";
        }

        ParserRuleContext expr = childRule(def, "expression");
        if (expr == null) {
            return null;
        }
        List<ColumnRef> sources = sourcesOf(expr, scope);
        String own = ownName(def, expr);

        String name;
        ColumnDerivation derivation = classify(expr, sources);
        if (own != null) {
            name = positionalName != null ? positionalName : own;
        } else if (positionalName != null) {
            // 列项本身无名，靠 INSERT / CTE 列名清单按位置对齐——名字对得上纯属巧合
            name = positionalName;
            derivation = ColumnDerivation.POSITIONAL;
        } else {
            name = "expr_" + (ordinal + 1);
            derivation = ColumnDerivation.UNNAMED;
        }

        edges.add(edge(target, name, sources, derivation, textOf(expr), ordinal));
        return name;
    }

    // ============================================
    // 关系登记
    // ============================================

    private void registerCtes(ParserRuleContext with, QueryScope scope) {
        for (ParserRuleContext cte : childRules(with, "cteDefinition")) {
            String name = uidOf(childRule(cte, "cteName"));
            if (name == null) {
                continue;
            }
            List<String> declared = columnNames(childRule(cte, "columnNameList"));
            ParserRuleContext body = childRule(cte, "queryExpression");
            List<String> output = body == null
                    ? new ArrayList<String>() : processQuery(body, scope, name, declared);
            scope.register(Relation.derived(name, declared.isEmpty() ? output : declared), null);
        }
    }

    private void registerFrom(ParserRuleContext from, QueryScope scope) {
        for (ParserRuleContext ref : childRules(from, "tableReference")) {
            registerRef(ref, scope);
        }
    }

    private void registerRef(ParserRuleContext ref, QueryScope scope) {
        // JOIN / LATERAL VIEW 的左侧永远是嵌套 tableReference
        ParserRuleContext left = childRule(ref, "tableReference");
        if (left != null) {
            registerRef(left, scope);
        }

        ParserRuleContext tvf = childRule(ref, "tvfFunction");
        if (tvf != null) {
            registerTvf(tvf, ref, scope);
        }

        ParserRuleContext tablePath = childRule(ref, "tablePath");
        if (tablePath != null) {
            String full = tableName(tablePath);
            // 与外层同名时是 CTE 被引用，不是又一张物理表：重复登记会让限定列歧义、裸 * 展开出两套关系
            Relation known = scope.bind(full);
            scope.register(known != null ? known : Relation.physicalTable(full),
                    uidOf(childRule(ref, "alias")));
        }

        ParserRuleContext sub = childRule(ref, "queryExpression");
        if (sub != null) {
            String alias = uidOf(childRule(ref, "alias"));
            String subTarget = alias != null ? alias : nextPseudo("sub");
            // 带别名时以别名登记，但它在图上仍是中间产物：先记名，边才带得走这个标记
            internalRelations.add(subTarget);
            List<String> output = processQuery(sub, scope, subTarget, null);
            scope.register(Relation.derived(subTarget, output), alias);
        }

        ParserRuleContext lateral = childRule(ref, "lateralView");
        if (lateral != null) {
            registerLateralView(lateral, scope);
        }

        if (hasToken(ref, "KW_UNNEST")) {
            registerUnnest(ref, scope);
        }
    }

    /** 窗口 TVF 的行就是输入表的行，时间列与窗口大小只限定行，不改变列取值 */
    private void registerTvf(ParserRuleContext tvf, ParserRuleContext ref, QueryScope scope) {
        ParserRuleContext tablePath = childRule(tvf, "tablePath");
        if (tablePath == null) {
            return;
        }
        String full = tableName(tablePath);
        Relation known = scope.bind(full);
        scope.register(known != null ? known : Relation.physicalTable(full),
                uidOf(childRule(ref, "alias")));
    }

    /**
     * {@code LATERAL VIEW explode(base.arr) e AS elem} 与 {@code CROSS JOIN UNNEST(arr) AS e (elem)}：
     * 展开出的行不是物理表，挂到伪关系上，由图构建层折叠回参数的真实来源列。
     */
    private void registerLateralView(ParserRuleContext lateral, QueryScope scope) {
        List<String> columns = columnNames(childRule(lateral, "columnNameList"));
        // 参数只在展开之前可见：先解来源，再登记伪关系
        ParserRuleContext rowAlias = childRule(lateral, "lateralViewTableAlias");
        // posexplode 的首列是行下标，和 Presto/Flink 的 WITH ORDINALITY 一样没有上游字段
        int counter = "POSEXPLODE".equals(functionNameOf(lateral)) && columns.size() >= 2 ? 0 : -1;
        registerExpansion("lat", lateral, columns, argSources(lateral, scope),
                counter, uidOf(rowAlias), scope);
    }

    private void registerUnnest(ParserRuleContext ref, QueryScope scope) {
        List<List<ColumnRef>> perArg = argSources(ref, scope);
        ParserRuleContext withColumns = childRule(ref, "aliasWithColumns");
        String rowAlias;
        List<String> columns;
        if (withColumns == null) {
            // 只给别名时，别名就是展开列的名字（Trino 的 UNNEST(x) AS v 写法）
            rowAlias = uidOf(childRule(ref, "alias"));
            columns = rowAlias == null ? new ArrayList<String>()
                    : new ArrayList<>(Collections.singletonList(rowAlias));
        } else {
            // aliasWithColumns : KW_AS? uid LPAREN uid (COMMA uid)* RPAREN，首段是行别名
            List<String> uids = directUids(withColumns);
            rowAlias = uids.isEmpty() ? null : uids.get(0);
            columns = uids.isEmpty() ? new ArrayList<String>()
                    : new ArrayList<>(uids.subList(1, uids.size()));
        }
        int counter = hasToken(ref, "KW_ORDINALITY") && columns.size() >= 2
                ? columns.size() - 1 : -1;
        registerExpansion("unnest", ref, columns, perArg, counter, rowAlias, scope);
    }

    /** 展开参数逐个解析来源，列与参数一一对应时可逐列回溯 */
    private List<List<ColumnRef>> argSources(ParserRuleContext node, QueryScope scope) {
        List<List<ColumnRef>> perArg = new ArrayList<>();
        for (ParserRuleContext arg : childRules(node, "expression")) {
            perArg.add(sourcesOf(arg, scope));
        }
        return perArg;
    }

    /** {@code counter} 是行下标列的下标，没有下标列时为 -1 */
    private void registerExpansion(String kind, ParserRuleContext source, List<String> columns,
                                   List<List<ColumnRef>> perArg, int counter,
                                   String rowAlias, QueryScope scope) {
        String pseudo = nextPseudo(kind);
        // UNNEST(a, b) AS u (x, y) 第 i 列来自第 i 个数组；posexplode / MAP 展开两列一参，只能整体回溯
        boolean oneToOne = perArg.size() == columns.size();
        List<ColumnRef> union = new ArrayList<>();
        for (List<ColumnRef> part : perArg) {
            for (ColumnRef ref : part) {
                if (!union.contains(ref)) {
                    union.add(ref);
                }
            }
        }
        for (int i = 0; i < columns.size(); i++) {
            boolean generated = i == counter;
            List<ColumnRef> sources = generated ? new ArrayList<ColumnRef>()
                    : (oneToOne ? perArg.get(i) : union);
            // 常量数组展开（UNNEST(ARRAY[1, 2])）与行下标一样没有上游字段
            edges.add(edge(pseudo, columns.get(i), sources,
                    generated || sources.isEmpty() ? ColumnDerivation.CONSTANT
                            : ColumnDerivation.EXPRESSION,
                    textOf(source), i));
        }
        scope.register(Relation.derived(pseudo, columns), rowAlias);
    }

    // ============================================
    // 列绑定
    // ============================================

    private List<ColumnRef> sourcesOf(ParserRuleContext expr, QueryScope scope) {
        List<RawRef> raw = new ArrayList<>();
        collect(expr, scope, raw);

        Map<String, ColumnRef> unique = new LinkedHashMap<>();
        for (RawRef ref : raw) {
            for (ColumnRef bound : bind(ref, scope)) {
                unique.putIfAbsent(bound.nodeId(), bound);
            }
        }
        return new ArrayList<>(unique.values());
    }

    /**
     * 收集表达式里的列叶子。遇到嵌套查询就整只摘出去（伪节点），不再下钻——
     * 子查询内部的作用域与外层无关。
     */
    private void collect(ParseTree node, QueryScope scope, List<RawRef> out) {
        if (node == null) {
            return;
        }
        String name = ruleName(node);
        if ("queryExpression".equals(name)) {
            out.add(subqueryRef((ParserRuleContext) node, scope));
            return;
        }
        if ("columnRef".equals(name)) {
            out.add(RawRef.column(directUids((ParserRuleContext) node)));
            return;
        }
        if ("primaryExpression".equals(name)) {
            ParserRuleContext tablePath = childRule((ParserRuleContext) node, "tablePath");
            if (tablePath != null) {
                // qualifiedStar 要的两件事这里一次拿全：表名段与是否为 t.* 形式
                out.add(RawRef.star(directUids(tablePath)));
                return;
            }
        }
        if (isBareStar(node)) {
            out.add(RawRef.star(Collections.<String>emptyList()));
            return;
        }
        for (int i = 0; i < node.getChildCount(); i++) {
            collect(node.getChild(i), scope, out);
        }
    }

    private RawRef subqueryRef(ParserRuleContext qe, QueryScope scope) {
        String pseudo = nextPseudo("sub");
        List<String> output = processQuery(qe, scope, pseudo, null);
        String column = output.isEmpty() ? "*" : output.get(0);
        return RawRef.preBound(pseudo, Collections.singletonList(column));
    }

    private List<ColumnRef> bind(RawRef ref, QueryScope scope) {
        if (ref.boundTable != null) {
            return Collections.singletonList(
                    columnRef(null, ref.columns.get(0), ref.boundTable, ref.raw));
        }
        if (ref.star) {
            if (ref.columns.isEmpty()) {
                List<ColumnRef> all = new ArrayList<>();
                for (Relation relation : scope.getRelations()) {
                    all.add(columnRef(null, "*", relation.getName(), "*"));
                }
                return all;
            }
            Relation hit = bindStarQualifier(ref.columns, scope);
            return Collections.singletonList(
                    columnRef(join(ref.columns), "*", hit == null ? null : hit.getName(), ref.raw));
        }

        String column = ref.columns.get(ref.columns.size() - 1);
        String qualifier = ref.columns.size() > 1
                ? join(ref.columns.subList(0, ref.columns.size() - 1)) : null;
        Relation hit = ref.columns.size() == 1
                ? scope.bindByColumn(column) : bindQualified(ref.columns, scope);

        if (hit == null && ref.columns.size() > 1 && !scope.isNamespacePrefix(qualifier)) {
            // 限定符不是作用域里的表，也不是任何表的命名空间前缀：那是结构体列的访问路径
            // （operation.info_str），取值来自根列 operation，末段只是往下钻的字段名。
            // 根列的归属沿用未限定列的口径：语料里 UNNEST 逗号并列时，展开列报得出自己的
            // 列清单，根列只可能属于那张报不出表头的表。
            // 根列也绑不上时保留原样，宁可承认猜不到也不编造列名。
            Relation root = scope.bindByColumn(ref.columns.get(0));
            if (root != null) {
                column = ref.columns.get(0);
                qualifier = null;
                hit = root;
            }
        }
        return Collections.singletonList(
                columnRef(qualifier, column, hit == null ? null : hit.getName(), ref.raw));
    }

    /** 限定符可能是别名，也可能是 db.table 或 catalog.db.table：整体试一次，再退到末段 */
    private Relation bindQualified(List<String> segments, QueryScope scope) {
        List<String> qualifiers = segments.subList(0, segments.size() - 1);
        Relation hit = scope.bind(join(qualifiers));
        if (hit == null && qualifiers.size() > 1) {
            hit = scope.bind(qualifiers.get(qualifiers.size() - 1));
        }
        return hit;
    }

    /** 星号形式整串都是表名（{@code db.tbl.*}），没有末段列名可让 */
    private Relation bindStarQualifier(List<String> segments, QueryScope scope) {
        Relation hit = scope.bind(join(segments));
        if (hit == null && segments.size() > 1) {
            hit = scope.bind(segments.get(segments.size() - 1));
        }
        return hit;
    }

    private List<ColumnRef> starSources(List<String> qualifierParts, QueryScope scope) {
        if (qualifierParts == null) {
            List<ColumnRef> all = new ArrayList<>();
            for (Relation relation : scope.getRelations()) {
                all.add(columnRef(null, "*", relation.getName(), "*"));
            }
            return all;
        }
        return bind(RawRef.star(qualifierParts), scope);
    }

    private ColumnRef columnRef(String qualifier, String column, String boundTable, String raw) {
        return ColumnRef.builder()
                .qualifier(qualifier)
                .column(column)
                .boundTable(boundTable)
                .rawText(raw)
                .build();
    }

    // ============================================
    // 命名与分类
    // ============================================

    /** {@code t.*} 结构：{@code primaryExpression : tablePath DOT MULT}；裸 {@code *} 返回 null */
    private List<String> qualifiedStar(ParserRuleContext def) {
        ParserRuleContext expr = childRule(def, "expression");
        if (expr == null || expr.getChildCount() != 1) {
            return null;
        }
        ParserRuleContext primary = childRule(expr, "primaryExpression");
        if (primary == null || !hasStarChild(primary)) {
            return null;
        }
        ParserRuleContext tablePath = childRule(primary, "tablePath");
        return tablePath == null ? null : directUids(tablePath);
    }

    private String ownName(ParserRuleContext def, ParserRuleContext expr) {
        String alias = uidOf(childRule(def, "alias"));
        if (alias != null) {
            return alias;
        }
        ParserRuleContext primary = expr.getChildCount() == 1
                ? childRule(expr, "primaryExpression") : null;
        ParserRuleContext columnRefRule = primary == null ? null : childRule(primary, "columnRef");
        if (columnRefRule != null) {
            List<String> names = directUids(columnRefRule);
            if (!names.isEmpty()) {
                return names.get(names.size() - 1);
            }
        }
        return null;
    }

    private ColumnDerivation classify(ParserRuleContext expr, List<ColumnRef> sources) {
        if (sources.isEmpty()) {
            return ColumnDerivation.CONSTANT;
        }
        for (ColumnRef source : sources) {
            if (!source.isResolved()) {
                return ColumnDerivation.UNRESOLVED;
            }
        }
        if (hasAggregate(expr)) {
            return ColumnDerivation.AGGREGATE;
        }
        if (isPlainColumn(expr)) {
            return ColumnDerivation.IDENTITY;
        }
        return ColumnDerivation.EXPRESSION;
    }

    private boolean isPlainColumn(ParserRuleContext expr) {
        if (expr == null || expr.getChildCount() != 1) {
            return false;
        }
        ParserRuleContext primary = childRule(expr, "primaryExpression");
        return primary != null && childRule(primary, "columnRef") != null;
    }

    /** 聚合与开窗都把多行压成一行的值，置信度同级；子查询内部另算，不穿越作用域 */
    private boolean hasAggregate(ParseTree node) {
        if (node == null) {
            return false;
        }
        String name = ruleName(node);
        if ("queryExpression".equals(name)) {
            return false;
        }
        if ("functionCall".equals(name)) {
            // 函数名里的关键字 token 大小写随用户，统一按大写比对
            return AGGREGATE_FUNCTIONS.contains(functionNameOf((ParserRuleContext) node));
        }
        if ("windowDefinition".equals(name)) {
            return true;
        }
        for (int i = 0; i < node.getChildCount(); i++) {
            if (hasAggregate(node.getChild(i))) {
                return true;
            }
        }
        return false;
    }

    /** 规则里的 {@code functionName} 子节点文本，大写归一后与聚合/展开函数表比对 */
    private String functionNameOf(ParserRuleContext ctx) {
        ParserRuleContext function = childRule(ctx, "functionName");
        if (function == null) {
            return "";
        }
        String text = NameNormalizer.normalize(function.getText());
        return text == null ? "" : text.toUpperCase(Locale.ROOT);
    }

    // ============================================
    // 解析树工具：规则名 / token 名
    // ============================================

    private String ruleName(ParseTree node) {
        if (!(node instanceof ParserRuleContext)) {
            return null;
        }
        int index = ((ParserRuleContext) node).getRuleIndex();
        return index >= 0 && index < ruleNames.length ? ruleNames[index] : null;
    }

    private ParserRuleContext childRule(ParserRuleContext ctx, String name) {
        if (ctx == null) {
            return null;
        }
        for (int i = 0; i < ctx.getChildCount(); i++) {
            ParseTree child = ctx.getChild(i);
            if (name.equals(ruleName(child))) {
                return (ParserRuleContext) child;
            }
        }
        return null;
    }

    private List<ParserRuleContext> childRules(ParseTree ctx, String name) {
        List<ParserRuleContext> out = new ArrayList<>();
        if (ctx == null) {
            return out;
        }
        for (int i = 0; i < ctx.getChildCount(); i++) {
            ParseTree child = ctx.getChild(i);
            if (name.equals(ruleName(child))) {
                out.add((ParserRuleContext) child);
            }
        }
        return out;
    }

    /** 只看直接子节点：{@code tablePath}、{@code columnRef}、{@code columnNameList} 的 uid 都在第一层 */
    private List<String> directUids(ParserRuleContext ctx) {
        List<String> names = new ArrayList<>();
        if (ctx == null) {
            return names;
        }
        for (int i = 0; i < ctx.getChildCount(); i++) {
            ParseTree child = ctx.getChild(i);
            if ("uid".equals(ruleName(child))) {
                names.add(NameNormalizer.normalize(child.getText()));
            }
        }
        return names;
    }

    private String uidOf(ParserRuleContext ctx) {
        ParserRuleContext uid = childRule(ctx, "uid");
        return uid == null ? null : NameNormalizer.normalize(uid.getText());
    }

    private List<String> columnNames(ParserRuleContext columnNameList) {
        return columnNameList == null ? new ArrayList<String>() : directUids(columnNameList);
    }

    private String tableName(ParserRuleContext tablePath) {
        return join(directUids(tablePath));
    }

    /**
     * 乘号与星号是同一个 MULT token：只有父节点里它是唯一的子节点时才是"全部列"的星号，
     * 否则 {@code amount * rate} 会凭空多出一条整表来源。
     */
    private boolean isBareStar(ParseTree node) {
        return isStar(node) && (node.getParent() == null || node.getParent().getChildCount() == 1);
    }

    private boolean isStar(ParseTree node) {
        return node instanceof TerminalNode && "*".equals(node.getText());
    }

    private boolean hasStarChild(ParserRuleContext ctx) {
        if (ctx == null) {
            return false;
        }
        for (int i = 0; i < ctx.getChildCount(); i++) {
            if (isStar(ctx.getChild(i))) {
                return true;
            }
        }
        return false;
    }

    private boolean hasToken(ParserRuleContext ctx, String symbolicName) {
        if (ctx == null) {
            return false;
        }
        for (int i = 0; i < ctx.getChildCount(); i++) {
            ParseTree child = ctx.getChild(i);
            if (child instanceof TerminalNode
                    && symbolicName.equals(vocabulary.getSymbolicName(
                            ((TerminalNode) child).getSymbol().getType()))) {
                return true;
            }
        }
        return false;
    }

    private String nextPseudo(String kind) {
        String name = "#" + kind + (++pseudoCounter);
        internalRelations.add(name);
        return name;
    }

    /**
     * 边指向的关系是否是引擎内部的中间关系（子查询、展开行）。图构建层要折掉它们，
     * 但名字帮不了它：带别名的子查询就以别名命名（{@code s}、{@code x}），跟 CTE、物理表
     * 长得一样。只有这里知道哪些是中间产物，所以由引擎标出来。
     */
    private boolean isInternal(String relation) {
        return internalRelations.contains(relation);
    }

    private ColumnEdge edge(String target, String column, List<ColumnRef> sources,
                            ColumnDerivation derivation, String transform, int ordinal) {
        return ColumnEdge.builder()
                .targetTable(target)
                .targetColumn(column)
                .sources(sources)
                .derivation(derivation)
                .transform(transform)
                .ordinal(ordinal)
                .engine(engine)
                .targetInternal(isInternal(target))
                .build();
    }

    private String join(List<String> parts) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.size(); i++) {
            if (i > 0) {
                sb.append('.');
            }
            sb.append(parts.get(i));
        }
        return sb.toString();
    }

    /** 取原始文本而不是 getText()：拼接式文本会丢掉空格与引号，UI 展示证据时要原句 */
    private String textOf(ParserRuleContext ctx) {
        if (ctx == null || ctx.getStart() == null || ctx.getStop() == null) {
            return null;
        }
        int start = ctx.getStart().getStartIndex();
        int stop = ctx.getStop().getStopIndex();
        if (start < 0 || stop < start || stop >= sql.length()) {
            return ctx.getText();
        }
        return sql.substring(start, stop + 1);
    }

    /** 表达式里的一个列引用雏形，绑定作用域前只是归一化后的点名片段 */
    private static final class RawRef {

        private final List<String> columns;
        private final boolean star;
        private final String raw;
        private final String boundTable;

        private RawRef(List<String> columns, boolean star, String raw, String boundTable) {
            this.columns = columns;
            this.star = star;
            this.raw = raw;
            this.boundTable = boundTable;
        }

        static RawRef column(List<String> segments) {
            return new RawRef(segments, false, text(segments), null);
        }

        static RawRef star(List<String> qualifierParts) {
            return new RawRef(qualifierParts, true, text(qualifierParts) + ".*", null);
        }

        static RawRef preBound(String table, List<String> columns) {
            return new RawRef(columns, false, table + "." + text(columns), table);
        }

        private static String text(List<String> parts) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parts.size(); i++) {
                if (i > 0) {
                    sb.append('.');
                }
                sb.append(parts.get(i));
            }
            return sb.toString();
        }
    }
}
