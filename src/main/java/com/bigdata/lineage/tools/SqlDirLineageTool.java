package com.bigdata.lineage.tools;

import com.bigdata.lineage.graph.ColumnGraphBuilder;
import com.bigdata.lineage.graph.GraphNode;
import com.bigdata.lineage.graph.LineageGraph;
import com.bigdata.lineage.parser.MultiEngineSQLLineageParser;
import com.bigdata.lineage.parser.model.ColumnDerivation;
import com.bigdata.lineage.parser.model.ColumnEdge;
import com.bigdata.lineage.parser.model.ColumnRef;
import com.bigdata.lineage.parser.model.TableLineage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * SQL 目录批量血缘扫描工具
 *
 * 用法：java ... SqlDirLineageTool [sql目录，默认 ./sql] [--columns]
 * 递归读取目录下所有 .sql 文件，解析每条语句的输入表/输出表，最后汇总去重。
 * --columns 额外打印字段级血缘；无论是否开启，汇总里都会给出列级质量体检
 * （未绑定来源的边、星号边、以及"列名不在原句里"的作用域泄漏嫌疑）。
 */
public class SqlDirLineageTool {

    public static void main(String[] args) throws IOException {
        boolean showColumns = false;
        Path dir = Paths.get("sql");
        for (String arg : args) {
            if ("--columns".equals(arg)) {
                showColumns = true;
            } else {
                dir = Paths.get(arg);
            }
        }

        if (!Files.isDirectory(dir)) {
            System.out.println("目录不存在：" + dir.toAbsolutePath());
            return;
        }

        List<Path> files;
        try (Stream<Path> stream = Files.walk(dir)) {
            files = stream.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().toLowerCase().endsWith(".sql"))
                    .sorted()
                    .collect(Collectors.toList());
        }

        if (files.isEmpty()) {
            System.out.println("目录 " + dir.toAbsolutePath() + " 下没有找到 .sql 文件，请将 SQL 文件放入后重试");
            return;
        }

        MultiEngineSQLLineageParser parser = new MultiEngineSQLLineageParser();
        Set<String> allOutputs = new TreeSet<>();
        Set<String> allInputs = new TreeSet<>();
        List<String> unparsed = new ArrayList<>();
        List<String> unresolved = new ArrayList<>();
        List<String> starEdges = new ArrayList<>();
        List<String> leaks = new ArrayList<>();
        List<TableLineage> all = new ArrayList<>();
        int edgeCount = 0;

        for (Path file : files) {
            String sql = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
            List<TableLineage> lineages = parser.extractTableLineages(sql);
            all.addAll(lineages);

            List<String> lines = new ArrayList<>();
            for (TableLineage lineage : lineages) {
                String target = lineage.getTargetTable();
                Set<String> sources = lineage.getSourceTables();
                boolean hasTarget = target != null && !target.isEmpty();
                boolean hasSource = sources != null && !sources.isEmpty();
                if (!hasTarget && !hasSource) {
                    // USE/DROP 等无血缘语句，跳过噪音
                    continue;
                }
                if (hasTarget) {
                    allOutputs.add(target);
                }
                if (hasSource) {
                    allInputs.addAll(sources);
                }

                String type = lineage.getProcessType() == null ? "UNKNOWN" : lineage.getProcessType();
                if ("INSERT".equals(type) && lineage.getInsertMode() != null) {
                    type = type + " " + lineage.getInsertMode();
                }
                String srcPart = (sources == null || sources.isEmpty())
                        ? "-"
                        : new TreeSet<>(sources).stream().collect(Collectors.joining(", "));
                lines.add("  [" + type + "] 输出: " + (target == null ? "-" : target)
                        + "  <- 输入: " + srcPart);

                List<ColumnEdge> edges = lineage.getColumnEdges();
                edgeCount += edges.size();
                String statement = statementOf(lineage);
                for (ColumnEdge edge : edges) {
                    String desc = describeEdge(edge);
                    if (edge.getDerivation() == ColumnDerivation.UNRESOLVED) {
                        unresolved.add(desc);
                    } else if (edge.getDerivation() == ColumnDerivation.STAR) {
                        starEdges.add(desc);
                    }
                    for (String column : leakedColumns(edge, statement)) {
                        leaks.add(file.getFileName() + " " + desc + " 的列名 " + column
                                + " 未出现在原语句中");
                    }
                    if (showColumns) {
                        lines.add("      " + desc + " <- " + edge.getSources());
                    }
                }
            }

            System.out.println("== " + file + " ==");
            if (lines.isEmpty()) {
                System.out.println("  (未解析出血缘语句)");
                unparsed.add(file.toString());
            } else {
                lines.forEach(System.out::println);
            }
        }

        allInputs.removeAll(allOutputs);

        System.out.println();
        System.out.println("================ 汇总（去重后） ================");
        System.out.println("文件数: " + files.size() + "，其中有血缘: " + (files.size() - unparsed.size()));
        System.out.println();
        System.out.println("输出表 (" + allOutputs.size() + "):");
        allOutputs.forEach(t -> System.out.println("  " + t));
        System.out.println();
        System.out.println("纯输入表 (" + allInputs.size() + ")（未出现在任何输出表中）:");
        allInputs.forEach(t -> System.out.println("  " + t));
        if (!unparsed.isEmpty()) {
            System.out.println();
            System.out.println("未解析出结果的文件 (" + unparsed.size() + "):");
            unparsed.forEach(f -> System.out.println("  " + f));
        }

        System.out.println();
        System.out.println("================ 字段级血缘体检 ================");
        System.out.println("字段级边总数: " + edgeCount);
        printGroup("列名不在原句中（疑似作用域泄漏）", leaks);
        printGroup("UNRESOLVED（来源绑定失败，需要人工确认）", unresolved);
        printGroup("STAR（v1 按约定不展开）", starEdges);

        reportGraph(all);
    }

    /**
     * 图构建体检：折叠后不该再有伪节点，每个物理表都该有层号。
     * 这两条在单测里各管一段，只有在全语料上跑才看得出漏折与错接。
     */
    private static void reportGraph(List<TableLineage> statements) {
        LineageGraph graph = ColumnGraphBuilder.build(statements);
        List<String> leaks = new ArrayList<>();
        List<String> unranked = new ArrayList<>();
        int local = 0;
        for (GraphNode node : graph.getNodes().values()) {
            if (node.getId().contains("#")) {
                leaks.add(node.getId());
            }
            if (node.isLocal()) {
                local++;
            } else if (!graph.getRanks().containsKey(node.getId())) {
                unranked.add(node.getId());
            }
        }
        System.out.println();
        System.out.println("================ 血缘图体检 ================");
        System.out.println("节点: " + graph.size() + "（语句内关系 " + local
                + "，物理表 " + (graph.size() - local) + "）");
        System.out.println("字段级边: " + graph.getColumnLinks().size()
                + "，表级边: " + graph.getTableLinks().size());
        System.out.println("有字段级血缘的表: " + graph.tableCountWithColumns());
        System.out.println("最大层号: " + maxRank(graph) + "，环上表: " + graph.getCyclicTables());
        printGroup("未折叠的伪节点（必须为 0）", leaks);
        printGroup("没有层号的物理表（必须为 0）", unranked);
    }

    private static int maxRank(LineageGraph graph) {
        int max = 0;
        for (Integer rank : graph.getRanks().values()) {
            max = Math.max(max, rank);
        }
        return max;
    }

    /** 边的可读数：目标列 + 加工方式 */
    private static String describeEdge(ColumnEdge edge) {
        return edge.getTargetTable() + "." + edge.getTargetColumn()
                + "[" + edge.getDerivation() + "]";
    }

    private static String statementOf(TableLineage lineage) {
        return lineage.getOriginalSql() == null ? "" : lineage.getOriginalSql().toLowerCase();
    }

    /**
     * 幽灵列检查：端点列名必须在原句里出现过。伪关系、占位列（expr_N）与通配符不是 SQL 里写的名字。
     */
    private static List<String> leakedColumns(ColumnEdge edge, String statement) {
        List<String> hit = new ArrayList<>();
        if (statement.isEmpty()) {
            return hit;
        }
        if (isGhost(edge.getTargetColumn(), edge.getTargetTable(), statement)) {
            hit.add(edge.getTargetColumn());
        }
        for (ColumnRef ref : edge.getSources()) {
            if (isGhost(ref.getColumn(), ref.getBoundTable(), statement)) {
                hit.add(ref.nodeId());
            }
        }
        return hit;
    }

    private static boolean isGhost(String column, String table, String statement) {
        if (column == null || !column.matches("\\w+") || column.startsWith("expr_")
                || table != null && table.startsWith("#")) {
            return false;
        }
        return !statement.contains(column);
    }

    private static void printGroup(String title, List<String> items) {
        System.out.println();
        System.out.println(title + " (" + items.size() + "):");
        items.forEach(i -> System.out.println("  " + i));
    }
}
