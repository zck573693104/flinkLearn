package com.bigdata.lineage.tools;

import com.bigdata.lineage.parser.MultiEngineSQLLineageParser;
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
 * 用法：java ... SqlDirLineageTool [sql目录，默认 ./sql]
 * 递归读取目录下所有 .sql 文件，解析每条语句的输入表/输出表，最后汇总去重。
 */
public class SqlDirLineageTool {

    public static void main(String[] args) throws IOException {
        Path dir = Paths.get(args.length > 0 ? args[0] : "sql");

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

        for (Path file : files) {
            String sql = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
            List<TableLineage> lineages = parser.extractTableLineages(sql);

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
    }
}
