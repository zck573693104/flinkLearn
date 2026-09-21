package com.bigdata.lineage.web;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import com.bigdata.lineage.graph.LineageStore;
import com.bigdata.lineage.graph.ScanReport;
import com.bigdata.lineage.parser.MultiEngineSQLLineageParser;
import com.bigdata.lineage.parser.model.ColumnEdge;
import com.bigdata.lineage.parser.model.TableLineage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 语料目录扫描器：读 .sql → 逐语句解析 → 写语句标识 → 建图 → 原子换快照。
 *
 * <p>jobId 在这里定为「相对路径#该文件的血缘语句序号」（如 {@code dwd/part3.sql#7}），
 * 而不是图构建层默认的内容哈希：UI 上的每条边都要能回答"这条血缘是哪份文件哪条语句给的"，
 * 哈希答不上来。{@code ColumnEdge.jobId} 解析层不填，正是留给调用方的这个口子。
 */
@Slf4j
@Component
public class CorpusScanner implements ApplicationRunner {

    /** 缓存返回的是同一批可变 DTO，逐文件回写 jobId 会污染上一轮，所以整个扫描器不带缓存 */
    private final MultiEngineSQLLineageParser parser = new MultiEngineSQLLineageParser(false);

    private final LineageStore store;
    private final LineageProperties properties;

    public CorpusScanner(LineageStore store, LineageProperties properties) {
        this.store = store;
        this.properties = properties;
    }

    /** 启动扫描失败只降级不阻断：目录没准备好也要能把服务和静态页起来 */
    @Override
    public void run(ApplicationArguments args) {
        if (!properties.isRescanOnStart()) {
            log.info("lineage.rescan-on-start=false，跳过启动扫描");
            return;
        }
        try {
            rescan(null);
        } catch (RuntimeException | IOException e) {
            log.warn("启动扫描 {} 失败，WebUI 以空快照启动：{}", properties.getScanDir(), e.getMessage());
        }
    }

    /**
     * 重扫并把结果换成新快照。
     *
     * @param dir 覆盖配置目录；为空则用 {@code lineage.scan-dir}
     * @throws IOException          目录不可读
     * @throws IllegalArgumentException 目录不存在——让 API 回 400，而不是把已有快照清空
     */
    public synchronized LineageStore.Snapshot rescan(String dir) throws IOException {
        Path root = resolve(dir);
        if (!Files.isDirectory(root)) {
            throw new IllegalArgumentException("目录不存在：" + root);
        }
        long start = System.currentTimeMillis();
        List<Path> files = sqlFiles(root);

        List<TableLineage> statements = new ArrayList<>();
        Map<String, String> sqlByJob = new LinkedHashMap<>();
        List<String> withoutLineage = new ArrayList<>();
        for (Path file : files) {
            String relative = root.relativize(file).toString().replace('\\', '/');
            List<TableLineage> parsed = read(file, relative, withoutLineage);
            for (int i = 0; i < parsed.size(); i++) {
                TableLineage lineage = parsed.get(i);
                String jobId = relative + "#" + (i + 1);
                stamp(lineage, jobId);
                sqlByJob.put(jobId, lineage.getOriginalSql());
                statements.add(lineage);
            }
        }

        ScanReport report = ScanReport.of(files.size(), withoutLineage, statements);
        store.replace(statements, report, root.toString(), sqlByJob, System.currentTimeMillis() - start);
        log.info("扫描 {} 完成：{} 个文件、{} 条血缘语句、{} 条字段边，用时 {}ms",
                root, files.size(), report.getStatementCount(),
                store.graph().getColumnLinks().size(), System.currentTimeMillis() - start);
        return store.current();
    }

    private Path resolve(String dir) {
        String raw = dir == null || dir.trim().isEmpty() ? properties.getScanDir() : dir.trim();
        return Paths.get(raw).toAbsolutePath().normalize();
    }

    private static List<Path> sqlFiles(Path root) throws IOException {
        try (Stream<Path> stream = Files.walk(root)) {
            return stream.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().toLowerCase().endsWith(".sql"))
                    .sorted()
                    .collect(Collectors.toList());
        }
    }

    /** 读不下来的文件同样记进 filesWithoutLineage：质量视图要能看见它，不能悄悄漏掉 */
    private List<TableLineage> read(Path file, String relative, List<String> withoutLineage) {
        String sql;
        try {
            sql = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
        } catch (IOException e) {
            log.warn("读取 {} 失败：{}", relative, e.getMessage());
            withoutLineage.add(relative + " (读取失败)");
            return new ArrayList<>();
        }
        List<TableLineage> parsed = parser.extractTableLineages(sql, false);
        if (parsed.isEmpty()) {
            withoutLineage.add(relative);
        }
        return parsed;
    }

    private static void stamp(TableLineage lineage, String jobId) {
        List<ColumnEdge> edges = lineage.getColumnEdges();
        if (edges == null) {
            return;
        }
        for (ColumnEdge edge : edges) {
            edge.setJobId(jobId);
        }
    }
}
