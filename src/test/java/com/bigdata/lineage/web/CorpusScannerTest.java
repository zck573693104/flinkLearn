package com.bigdata.lineage.web;

import com.bigdata.lineage.graph.ColumnLink;
import com.bigdata.lineage.graph.LineageStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 扫描器契约：jobId 必须是「相对路径#语句序号」，坏目录不能把已有快照清掉。 */
class CorpusScannerTest {

    @TempDir
    Path dir;

    private final LineageStore store = new LineageStore();

    private CorpusScanner scanner() {
        LineageProperties properties = new LineageProperties();
        properties.setScanDir(dir.toString());
        return new CorpusScanner(store, properties);
    }

    private void write(String name, String sql) throws IOException {
        Path file = dir.resolve(name);
        Files.createDirectories(file.getParent());
        Files.write(file, sql.getBytes(StandardCharsets.UTF_8));
    }

    private Set<String> jobIds() {
        Set<String> jobs = new LinkedHashSet<>();
        for (ColumnLink link : store.graph().getColumnLinks()) {
            jobs.add(link.getJobId());
        }
        return jobs;
    }

    @Test
    void everyStatementIsTrackedBackToItsFileAndOrdinal() throws IOException {
        write("a.sql", "INSERT INTO dwd.t SELECT id FROM ods.src;\n"
                + "INSERT INTO dws.u SELECT id FROM dwd.t;");
        write("nested/b.sql", "INSERT INTO dws.v SELECT id FROM dws.u;");

        LineageStore.Snapshot snapshot = scanner().rescan(null);

        assertEquals(new LinkedHashSet<>(Arrays.asList(
                "a.sql#1", "a.sql#2", "nested/b.sql#1")), jobIds());
        assertEquals(2, snapshot.getReport().getFileCount());
        assertEquals(3, snapshot.getReport().getStatementCount());
        assertTrue(snapshot.sqlOf("a.sql#2").contains("INSERT INTO dws.u"));
        assertEquals(dir.toAbsolutePath().toString(), snapshot.getSource());
    }

    /** 没解析出血缘的文件要出现在质量视图里，而不是被静默跳过 */
    @Test
    void filesWithoutLineageAreReported() throws IOException {
        write("use.sql", "USE mydb;\n");

        scanner().rescan(null);

        assertEquals(Collections.singletonList("use.sql"),
                store.current().getReport().getFilesWithoutLineage());
        assertEquals(1, store.current().getReport().getFileCount());
        assertEquals(0, store.current().getReport().getStatementCount());
    }

    @Test
    void aBadDirectoryFailsLoudlyAndKeepsTheLastGoodSnapshot() throws IOException {
        write("a.sql", "INSERT INTO dwd.t SELECT id FROM ods.src;");
        CorpusScanner scanner = scanner();
        scanner.rescan(null);

        assertThrows(IllegalArgumentException.class, () -> scanner.rescan(dir.resolve("nope").toString()));
        assertTrue(store.graph().getNodes().containsKey("dwd.t"), "坏目录不能把已扫到的血缘清掉");
        assertTrue(store.current().sqlOf("a.sql#1").contains("ods.src"));
    }

    /** 启动扫描失败不能让页面只剩"什么都没有"：原因得能经 /api/overview 走到前端告警条 */
    @Test
    void aFailedStartupScanKeepsTheReasonForTheUi() throws IOException {
        LineageProperties properties = new LineageProperties();
        properties.setScanDir(dir.resolve("nope").toString());
        CorpusScanner scanner = new CorpusScanner(store, properties);

        assertEquals("running", scanner.getScanPhase(), "端口先于扫描就绪，初始就得是 running");
        scanner.run(null);   // run 只读配置，不读命令行参数

        assertTrue(scanner.getScanError().contains("目录不存在"), "空快照要带上为什么是空的");
        assertEquals("failed", scanner.getScanPhase());
        assertTrue(store.current().isEmpty(), "降级不阻断：服务照样起，只是没数据");

        write("a.sql", "INSERT INTO dwd.t SELECT id FROM ods.src;");
        scanner.rescan(dir.toString());
        assertNull(scanner.getScanError(), "扫成功就不能留着上一次的告警");
        assertEquals("ready", scanner.getScanPhase(), "页面据此停止轮询、收起告警条");
    }

    /** 显式跳过启动扫描时别把页面吊在"还在扫"上 */
    @Test
    void skippingTheStartupScanIsItsOwnPhase() {
        LineageProperties properties = new LineageProperties();
        properties.setRescanOnStart(false);
        CorpusScanner scanner = new CorpusScanner(store, properties);

        scanner.run(null);

        assertEquals("skipped", scanner.getScanPhase());
        assertNull(scanner.getScanError());
    }

    /** 显式传目录时以参数为准：WebUI 的「换个目录重扫」靠这个；空目录就是把快照换空 */
    @Test
    void anExplicitDirOverridesTheConfiguredOne() throws IOException {
        write("a.sql", "INSERT INTO dwd.t SELECT id FROM ods.src;");
        Path other = Files.createDirectories(dir.resolve("nested"));

        LineageStore.Snapshot snapshot = scanner().rescan(other.toString());

        assertEquals(0, snapshot.getReport().getFileCount());
        assertTrue(store.current().isEmpty(), "换目录之后旧目录的血缘不能留下来");
        assertEquals(other.toAbsolutePath().toString(), snapshot.getSource());
    }
}
