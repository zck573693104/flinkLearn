package com.bigdata.lineage.graph;

import com.bigdata.lineage.parser.model.ColumnDerivation;
import com.bigdata.lineage.parser.model.ColumnEdge;
import com.bigdata.lineage.parser.model.TableLineage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 一次扫描的质量账本：/api/overview 的数字与 /api/issues 的清单都从这里读。
 *
 * <p>为什么在扫描期算好而不是读侧现算：快照必须整体不可变，读侧若保留原始
 * {@link TableLineage} 集合，等于把一堆 Lombok 可变 DTO 暴露给并发请求。
 * 账本只留计数与字符串，图本身归 {@code LineageGraph}。
 *
 * <p>计数是全集，明细截断到 {@value #DETAIL_CAP} 条——语料再大也不能把 API 撑爆，
 * 但"有多少问题"这个数字必须是真数，否则质量视图会骗人。
 */
public final class ScanReport {

    private static final int DETAIL_CAP = 200;

    private final int fileCount;
    private final int statementCount;
    private final int parseErrorCount;
    private final int unresolvedCount;
    private final int starCount;
    private final List<String> parseErrorSql;
    private final List<String> unresolvedColumns;
    private final List<String> starColumns;
    private final List<String> filesWithoutLineage;

    private ScanReport(int fileCount, int statementCount, int parseErrorCount,
                       int unresolvedCount, int starCount, List<String> parseErrorSql,
                       List<String> unresolvedColumns, List<String> starColumns,
                       List<String> filesWithoutLineage) {
        this.fileCount = fileCount;
        this.statementCount = statementCount;
        this.parseErrorCount = parseErrorCount;
        this.unresolvedCount = unresolvedCount;
        this.starCount = starCount;
        this.parseErrorSql = Collections.unmodifiableList(parseErrorSql);
        this.unresolvedColumns = Collections.unmodifiableList(unresolvedColumns);
        this.starColumns = Collections.unmodifiableList(starColumns);
        this.filesWithoutLineage = Collections.unmodifiableList(filesWithoutLineage);
    }

    /** 还没扫过的账本：全 0 空清单，省得读侧到处判 null */
    public static ScanReport empty() {
        return new ScanReport(0, 0, 0, 0, 0, new ArrayList<String>(),
                new ArrayList<String>(), new ArrayList<String>(), new ArrayList<String>());
    }

    /**
     * @param filesWithoutLineage 解析后既无输入也无输出的文件，多半是语法缺口
     */
    public static ScanReport of(int fileCount, List<String> filesWithoutLineage,
                                List<TableLineage> statements) {
        List<String> errors = new ArrayList<>();
        List<String> unresolved = new ArrayList<>();
        List<String> star = new ArrayList<>();
        int errorCount = 0;
        int unresolvedCount = 0;
        int starCount = 0;
        int counted = 0;
        for (TableLineage lineage : statements) {
            if (lineage == null) {
                continue;
            }
            counted++;
            if (lineage.isParseError()) {
                errorCount++;
                addDetail(errors, snippet(lineage.getOriginalSql()));
            }
            for (ColumnEdge edge : nullSafe(lineage.getColumnEdges())) {
                if (edge.getDerivation() == ColumnDerivation.UNRESOLVED) {
                    unresolvedCount++;
                    addDetail(unresolved, describe(edge) + " <- " + edge.getSources());
                } else if (edge.getDerivation() == ColumnDerivation.STAR) {
                    starCount++;
                    addDetail(star, describe(edge));
                }
            }
        }
        return new ScanReport(fileCount, counted, errorCount, unresolvedCount, starCount,
                errors, unresolved, star, new ArrayList<>(filesWithoutLineage));
    }

    private static List<ColumnEdge> nullSafe(List<ColumnEdge> edges) {
        return edges == null ? Collections.<ColumnEdge>emptyList() : edges;
    }

    /** 边的问题描述只取一次，明细里带上来源便于直接判断是哪类缺口 */
    private static String describe(ColumnEdge edge) {
        return edge.getTargetTable() + "." + edge.getTargetColumn()
                + "[" + edge.getDerivation() + "]";
    }

    private static void addDetail(List<String> list, String item) {
        if (list.size() < DETAIL_CAP) {
            list.add(item);
        }
    }

    private static String snippet(String sql) {
        if (sql == null) {
            return "";
        }
        String flat = sql.replaceAll("\\s+", " ").trim();
        return flat.length() <= 160 ? flat : flat.substring(0, 160) + "...";
    }

    public int getFileCount() {
        return fileCount;
    }

    public int getStatementCount() {
        return statementCount;
    }

    public int getParseErrorCount() {
        return parseErrorCount;
    }

    public int getUnresolvedCount() {
        return unresolvedCount;
    }

    public int getStarCount() {
        return starCount;
    }

    public List<String> getParseErrorSql() {
        return parseErrorSql;
    }

    public List<String> getUnresolvedColumns() {
        return unresolvedColumns;
    }

    public List<String> getStarColumns() {
        return starColumns;
    }

    public List<String> getFilesWithoutLineage() {
        return filesWithoutLineage;
    }
}
