package com.bigdata.lineage.parser.model;

/**
 * 字段边的加工方式。置信度与方式一一对应，散落在各处会让 UI 徽标和数值口径不一致。
 */
public enum ColumnDerivation {

    /** 直通列：a.x -&gt; t.x */
    IDENTITY(0.95),

    /** 表达式 / 函数 / CASE / CONCAT */
    EXPRESSION(0.85),

    /** 聚合或开窗 */
    AGGREGATE(0.80),

    /** 字面量，无来源列 */
    CONSTANT(0.90),

    /** SELECT 项无名，靠 INSERT 列名清单按位置对齐得到目标列名 */
    POSITIONAL(0.70),

    /** 无别名的表达式列，目标列名是占位符 */
    UNNAMED(0.40),

    /** SELECT * / t.*，未展开为具体列 */
    STAR(0.30),

    /** 限定符在当前作用域绑不上 */
    UNRESOLVED(0.20);

    private final double confidence;

    ColumnDerivation(double confidence) {
        this.confidence = confidence;
    }

    public double getConfidence() {
        return confidence;
    }
}
