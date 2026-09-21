package com.bigdata.lineage.parser;

import java.util.Locale;

/**
 * 标识符归一化。SQL 标识符大小写不敏感，反引号/双引号只是转义手段，
 * 不统一口径会让同一个表在图里出现两个节点。
 */
public final class NameNormalizer {

    private NameNormalizer() {
    }

    /** 去引号、压缩空白、转小写；null 原样返回 null */
    public static String normalize(String raw) {
        if (raw == null) {
            return null;
        }
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            return trimmed;
        }
        char quote = trimmed.charAt(0);
        if ((quote == '`' || quote == '"') && trimmed.length() > 1 && trimmed.charAt(trimmed.length() - 1) == quote) {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        }
        return trimmed.toLowerCase(Locale.ROOT);
    }

    /** 逐段去引号后重新拼接限定名：{@code db.`Order`} -&gt; {@code db.order} */
    public static String normalizeQualified(String dotted) {
        if (dotted == null || dotted.isEmpty()) {
            return dotted;
        }
        String[] parts = dotted.split("\\.");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) {
                sb.append('.');
            }
            sb.append(normalize(parts[i]));
        }
        return sb.toString();
    }
}
