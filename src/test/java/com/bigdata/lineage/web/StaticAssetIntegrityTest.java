package com.bigdata.lineage.web;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 零构建前端的静态完整性：这套 UI 没有打包步骤也没有 JS 测试，一个语法错就能让
 * 整页白屏，而 250 多个 Java 用例全绿也照样发布出去（实测踩过：注释里写带星号的 glob
 * 路径，星号紧跟斜杠就把块注释提前闭合，中文掉进了代码位——Java 注释同样怕这个）。
 *
 * <p>只钉三件靠文本就能判的事：注释状态机走得通、import 与 index.html 引用的文件存在、
 * main.js 取的 DOM id 存在。都是"删了/改名忘了同步"这一类静默失效，不评价样式与交互好坏。
 */
class StaticAssetIntegrityTest {

    private static final Path STATIC = Paths.get("src/main/resources/static");

    @Test
    void blockCommentsCloseInEveryModule() throws IOException {
        for (Path file : modules()) {
            List<String> problems = commentProblems(read(file));
            assertTrue(problems.isEmpty(), () -> file + " 的块注释状态机走不通（注释里出现星号紧跟斜杠，"
                + "注释就被提前关掉，后面的中文落到代码位上，浏览器整页白屏）：" + problems);
        }
    }

    @Test
    void everyImportPointsAtAnExistingModule() throws IOException {
        Pattern pattern = Pattern.compile("from\\s+'(\\./[^']+)'");
        for (Path file : modules()) {
            Matcher matcher = pattern.matcher(read(file));
            while (matcher.find()) {
                Path target = file.getParent().resolve(matcher.group(1)).normalize();
                assertTrue(Files.isRegularFile(target),
                    file + " import 了不存在的模块：" + matcher.group(1));
            }
        }
    }

    @Test
    void indexHtmlReferencesOnlyAssetsThatExist() throws IOException {
        String html = read(STATIC.resolve("index.html"));
        Matcher matcher = Pattern.compile("(?:src|href)=\"(/(?!/)[^\"]+)\"").matcher(html);
        List<String> missing = new ArrayList<>();
        int checked = 0;
        while (matcher.find()) {
            checked++;
            if (!Files.isRegularFile(STATIC.resolve(matcher.group(1).substring(1)))) {
                missing.add(matcher.group(1));
            }
        }
        assertTrue(checked >= 2, "index.html 一条本地资源引用都没扫到，多半是正则失效（checked=" + checked + "）");
        assertTrue(missing.isEmpty(), "index.html 引用了不存在的文件：" + missing);
    }

    @Test
    void domIdsUsedByMainJsExistInIndexHtml() throws IOException {
        String html = read(STATIC.resolve("index.html"));
        Matcher matcher = Pattern.compile("getElementById\\('([^']+)'\\)")
            .matcher(read(STATIC.resolve("js/main.js")));
        List<String> missing = new ArrayList<>();
        int checked = 0;
        while (matcher.find()) {
            checked++;
            if (!html.contains("id=\"" + matcher.group(1) + "\"")) {
                missing.add(matcher.group(1));
            }
        }
        assertTrue(checked >= 10, "main.js 里的 getElementById 只扫到 " + checked + " 处，多半是正则失效");
        assertTrue(missing.isEmpty(), () -> "main.js 取不到的 DOM id（拿到 null，一点就抛）：" + missing);
    }

    private static List<Path> modules() throws IOException {
        try (Stream<Path> stream = Files.list(STATIC.resolve("js"))) {
            return stream.filter((path) -> path.toString().endsWith(".js")).sorted().collect(Collectors.toList());
        }
    }

    private static String read(Path path) throws IOException {
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }

    /**
     * 逐字符走一遍状态机，报两种失效：块注释开了没关，或者被提前关掉
     * （写 glob 路径时星号紧跟斜杠就是这一类）——两种都会让开关次数对不上。
     *
     * <p>字符串字面量里的星号斜杠不计数，所以 JS 里写 glob 字符串是安全的。
     * 嵌套模板串（{@code `${` 里再套反引号}）的跟踪是近似的：只要这类串里不含
     * 块注释标记就不影响判定，本仓库当前如此。
     */
    private static List<String> commentProblems(String source) {
        List<String> problems = new ArrayList<>();
        boolean inBlock = false;
        boolean inLine = false;
        char quote = 0;
        int line = 1;
        int blockStart = 0;
        int opened = 0;
        int closed = 0;
        for (int i = 0; i < source.length(); i++) {
            char c = source.charAt(i);
            if (c == '\n') {
                line++;
                inLine = false;
                quote = 0;
                continue;
            }
            if (inLine) {
                continue;
            }
            if (inBlock) {
                if (c == '*' && i + 1 < source.length() && source.charAt(i + 1) == '/') {
                    inBlock = false;
                    closed++;
                    i++;
                }
                continue;
            }
            if (quote != 0) {
                if (c == '\\') {
                    i++;
                } else if (c == quote) {
                    quote = 0;
                }
                continue;
            }
            if (c == '/' && i + 1 < source.length()) {
                char next = source.charAt(i + 1);
                if (next == '*') {
                    inBlock = true;
                    opened++;
                    blockStart = line;
                    i++;
                    continue;
                }
                if (next == '/') {
                    inLine = true;
                    i++;
                    continue;
                }
            }
            if (c == '\'' || c == '"' || c == '`') {
                quote = c;
                continue;
            }
            if (c == '*' && i + 1 < source.length() && source.charAt(i + 1) == '/') {
                problems.add("第 " + line + " 行出现了没有配对的块注释结尾（说明上面有注释被提前关掉了）");
                i++;
            }
        }
        if (opened != closed) {
            problems.add("块注释开了 " + opened + " 次、关了 " + closed + " 次");
        }
        if (inBlock) {
            problems.add("第 " + blockStart + " 行开的块注释到文件末尾还没关");
        }
        return problems;
    }
}
