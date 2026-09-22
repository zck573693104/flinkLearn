# 多引擎 SQL 血缘解析（ANTLR4）

基于 ANTLR4 的 SQL 表级 + 字段级血缘提取工具，支持 **Flink / Spark / Presto** 三种方言，纯语法树遍历，不依赖 Calcite、Flink Planner 或任何外部 SQL 解析 jar。带一个离线可跑的 WebUI：表层级 DAG + 字段来源链路，图上每条边都能回溯到原始 SQL。

## 环境

| 项 | 版本 |
|---|---|
| JDK | 17 |
| Maven | 3.8.x（依赖已缓存在本地仓库，可 `mvn -o` 离线构建） |
| ANTLR | 4.9.3（`antlr4-maven-plugin` 从本仓库 `.g4` 生成解析器） |

## 构建与测试

```bash
mvn -o clean test          # 259 个用例
```

`clean` 是必要的：解析器由 `.g4` 生成到 `target/generated-sources`，改过语法文件后不清理会用到陈旧产物。

前端没有构建步骤，也就没有编译器帮忙兜底，所以 `web/StaticAssetIntegrityTest.java` 在 JUnit 里补了四道静态检查：块注释是否每份模块都配对闭合（JSDoc 里写 glob 路径的 `*/` 会提前关注释、把中文散文甩到代码位上，实测能让整站白屏而 255 个 Java 用例全绿）、`import './x.js'` 指向的模块在不在、`index.html` 的 `src`/`href` 指向的资源在不在、`main.js` 的 `getElementById` 在 `index.html` 里有没有对应的 `id`。

## 批量解析一个目录下的 SQL

```bash
run-lineage.bat            # 解析 ./sql
run-lineage.bat D:\dw\sql  # 解析任意目录（Windows 路径不能含空格）
```

输出每条语句的 `[类型] 输出: X <- 输入: a, b`，末尾给去重汇总：输出表清单、纯输入表清单（未出现在任何输出表中的表）、字段级体检（幽灵列/UNRESOLVED/STAR）与血缘图体检（未折叠伪节点、缺层号物理表）。

要看每条字段边的来源清单，直接给工具加 `--columns`（`run-lineage.bat` 只转发目录参数）：

```bash
MAVEN_OPTS=-Dfile.encoding=UTF-8 mvn -o compile \
  org.codehaus.mojo:exec-maven-plugin:3.1.1:java \
  -Dexec.mainClass=com.bigdata.lineage.tools.SqlDirLineageTool \
  -Dexec.args="sql --columns"
```

改完解析逻辑再核对图边数时，比对两份报告的**来源列名多重集合**比看计数靠谱：
`grep -ao "column=[a-z0-9_]*" 报告 | sort | uniq -c`。计数变化说得清"少了几个节点"，只有这份 diff 能说清"没顺手删掉真列"。

## 起 WebUI

**不需要任何数据库、消息队列或 npm 构建步骤**：血缘结果只存在进程内的不可变快照里，启动时扫一遍 SQL 目录（`POST /api/scan` 可随时换快照）。机器上没有 MySQL 也能跑完全部功能。

```bash
mvn -o clean package             # 259 用例 + 可执行 jar
java -jar target/flinkLearn-0.0.1-SNAPSHOT.jar                # 默认扫描 ./sql
java -jar target/flinkLearn-0.0.1-SNAPSHOT.jar --lineage.scan-dir=D:\dw\sql
```

打开 `http://localhost:8080`。图渲染用的 Cytoscape 已经 vendor 进 `src/main/resources/static/vendor/`（见该目录 `README.md` 的版本与来源），**没有 CDN、没有 npm 构建步骤**，内网离线可用。布局不分给 dagre：`js/graph.js` 直接拿服务端下发的 `layer` 当列号摆坐标，并在 LR / TB 两个方向里挑放缩更大的那个，所以图例里的「第几层」和图上左右顺序一定一致。

画布窄到放不下整图时**优先保字不保图**：缩放停在 10px 可读标签的下限上、从链路起点开始摆，被裁掉的那一头拖动画布能看到，右下角同时写明"画布 WxH 放不下 N 个节点"。为什么这么取舍，见方案 §8.9。

字段级血缘是**一张表一个盒子、盒子里一行一个字段、线连字段行不连盒子**：点某个字段行就只剩这一列自己的通路（上游到下游），点盒顶的表名则以那张表为中心展开。CTE / 子查询 / UNNEST 折掉的中间关系不再只藏在边的 `hops` 里，而是摊成虚线暗盒画在上下游之间——一条逻辑边拆成几段，点任意一段仍然打开原边的逐跳证据。783 列那种宽表不再拒绝渲染，而是每盒只画有边的前若干列，剩下的折成一行"还有 N 列未画"并在右下角说清有多少段链路因为两端没画而暂时不显示。取舍与实测见方案 §8.10。

界面是深色"仪器台"风格，色值只在 `css/app.css` 的 `:root` 写一处，画布（`js/badges.js` 读同一批 CSS 变量）与图例共用这一份，改主题只改一个地方；字体只用系统自带的 Bahnschrift / Cascadia Mono，**不引 web 字体**（离线多一次外链请求不值）。配色与选中态为什么这么写，见方案 §8.6。

页面如果什么都没有，顶栏下面的横幅会直接说明是哪一种：语料还在扫（端口比扫描先就绪，页面会自己等到大图出现）、目录指错（`--lineage.scan-dir` 是相对路径时按**服务进程的工作目录**解析）、还是目录里确实没有 `.sql`。后两种可以在顶栏「语料目录」填绝对路径点「重扫目录」当场换过来，不必重启。

只想要接口不要页面的话：`GET /api/overview`、`/api/tables`、`/api/graph/table`、`/api/graph/column`、`/api/table/{t}/columns`、`/api/edge/column`、`/api/issues` 全部只读，`POST /api/parse` 收一段 SQL 返回试解析结果（不落快照），`POST /api/scan` 是唯一会换快照的写操作（可带 `{"dir":"..."}`）。契约与字段口径写在方案 §7.3。

顶栏「解析新 SQL」贴一段语句点「解析并画到图上」，中栏立刻换成这次解析出的血缘——`js/parsed.js` 把 `/api/parse` 的原始 payload 拼成和快照同形的视图形状，所以点表看字段、点字段边看逐跳来源，交互与快照视图完全一致；多了「试解析·表级 / 试解析·字段」两个页签来回切，少了落库这一步。这类视图刻意不写进 hash（刷新就没了，也不该被分享），点左栏任意表列即退出。详见方案 §8.8。

## 目录结构

```
src/main/antlr4/io/github/melin/superior/parser/
  {flink,spark,presto}/antlr4/*{Lexer,Parser}.g4    三套方言语法
src/main/java/com/bigdata/lineage/
  parser/MultiEngineSQLLineageParser.java          入口：逐语句自动探测引擎并采纳结果
  parser/FlinkSQLLineageParser.java                单引擎门面
  parser/SqlSplitUtils.java                        注释/引号感知的语句切分
  parser/SqlCache.java                             LRU 结果缓存
  parser/extractor/*TableLineageExtractor.java     三方言 Visitor，表级血缘提取核心
  parser/extractor/ColumnLineageEngine.java        字段级血缘引擎，规则名驱动，三方言共用一份
  parser/scope/{QueryScope,Relation}.java          列绑定的作用域栈（CTE / 子查询 / 伪关系）
  parser/model/{TableLineage,ColumnEdge,ColumnRef}.java   结果模型（含 confidence / parseError）
  graph/                                           归一化图模型：折叠中间关系、分层 Tarjan+Kahn、不可变快照
  web/CorpusScanner.java                           目录扫描，jobId = 相对路径#语句序号
  web/api/LineageApiController.java                只读查询端点
  tools/SqlDirLineageTool.java                     目录批量解析 CLI
src/main/resources/static/                         前端：无框架、无打包，ES module 直出
src/test/java/com/bigdata/lineage/                 表级/字段级/切分/Web 四层回归
sql/                                               离线数仓语料（15 个文件，0 解析错误）
outputs/                                           方案与总结文档
```

## 约定

- **每套语法只支持本引擎方言**：Spark 有 `LATERAL VIEW` / `DISTRIBUTE BY` / `QUALIFY` / `RLIKE`，Presto 有 `CROSS JOIN UNNEST`，时态表 JOIN 与窗口 TVF 只在 Flink。补语法缺口时不要跨引擎搬运规则。
- **血缘只依赖 ANTLR 解析结果**，不引入正则或字符串猜测式的旁路解析。
- 语法有错误时 ANTLR 会做错误恢复并产出部分结果，这类结果 `parseError=true`、`confidence=0.5`，调用方必须区分对待。
- 词法保留字要成对维护：`.g4` 里 Parser 引用但 Lexer 未定义 → 隐式 token 告警；Lexer 定义但 Parser 未引用 → 该词无法再作为标识符使用，会静默吞掉列名/表名。
- 反过来也有"看着像列其实不是列"的：`current_timestamp` 这类无参关键字函数（三套语法都没为它立 token）与 lambda 形参 `x -> …`，在解析树里就是普通 `uid`。它们在 `ColumnLineageEngine` 的 `NILADIC_FUNCTIONS` 与形参作用域里被摘掉——别在 grammar 里给它们加 token，那等于把这些词从标识符里收走。

## 下一步

字段级血缘 + WebUI（M0 语法缺口 → M6 残留收敛）已全部落地，设计、口径与逐里程碑实测见
[outputs/字段级血缘与WebUI技术方案.md](outputs/字段级血缘与WebUI技术方案.md)。没有排期中的待办：**血缘结果不落库**（§10 已定稿，WebUI 不依赖任何数据库），要演进就靠新语料驱动——把 SQL 丢进 `sql/` 或 `run-lineage.bat <目录>`，语法缺口和未绑定列会自己冒出来（口径见方案 §11）。

## 历史

本分支是血缘专用分支，已移除全部 Flink 作业 / UDF / Hive / Kafka / MySQL 相关代码与依赖，只保留解析器实际用到的部分。需要那些代码请回到 `master` 分支。
