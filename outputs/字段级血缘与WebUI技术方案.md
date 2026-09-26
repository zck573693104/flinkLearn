# 字段级血缘 + WebUI 技术方案

> 分支：`lineage-only-webui`（本方案实施分支，血缘专用、无 Flink 依赖）
> 派生自：`feat-column-lineage-webui` @ `546ae69`
> 日期：2026-09-21（最后更新 2026-09-22）
> 状态：**M0–M8 已全部落地**，§2–§13 是设计依据，§8.4–§8.13 与 §14 是逐项实测记录

## 0. 文档定位

本方案是字段级（列级）血缘 + WebUI 展示的唯一现行设计。

历史上两份 Calcite 路线的文档（`outputs/列级血缘实现方案.md`、`outputs/列级血缘实现总结.md`）**已随 §2.0 的文档清理删除**，需要时可从 `master` 取回。它们作废的理由记录在这里，避免后来者重走：

| 旧文档 | 作废原因 |
|---|---|
| `列级血缘实现方案.md` | 基于 Calcite `RelMetadataQuery.getColumnOrigins()`（Route B），依赖 Flink Planner 的 `RelNode`。该路线在当前代码里是占位符且已被明确移除（提交 `8df7442`），与"血缘关系只依赖 antlr4 解析"的约束冲突 |
| `列级血缘实现总结.md` | 描述的是上述 Calcite 方案的"已完成"状态，与现状不符 |

立项时（2026-09-21）仓库**没有任何列级血缘代码**（已核查：`parser/model/` 与 `model/` 两套 DTO 全部是表级字段，无 `Column*` 结构）。本方案是新建，不是改造遗留物；§4–§8 描述的东西现已全部在仓库里，落地情况以各节末尾的实测记录为准。

## 1. 需求拆解

用户诉求："用 webui 展示表的血缘层级关系以及字段关系，清晰展示出字段的来源"。拆成三件必须可验证的事：

1. **表级层级关系**：DAG 按数据流方向分层排布（ODS → DWD → DWS → ADS），能看上下游链路、能定位某个表在第几层、能按方向（上游/下游/双向）和深度裁剪子图。
2. **字段关系**：给定目标表的一个字段，能列出它的来源字段（跨 CTE、跨子查询的多跳链路），每条边标注加工方式（直通 / 表达式 / 聚合 / 常量 / 星号未展开）。
3. **来源可追溯**：每条字段边都要能回指到证据——哪个文件、哪条语句、SELECT 列表第几位、原始表达式文本。UI 上点开即见 SQL 片段。

已确认的三个决策（本方案据此定型）：

- ① **v1 不做 `SELECT *` 展开**：星号画成一条表级未展开边，带 `STAR` 徽标，不猜列名。
- ② **引入 `spring-boot-starter-web:2.7.18`** 承载 Web 层。
- ③ **图形库 vendored**：JS 库文件提交进 `src/main/resources/static/vendor/`，不引 webjars、不依赖 CDN、不引前端构建链。

## 2. 现状盘点（结论均可按 文件:行号 复核）

### 2.0 本分支已完成的结构收敛（先读这节；§7 pom、§9 里程碑都以它为前提）

#### 2.0.1 为什么不拆多模块，而是直接删

最初的想法是"血缘 + WebUI 拆成独立 Maven 模块，与 Flink 作业共存于一个 reactor"。实测否掉了省事版本：

```
$ mvn validate   # 根 pom 保持 <packaging>jar</packaging> + <modules>lineage-web</modules>
[ERROR] 'packaging' with value 'jar' is invalid. Aggregator projects require 'pom' as packaging.
```

即"根 pom 原地不动、只挂一个无 parent 的子模块"Maven 3.8.5 直接拒绝。要做真多模块，根 pom 必须变 `pom` 打包，于是 **61 个 Flink 类 + `src/main/resources/{config/krb5.ini,user.keytab,local.properties,hbase-1.sql}` 全得搬家**，`run-lineage.bat` 也要跟着改。

前置核查给出了更简单的解法——血缘与非血缘代码**双向零引用**：

| 核查项 | 结果 |
|---|---|
| 非血缘代码 import 血缘（`grep -rn "import com.bigdata.lineage" src` 排除血缘自身） | 0 |
| 血缘代码 import 非血缘（`^import com.bigdata` 且不含 `.lineage`） | 0 |
| 血缘的外部依赖 | 只有 `io.github.melin`（本仓库 `.g4` 生成）、`org.antlr.v4`、`lombok`、JDK、JUnit |
| 血缘包里的 `org.apache.flink` / `org.apache.calcite` | 0（Route B 早在 `8df7442` 移除） |

没有共生需求 → 删掉 Flink 侧，单模块 + 干净依赖树。**历史代码在 `master` 完整保留**，本分支不试图兼容它。

#### 2.0.2 删除清单（`git diff --cached --stat`：**83 个跟踪文件删除、−4207 行，pom/yml 两处改写 +24 行**）

| 类别 | 内容 |
|---|---|
| Flink 作业与 UDF | `com/bigdata/{agg,constants,demo,job,leetcode,process,udf,utils}` 共 61 个类 |
| 死的手写解析基类 | `src/main/java/io/github/melin/**/Base{Flink,Presto}SqlParser.java`（无 `.g4` 声明 `superClass`，零引用） |
| 表级半成品存储三件套 | `lineage/controller/TableLineageController.java`、`lineage/service/TableLineageService.java`、`lineage/model/TableLineage.java`（零引用；`model.TableLineage` 与 `parser.model.TableLineage` 重名混淆）。即原 §9 M6 清理项，提前完成 |
| 违反"只依赖 antlr4"的旁路解析 | `com/bigdata/SqlCommandParser.java`（正则式 SQL 语句分类器）、`com/bigdata/Demo.java` |
| 非标准源码目录 | `src/main/test/com/{CoordinateTransformUtil,GPSConverterUtils,Test}.java`（Maven 从不编译该路径） |
| 运维脚本与残留 | `sh/**`（8 个 Hive/CK 部署脚本）、`flink-sql-lineage-parser/pom.xml`（空壳残留）、`superior-sql-parser-temp`（误提交的 mode 160000 gitlink、无 `.gitmodules`，`213dd0d` 当时只移了索引项；磁盘上那份的删除记录见 §2.0.4 末段） |
| 无关资源 | `hbase-1.sql`、`local.properties`、`config/{krb5.ini,user.keytab}` |

#### 2.0.3 保留结果与验证

```
src/main/antlr4/io/github/melin/superior/parser/{flink,spark,presto}/antlr4/*.g4   6 份语法
src/main/java/com/bigdata/lineage/parser/**                                       7 个类
src/main/java/com/bigdata/lineage/parser/model/TableLineage.java
src/main/java/com/bigdata/lineage/tools/SqlDirLineageTool.java
src/test/java/com/bigdata/lineage/parser/**                                       4 个测试类
src/main/resources/{application.yml, log4j2.xml}
```

- `pom.xml` 依赖从 ~30 项砍到 **8 项**：`antlr4-runtime:4.9.3`、`lombok:1.18.28`、`slf4j-api:1.7.25` + log4j2 三件套 `2.9.1`、`junit-jupiter`/`junit-vintage-engine:5.9.3`、`junit:4.13.2`。全部 Flink/hive/hbase/kafka/mysql/HikariCP/jedis/easyexcel/fastjson2/obs/commons/spring-expression 已移除；插件只留 `antlr4-maven-plugin` + compiler/resources/surefire。
- `mvn -o clean test` → **125/125**（Flink 31 / SparkPresto 48 / MultiEngine 37 / SqlSplitUtils 9），耗时降到 **9.7s**。
- `SqlDirLineageTool sql` → 文件数 16、输出表 17、纯输入表 0，**与清理前基线一致**。
- `run-lineage.bat` 无需改动（仍是根目录 `mvn exec:java -Dexec.mainClass=...SqlDirLineageTool`）。
- `junit-vintage-engine` + JUnit4 必须保留：`FlinkSQLLineageParserTest.java:5,7` 用的仍是 `org.junit.Test` / `org.junit.Assert`。
- `application.yml` 去掉 mysql/mybatis/jsqlparser 三段虚构配置（依赖不在 pom、`com.bigdata.lineage.model` 包已删），只留 `server.port` 与规划中的 `lineage.scan-dir`。

#### 2.0.4 文档与残留收敛（同批完成）

- 删除 **33 份历史 md**：根目录 27 份一次性报告（`BUG_FIX_REPORT.md`、`COMPILATION_*`、`TEST_*`、`SUPERIOR_PARSER_*`、`FLINK_PARSER_*`、`PROJECT_COMPLETION_REPORT.md`、`Flink SQL 血缘实现方案.md`、`superior-sql-parser-analysis.md` 等）+ `outputs/` 下 6 份旧总结/旧方案。全部是跟踪文件，`git checkout master -- <path>` 可取回。
- **重写 `README.md`**：旧版还在描述 Flink 作业与已删除的 superior 集成；新版只讲血缘解析器（环境、`mvn -o clean test`、`run-lineage.bat`、目录结构、三方言各管自己的语法约定、指向本方案）。`LICENSE.txt` 保留。
- 删除 `run-test.bat` + `classpath.txt`：前者调用 `com.bigdata.lineage.LineageSystemTest`，该类不存在；`classpath.txt` 只服务于它。
- 磁盘未跟踪垃圾已删：`cep-core/`（仅 5 个构建产物 `.class`/`.lst`/`test_output.log`，无源码）、`cep-web/`（空目录）、`compile_error.log`、`compile_result.log`、`tmpdb/`（含 16 份涉密语料副本，按约定提交前必删，源文件在 `D:\ai coding\...` 可再拷）。

**已确认删除（2026-09-21），代价记录在这里**：`superior-sql-parser-temp/` 是一个**独立的嵌套 git 仓库**（自带 `.git`，remote 为 `github.com/melin/superior-sql-parser`），其 HEAD `94fd069e`（分支 `flink-only-jdk17-dev`，"完成 Flink SQL 解析器 JDK 17 升级和模块精简"）**不存在于任何远端分支**，我们仓库历史上也只有一个 160000 gitlink 指针、没有它的对象。你回"需要"后目录已 `rm -rf`，**但删前没按本节的建议做 `git bundle` 导出**，该孤立提交已不可恢复（已核实上游 8 个分支 head 无一为 `94fd069e`，也没有 `flink-only-jdk17-dev`）。影响面有限：那是对第三方 superior-sql-parser 的本地 JDK 17 化改造，本分支的路线早已确定不引入它的 jar（§2.0 起我们只维护自己那 6 份 `.g4`）；若将来还要一份 JDK17 的 g4 参考，只能重新 clone 上游再改。唯一的补救渠道是磁盘级快照（Windows 文件历史记录/卷影副本），不在仓库内。

**流程教训**：删任何东西前先 `grep` 仓库文档对该路径的说明——这条建议本仓库自己写了 23 行，我是删完才读到的。

#### 2.0.5 与血缘无关的残留二次清理（2026-09-21 收尾）

方向确认：**WebUI 不接任何数据库**，血缘结果只活在进程内的不可变快照里，语料从磁盘扫。那么"数据"侧的文件也就没有存在理由了。这一批删的都是**跟踪文件**，`git show <commit>^:<path>` 随时取回——和 §2.0.4 那个嵌套仓库不是一回事。

| 删除 | 为什么与血缘无关 |
|---|---|
| `compile_output.log` | 9/19 的一次性编译日志，30KB，没人读 |
| `fix_chinese.py` | 把 `src/main/java` 里中文批量换成英文的一次性脚本；替换表含 `设置`→`Set`、`获取`→`Get` 这种高频词，现在全套中文注释与告警文案喂给它会被毁掉，留着就是地雷 |
| `setup-flink-parser.ps1` | 服务对象是已删的 `superior-sql-parser-temp/`（clone melin 上游、建 `flink-only-jdk17-dev`、删 11 个 superior 模块）。它顺带是那段 JDK 17 改造的唯一文字记录，git 历史里还在（`be3b22d`） |
| `setup-utf8.ps1` | PowerShell 编码设置；README 已用 `MAVEN_OPTS=-Dfile.encoding=UTF-8` 与纯 ASCII 的 `run-lineage.bat` 覆盖这个需求 |
| `sql/init_lineage_db.sql` | MySQL 建表脚本（`ENGINE=InnoDB`、`AUTO_INCREMENT`、Kafka→MySQL 示例数据）。既不是语料也不是本分支路线；而且**语料里那 3 条 parseError 全部出自它的三段 DDL**——我们只有 Flink/Spark/Presto 三套语法，MySQL 建表语句当然解析不过去 |

删完不重启、直接 `POST /api/scan` 换快照（这一步本身就是"运行时扫盘、没有库"的证明）：`/api/overview` 从 16 文件 / 26 语句 / 3 parseError / 17 表 → **15 文件 / 22 语句 / 0 parseError / 14 表**；图列边 2327、表边 6、UNRESOLVED 0、STAR 0、环 0、maxLayer 1 **一项都没动**——删掉的文件本来就不产出血缘。`.gitignore` 补了 `*.log` 防编译日志再进仓库。

**零数据库启动实测**：`mvn -o clean package` → 252 绿 + 21.7MB boot jar；启动前 `netstat` 确认 3306/5432 无监听（机器上根本没有数据库进程）；`java -jar` 直接起，`GET /`、三个静态资源、9 个端点全 200；真浏览器（browser-use）里表级 DAG 渲染 14 表 / 6 边，字段链路 `user_app_data.car_vin` 出 19 节点 / 18 边，右栏逐跳证据带 derivation 与置信度，console 零 error（只剩一条 cytoscape 自己关于 wheel sensitivity 的提示）。

### 2.1 Web 层：清理前的现状（下表部分行已被 2.0 的删除动作作废，保留作为决策依据）

| 事实 | 证据 |
|---|---|
| 无 Spring Boot 骨架：全仓库无 `@SpringBootApplication` / `SpringApplication.run` / `@MapperScan` | `grep` 零命中 |
| `pom.xml` 无 `<parent>`、无 `<packaging>`、无 `spring-boot-maven-plugin` | `pom.xml:5-9` |
| 唯一 spring 依赖是 `spring-expression:5.3.26` | `pom.xml:285-286` |
| `src/main/resources/` 下无 `static/`、`templates/`、`public/`、`webapp/`；全仓库零 `.js/.html/.css` | `find src/main/resources` → 只有 `application.yml`、`config/`、`hbase-1.sql`、`local.properties`、`log4j2.xml` |
| `application.yml` 是**死配置**（端口/数据源/mybatis 都无人读取） | `server.port: 8080`、`spring.datasource.url`、`mybatis.mapper-locations`（mybatis 不在 pom、无 mapper 目录） |
| `TableLineageController` 是**普通类**，不是 REST 控制器 | `controller/TableLineageController.java:13` `public class TableLineageController {`；零命中 `@RestController\|@GetMapping\|@ExceptionHandler\|WebMvcConfigurer` |
| `dao/` 是空目录；Service 是纯内存 Map | `service/TableLineageService.java:21` `private final Map<String, List<TableLineage>> lineageCache = new HashMap<>();` |
| 现有 `sql/init_lineage_db.sql` 的 `column_lineage` 表**不足以支撑字段溯源** | `sql/init_lineage_db.sql:35-42` 只有 `id/lineage_id/source_column/target_column`，缺 source_table/target_table/job_id/derivation/confidence/engine/唯一键（该文件已在 §2.0.5 删除，原文见 git 历史）|

结论：需要新建 Web 骨架 + 新建持久化模型；旧 `controller`/`service`/`model` 三件套是"表级 + 手写 Map 返回"的半成品，本方案**不复用它**（避免再造一套并行的血缘存储），而是在其上建立新的只读查询层，旧三件套列为 M6 清理项。

### 2.2 解析层：表级已稳定，列级只有缺口没有地基

- 入口：`MultiEngineSQLLineageParser.extractTableLineages(String)` → `:50`；引擎自动探测顺序 Flink→Spark→Presto `:110/:116/:122`；采纳条件 `isCleanLineage = !parseError && hasLineage()` `:148-150`；LRU 1000 缓存 `SqlCache.java:12-20`。
- 结果模型：`parser/model/TableLineage.java:17-62`（`targetTable/sourceTables/processType/insertMode/confidence/hasCte/hasTemporalJoin/hasWindowFunc/parseError/originalSql`）。
  ⚠ 已知坑：`confidence = 0.95` 没有 `@Builder.Default`，任何 `builder()` 不显式 `.confidence()` 得到 0.0。列级模型必须避开这个 Lombok 陷阱。
- 三方言 Visitor：`extractor/TableLineageExtractor.java`（Flink）、`SparkTableLineageExtractor.java`、`PrestoTableLineageExtractor.java`。三者结构对称，状态字段 `targetTable/sourceTables/cteNames/processType/insertMode/hasCte/...`，`addSourceTable` 用扁平 `cteNames` 过滤（Flink `:216`、Spark `:209`）。
- Grammar 规模：Flink parser 436 行 / 62 规则，Spark 537 / 75，Presto 469 / 68。
- 表级语料基线（不能破坏）：`run-lineage.bat sql` → 16 文件 / 17 输出表 / 3 WARN（MySQL 初始化脚本）；`mvn clean test` → 125/125。M5/M6 收尾后为 **15 文件 / 14 输出表 / 0 WARN**（那三段 MySQL DDL 已删，见 §2.0.5）、252 用例——当前有效基线以 §14 第 1 条为准。

### 2.3 做列级血缘要跨过的 6 个具体障碍（逐条已核实）

| # | 障碍 | 位置 | 影响 |
|---|---|---|---|
| 1 | Visitor 无作用域栈：`visit` 一路 `visitChildren` 下钻，子查询/CTE 的 CTE 名与别名全局共享，只有扁平 `Set<String> cteNames` | `TableLineageExtractor.java:89,121-133` | 嵌套 CTE 名会泄漏进兄弟/外层作用域；列绑定必须作用域化 |
| 2 | `tableReference` 的别名（`alias : KW_AS? uid`，Flink:298 / Spark:357 / Presto:351）**解析了但从未读取** | Flink `tableReference:115-124` | 无法把 `a.col` 绑定回物理表 |
| 3 | `columnRef : uid (DOT uid)*` 是平铺的，末段是列名、前缀是限定符，但规则本身不区分 | Flink `:245`（注释记录了为什么不能用 `tablePath DOT uid`） | 需按"末段=列名、其余=限定符"约定解析，不能改回嵌套规则 |
| 4 | `columnDef : MULT \| expression (alias)?`：有 `alias()` 时可取名，**无别名的表达式列没有名字** | Flink `:97-100` | 需要命名推导 + 位置对齐兜底 |
| 5 | `cteDefinition : cteName KW_AS LPAREN queryExpression RPAREN`，**没有列名清单**（三套 grammar 都缺） | Flink `:74-76` / Spark `:79-81` / Presto `:64-66` | `WITH t(a,b) AS (...)` 无法解析；CTE 输出列名只能从内层 SELECT 推 |
| 6 | Spark `lateralView` 的表别名是**无标签 `uid?`**，生成的 API 只给平铺 `uid()` 列表；Presto `UNNEST` 分支 `return null` 直接丢弃 | Spark `:140-143`（`SparkSqlParser.java:2191` 平铺 API）、Presto `PrestoTableLineageExtractor.java:177` | 行/列别名无法区分，`explode` 产出列断链 |

好消息（省掉一条语法改造）：`primaryExpression : tablePath DOT MULT`（Flink:230 / Spark:288 / Presto:241）已经存在，所以 `t.*` 能解析；裸 `*` 由 `columnDef : MULT` 覆盖。**星号检测不需要新规则**，只需在 Visitor 里识别这两处。

## 3. 总体架构

```
┌── 解析层（纯 ANTLR4，无新依赖）────────────────────────────────┐
│ FlinkSqlParser / SparkSqlParser / PrestoSqlParser              │
│   ↓ ColumnLineageExtractor（三方言各一，对称扩展）              │
│   QueryScope 作用域栈 + 别名绑定 + 列命名推导                    │
│   ↓ List<ColumnEdge>  +  已有 List<TableLineage>               │
└──────────────────────────┬─────────────────────────────────────┘
                           ↓
┌── 归一化 / 图构建层 ──────────────────────────────────────────┐
│ NameNormalizer（小写、去反引号、catalog.schema.table 补齐）     │
│ ColumnGraphBuilder：子查询伪节点折叠、别名解引用、多跳拼接        │
│ LayeredDagBuilder：Tarjan 缩环 + Kahn 最长路径分层              │
│ LineageStore（不可变快照 + AtomicReference 原子替换）           │
└──────────────────────────┬─────────────────────────────────────┘
                           ↓
┌── 服务层（Spring Boot 2.7.18，新增）─────────────────────────┐
│ LineageWebApplication + CorpusScanner(lineage.scan-dir)        │
│ /api/overview /api/tables /api/graph/table /api/graph/column  │
│ /api/table/{t}/columns /api/edge/column /api/issues           │
│ /api/parse(POST) /api/scan(POST)                              │
└──────────────────────────┬─────────────────────────────────────┘
                           ↓
┌── 前端层（static/ + vendored cytoscape，无构建链）────────────┐
│ 三栏：左 表列表/搜索/层级过滤 | 中 图（表级 DAG ⇄ 字段链路）     │
│       右 详情面板（来源清单 + derivation 徽标 + SQL 证据高亮）   │
└──────────────────────────────────────────────────────────────┘
```

数据流单向：语料目录 → 解析 → 归一化 → 快照 → REST → UI。UI 不做任何血缘推断，只渲染和裁剪。

## 4. 解析层设计：字段级血缘

### 4.1 新增数据模型（`com.bigdata.lineage.parser.model`，M1 已落地，字段以下为准）

```java
/** 一个字段的限定引用：解析期的中间产物，未保证绑定到物理表 */
public class ColumnRef {
    String qualifier;   // 限定符：表名或别名，可为 null（未限定）
    String column;      // 列名，归一化小写、去反引号
    String boundTable;  // 作用域里解析出的关系（物理表全名/CTE 名/子查询名），未绑定为 null
    String rawText;     // 原始文本，仅用于 UI 证据展示（字段路径 info_str 之类靠它）
    boolean isResolved();
    String nodeId();    // table.column，未绑定时退化为 qualifier.column 或裸列名
}

/** 一条字段级血缘边：statement 粒度，target 侧唯一 */
public class ColumnEdge {
    String targetTable;      // 归一化全限定表名，或语句内关系名（CTE / 子查询别名 / #sub1）
    String targetColumn;
    List<ColumnRef> sources; // 该目标字段的来源列集合（表达式可多个，常量为空）
    ColumnDerivation derivation; // 见 4.5 枚举，confidence 由枚举给出
    String transform;        // 归一化表达式文本，如 concat(a, '-', b)
    int ordinal;             // SELECT 列表下标，用于位置对齐与证据定位
    String jobId;            // 文件路径 + 语句序号，如 job_03.sql#7；单独解析时为 null
    String engine;           // FLINK | SPARK | PRESTO，建边时一次写定，避免在缓存共享对象上补字段
    boolean targetInternal;  // 目标关系是不是引擎内部中间关系，图构建层据此折叠（§6）
}
```

`targetInternal` 是 M2 加的：中间关系（子查询、展开行）只有引擎知道，所以由边带标记而不是让下游猜名字。M5 之后中间关系一律 `#sub/#lat/#unnest` 命名，但标记照留——判定不该抄一份起名规则（§6）。

同时给 `TableLineage` 增一个字段（不复用旧 DTO）：

```java
private List<ColumnEdge> columnEdges;   // @Builder.Default = Collections.emptyList()
```

**Lombok 约束（必须遵守）**：`TableLineage.java:37` 的 `confidence = 0.95` 缺 `@Builder.Default`，同类问题在新模型里一律用 `@Builder.Default` 显式声明，否则 builder 造出的边 confidence 恒为 0。顺手在本次改动里补上 `TableLineage` 的 `@Builder.Default`（`confidence/hasCte/hasTemporalJoin/hasWindowFunc` 四个字段）。

### 4.2 作用域模型（M1 已按此实现）

```java
final class QueryScope {
    final QueryScope parent;
    final Map<String, Relation> byKey;   // 关系名 / 别名 / 末段短名 → 关系，先到先得
    final List<Relation> relations;      // 顺序即位置，未限定列的唯一候选判定用
}

final class Relation {
    String name;              // 物理表全名、CTE 名或伪关系名 #sub<k>/#lat<k>/#unnest<k>
    List<String> knownColumns;// 派生关系声明得出来的列；空 = 表头未知
}
```

作用域规则（写死在实现里，避免歧义）：

- 每个查询表达式（外层 SELECT、集合操作分支、子查询、CTE 体、标量子查询）开一层 `QueryScope`，父层只用于**限定符**查找（相关子查询要引用外层表）；未限定列**不向父层兜底**——内层自己就有表却容不下这列时是真歧义，猜给外层表会编造来源。
- `tableReference` 的每个分支注册一个 `Relation`（JOIN 右侧的 `tablePath`/`(queryExpression)` 是同节点的直接子节点，一次登记到位）：
  - `tablePath (alias)?` → 物理表；同名已被 CTE 占用时沿用 CTE 关系，不重复登记
  - `LPAREN queryExpression RPAREN (alias)?` → 有别名用别名当关系名，无别名用伪节点 `#sub<k>`
  - `tvfFunction`（Flink）→ 取首参表名登记为同一关系，窗口列不产列依赖
  - `KW_UNNEST ... (aliasWithColumns | alias)?`（Presto/Flink）与 `lateralView`（Spark）→ 展开列挂到伪关系；`posexplode` 的下标列与 `WITH ORDINALITY` 的末列都是 CONSTANT，没有上游字段
  - `temporalClause`（Flink）→ 时态表按普通关系登记
- 伪关系（`#` 前缀）只服务列绑定，图构建层把它折掉；CTE 是"字段来源"的中间站，保留成节点。
- 集合操作在语法里右递归（`A UNION B INTERSECT C` → `A UNION (B INTERSECT C)`），分支必须递归走完，只看直接子节点会静默丢掉末分支；各分支共用同一套输出列名，没有显式列清单时由第一分支定名。
- CTAS/CREATE VIEW 的显式列清单（Flink 包在 `tableElement` 里，Spark/Presto 直接挂 `columnDefinition`）同样按位置命名目标列。

### 4.3 目标列命名与来源提取（每个 SELECT 一次）

对绑定到 INSERT/CTAS/CREATE VIEW 的**最外层** `selectClause`，逐 `columnDef`（下标 = `ordinal`）：

1. `MULT` 或 `primaryExpression: tablePath DOT MULT` → `derivation=STAR`，产一条 **表级未展开边**（target 列名记 `*`，source = 该限定符绑定的关系），不猜列。
2. 有 `alias` → `targetColumn = alias.uid` 归一化。
3. 无 `alias` 且表达式是纯 `columnRef` → `targetColumn =` 末段（例如 `SELECT a.emp_id FROM ...` → `emp_id`）。
4. 无 `alias` 且是函数/算术表达式 → 命名不确定，`targetColumn = <unnamed#ordinal>`，`derivation=UNNAMED`；**若 `insertStatement` 带 `columnNameList`，按 ordinal 对齐到真实列名**，`derivation=POSITIONAL`（障碍 #4 的主解法；`columnNameList` 规则三套都在，Flink:184/Spark:245/Presto:204，目前解析了从不读）。
5. 从表达式树收集 `columnRef` 叶子 → `ColumnRef(qualifier = 除末段外的部分, column = 末段)`。
6. 绑定 qualifier（顺序严格）：
   - 有 qualifier 且命中 `byAlias` → 该关系
   - 有 qualifier 且等于本层可见 CTE 名 → 派生关系
   - qualifier 恰等于某物理表全限定名或其末段 → 该关系
   - 无 qualifier 且本层**只有一个**关系 → 该关系（唯一候选规则）
   - 否则 `UNRESOLVED`
7. 绑定结果若是派生关系（CTE/子查询），继续递归进入它的 `outputColumns` 找到对应的来源列，一直回溯到物理表 → 这就是"多跳链路"的来源。**跳与跳之间各产一条边**，由图构建层串成链；不在解析层伪造跨语句的边。

聚合/开窗：`functionCall` 命中 `SUM/COUNT/AVG/MAX/MIN/COLLECT_*` 或带 `KW_OVER` → `derivation=AGGREGATE`。

### 4.4 与表级血缘的一致性约束

列级不能引出新泄漏，因此强制：

- `ColumnEdge.sources` 里绑定不上的 qualifier **不得**被写进 `TableLineage.sourceTables`。列级绑定与表级 `addSourceTable` 是两套逻辑，互不喂数据。
- 每条边的两端列名与表名，必须能在 `originalSql` 的归一化文本里逐字找到（见 §11 第 5 条 oracle）。找不到即判定为"编造"，测试红。

### 4.5 derivation 枚举与置信度

| derivation | 含义 | confidence | UI 呈现 |
|---|---|---|---|
| `IDENTITY` | 直通列 `a.x → t.x` | 0.95 | 实线 |
| `EXPRESSION` | 表达式/函数/CONCAT/CASE | 0.85 | 实线 + `fx` 徽标 |
| `AGGREGATE` | 聚合或开窗 | 0.80 | 实线 + `agg` 徽标 |
| `CONSTANT` | 字面量，`sources` 为空 | 0.90 | 灰色端点 |
| `POSITIONAL` | INSERT 列名清单按位置对齐 | 0.70 | 虚线 + `pos` 徽标 |
| `UNNAMED` | 表达式无别名且无列名清单，命名按 `expr_<ordinal+1>` 占位 | 0.40 | 灰色 + `?` 徽标，点击看 `transform` 原句 |
| `STAR` | 未展开星号 | 0.30 | 粗虚线 + `*` 徽标，点击提示"未展开" |
| `UNRESOLVED` | 限定符绑不上 / 跨语句依赖 | 0.20 | 灰色虚线，进 `/api/issues` |

实现期（M1）与整体回归确认的绑定细则，都不在 v1 方案里，写在这里对齐：

- **结构体字段路径**：`operation.info_str` 的 `operation` 若不是作用域里的关系、且不是任何已登记关系的
  命名空间前缀（`cat.sch` 之于 `cat.sch.tbl`——那是库限定符，不能当列名），就按 ROW 列的字段访问处理，
  来源是根列 `t.operation`（字段名留在 `rawText` 里给 UI 当证据）。真实语料 2376 条边里这类占 1282 条，
  不做的话 60% 的边都会退化成 `UNRESOLVED`。根列也绑不上时保持原样标 `UNRESOLVED`，不编造列名。
- **未限定列不向父作用域兜底**：内层自己就有表却没有一张容得下这列时是真歧义，标 `UNRESOLVED`；
  向父层兜底会让相关子查询把外层表当成自己的来源（`(SELECT v FROM p JOIN q)` 里的 `v` 凭空变成 `r.v`）。
- **展开函数的下标列**：Spark `posexplode` 的首列、Presto/Flink `WITH ORDINALITY` 的末列都是行计数器，
  按 `CONSTANT` 处理，没有上游字段。
- **集合操作右递归**：`A UNION B INTERSECT C` 在语法里是 `A UNION (B INTERSECT C)`，分支要递归走完；
  末分支的目标列名取第一分支的表头（`SELECT a … UNION ALL SELECT b` 对外只有 `a` 一列，`b` 不是目标列名）。
- **CTAS 的显式列清单**：Flink 把列包在 `tableElement` 里、Spark/Presto 直接挂 `columnDefinition`，
  两种形态都按位置命名目标列。
- **未知表头优先于猜测**：未限定列在多关系作用域里，先找声明了该列的派生关系；都不声明时，
  若"未知表头"的关系只剩一张就归它（其余都报出了自己的列，容不下这一列）。仍歧义才 `UNRESOLVED`。
- **乘号与星号同源**：`MULT` token 只有在父节点里独占时才当作"全部列"，否则 `amount * rate`
  会凭空多出一条整表来源。

## 5. 语法缺口修补（M0，先于解析层，逐方言各自补）

**只有这三处**，都是列级血缘的必要前提。按既有约束"spark/presto/flink 支持各自语法即可"，不跨引擎搬运。

1. `cteDefinition` 增列名清单（三套 SQL 标准都支持，故三方言同步）：
   ```
   cteDefinition : cteName (LPAREN columnNameList RPAREN)? KW_AS LPAREN queryExpression RPAREN ;
   ```
   Flink `:74` / Spark `:79` / Presto `:64`。**改前先确认 `uid` 与关键字冲突不会让 `AS` 被吞**（历史上有"未被语法使用的关键字吞噬标识符"问题）。
2. Spark `lateralView` 别名标签化（`SparkSqlParser.g4:140-143`）：
   ```
   lateralView
     : KW_LATERAL KW_VIEW KW_OUTER? functionName LPAREN (expression (COMMA expression)*)? RPAREN
       (KW_AS? uid)? KW_AS uid (COMMA uid)*
     ;
   ```
   让"表别名"和"列别名"在生成 API 里可分别取。
   ⚠ 此规则存在 ANTLR 歧义风险（两个可选 `KW_AS?`），必须用 `ProbeTok` 式探针跑 `LATERAL VIEW explode(arr) t AS e` 与 `LATERAL VIEW OUTER json_tuple(...) jt AS a, b` 两种写法验证；若有歧义改用显式 `lateralViewTableAlias` 子规则并在测试里锁死。
3. Presto `UNNEST` 的 `aliasWithColumns` 需要被 Visitor 读取（Presto `:113-115` 规则已在，属解析层而非语法层）；`tableReference` 的 `KW_JOIN KW_UNNEST` 分支 `:108-109` 同样要登记列别名。

Flink 侧不需要新增星号规则（`tablePath DOT MULT` 已在 `:230`）。

## 6. 归一化与图构建层（M2 已按此实现，包 `com.bigdata.lineage.graph`）

- `NameNormalizer.normalizeQualified(String)`（已有，M2 直接复用）：逐段去反引号/双引号 + `toLowerCase(Locale.ROOT)`；与 `MultiEngineSQLLineageParser` 缓存键 `sql.trim().toLowerCase()`（`:68`）同一口径，避免同一表两个节点。表级边与列端点在 `ColumnGraphBuilder.StatementContext` 里都先归一化再入图。
- **中间关系折叠**：`ColumnGraphBuilder` 两趟扫每条语句——先把"引擎内部关系"的产出边收进 `StatementContext.producers`，再折叠非内部边的来源，`walk` 顺着 producers 上溯到真实关系，把 `dwd.a → s.v → dws.x` 合成一跳，完整链路留在 `ColumnLink.hops`（含被折掉的中间列）给 UI 详情面板。
  - 关键坑（M2 记录，**M5 已改**）：M2 曾把带别名的子查询**以别名登记**（`subTarget = alias != null ? alias : nextPseudo("sub")`），名字与 CTE、物理表同形，靠 `#` 前缀判断会漏掉语料里绝大多数子查询——因此改由引擎在产出边时打 `ColumnEdge.targetInternal` 标记（`internalRelations` 集合登记），图构建层只认这个标记。M5 发现"别名当身份"本身是错的：`) t1 … ) t1` 逐层套同名别名在真实语料里很常见，两层被并成一个节点后折叠自判成环、放弃这一跳，字段链路断在一个既不是表也不是可折中间站的半截名字上。现在**别名只做查找键、身份一律 `nextPseudo("sub")`**，标记照旧由引擎带（起名是解析层私事，加一种伪节点不该让上层改判定）。
  - 折叠后加工方式取**整条链置信度最低**的那一档（`weakest`）：外层 `IDENTITY` 套内层 `AGGREGATE` 只能是 `AGGREGATE`。
  - 标了内部却没有产出边可折（引擎漏边）时**丢弃该跳并 log.warn**，宁缺勿假；`SqlDirLineageTool` 的"未折叠的伪节点"一节把这类残留变成可见计数（`sql/` 与涉密语料当前均为 0）。
  - **不是列的标识符**（M5）：三套 grammar 都没为 `current_timestamp` 这类无参关键字函数立 token，lambda 形参 `x -> …` 也是普通 `uid`，两者在解析树里跟裸列名同形。`collect` 下钻时按 `NILADIC_FUNCTIONS` 表与形参作用域（`lambdaScopes` 栈，体内遮蔽同名表别名）把它们摘掉，否则写时间戳的列会被报成"来源没绑上"，甚至凭空挂到作用域里唯一报不出列清单的表上。
- **CTE 节点保留 + 语句命名空间**：CTE 是"字段来源"的关键中间站，不折叠，作为 `GraphNode.isLocal()` 节点渲染（虚线边框 + 灰底）。但 CTE 名、伪节点名、绑不上的限定符都只在语句内唯一，语料里 `tmp`/`t`/`c` 跨语句复用，所以这些名字统一登记为 `[jobId]name`（`LocalRelation`）。语句标识取 `ColumnEdge.jobId`，其次取语句文本的 SHA-256 短摘要，最后才退回列表下标——用下标会在"分文件解析再拼列表"时把两个文件的同名 CTE 接成一个节点。
- **分层**：`LayeredDagBuilder` 先 Tarjan 缩强连通（**迭代实现**，20000 节点长链有回归测试钉住，递归 DFS 在语料规模会打穿栈），再在缩点 DAG 上跑 Kahn 最长路径得 rank；自依赖与互查表按 SCC 同层并落入 `Layers.getCyclic()`，UI 用红色回边标注。分层只跑物理表——语句内关系各自成岛。
- `LineageStore`：`AtomicReference<Snapshot>`，`Snapshot` = 一张不可变 `LineageGraph` + 语句数 + 来源标识 + 耗时；重扫时整块替换引用，读侧无锁且一次请求内看见的必然是同一次扫描。缓存语义沿用 `SqlCache` 的 LRU 思路，但快照不做增量 diff（v1 简单可靠）。
- 边去重：`ColumnLink` 按 `from>to>derivation`、`TableLink` 按 `from>to` 去重——语料里同一对表被多条语句写入是常态，并排两条一样的边只会让 UI 抖动。

## 7. 服务层：Spring Boot 引入方式

### 7.1 pom 改动（离线仓库已核实全部可用）

```xml
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-web</artifactId>
  <version>2.7.18</version>
  <exclusions>
    <!-- 项目已有 log4j-slf4j-impl，starter-logging 会带 logback 造成双绑定 -->
    <exclusion>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-logging</artifactId>
    </exclusion>
  </exclusions>
</dependency>
```

可行性核查（`D:/maven` 离线仓库）：`spring-boot-starter-web` 2.7.18 ✓、`spring-boot`/`autoconfigure`/`starter`/`starter-json`/`starter-tomcat` 2.7.18 ✓、`spring-web`/`spring-webmvc` 5.3.31 ✓、`tomcat-embed-core`/`-el`/`-websocket` 9.0.83 ✓、`jackson-databind`/`core`/`annotations`/`datatype-jdk8`/`jsr310`/`parameter-names` 2.13.5 ✓、`jakarta.annotation-api` 1.3.5 ✓、`snakeyaml` 1.30 ✓、`spring-jcl` 5.3.31 ✓。**无 `org.webjars` 任何 artifact** → 这就是必须 vendored 的硬约束来源。

配套两条（本分支的实际情况与拆模块方案不同，因此比原计划更简单）：

- **不再需要 `web` profile**。用 profile 隔离 `spring-boot-maven-plugin` 的唯一理由是"别影响 Flink 作业打包"；本分支已经没有 Flink 作业，插件直接放进 `<build><plugins>` 即可：
  ```xml
  <plugin>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-maven-plugin</artifactId> <!-- 2.7.18，离线仓库有 -->
    <configuration>
      <mainClass>com.bigdata.lineage.web.LineageWebApplication</mainClass>
    </configuration>
    <executions><execution><goals><goal>repackage</goal></goals></execution></executions>
  </plugin>
  ```
  起服务：`java -jar target/flinkLearn-0.0.1-SNAPSHOT.jar`（M3 实测走的是这条）；`mvn -o spring-boot:run` 未验证过离线仓库是否够插件 `run` 目标的依赖，别当既定事实用。
- **不再需要 spring 族版本对齐**。`spring-expression:5.3.26` 已随 §2.0 的 pom 精简删除，spring 版本全部由 starter-web 2.7.18 带的 5.3.31 单点决定，不存在混版本。
- 仍需保留的一条：排 `spring-boot-starter-logging`。项目现在是 `slf4j-api:1.7.25` + `log4j-slf4j-impl:2.9.1`（`log4j2.xml` 在 resources），starter 默认带的 logback 会造成 slf4j 双绑定。若嫌麻烦，替代方案是反过来删掉本项目的 log4j2 三件套、改用 starter 默认 logback——**推荐前者**（改动面小，且 `log4j2.xml` 的 warn 级别策略沿用至今）。

### 7.2 组件（M3 已按此落地）

```
com.bigdata.lineage.web/
  LineageWebApplication.java     @SpringBootApplication(scanBasePackages="...web") + @Bean LineageStore
  LineageProperties.java         @ConfigurationProperties("lineage")：scanDir、rescanOnStart
  CorpusScanner.java             ApplicationRunner：递归 .sql → 逐语句解析 → 回写 jobId → 换快照
  GraphAssembler.java            子图裁剪（root/direction/depth）+ Cytoscape JSON 序列化
  api/LineageApiController.java  下列端点（M3 那 9 个 + §8.11 的 sqlflow 两个）
  api/ApiResponse.java           {success,data,message} 信封
  api/ApiErrorAdvice.java        @RestControllerAdvice：IAE/IO → 400，其余 → 500
  sqlflow/                       字段级的"对方形状"：模型 + 关系 + **网格落位**（只给 `layer/slot`，不给像素）——Statement/Chain/Context/Flow/Assembler 五个类，口径见 §8.13
com.bigdata.lineage.graph/
  ScanReport.java                质量账本：计数是全集，明细截到 200 条
  LineageStore.java              AtomicReference<Snapshot>；Snapshot = 图 + 账本 + jobId→SQL 索引
```

三条实现期定下来的口径，后续改动别再动摇：

- **jobId = 「相对路径#该文件的血缘语句序号」**（`ck/user_app_data_ck_dml.sql#1`），由 `CorpusScanner` 回写进 `ColumnEdge.jobId`（解析层不填，就是留给调用方的口子），`ColumnGraphBuilder.localName()` 优先采纳它。图构建层默认的内容 SHA-256 只用于 `/api/parse` 这种没有文件上下文的场景。**扫描器必须用 `new MultiEngineSQLLineageParser(false)` 关掉缓存**：缓存返回的是同一批可变 DTO，逐文件回写 jobId 会污染上一轮。
- **坏目录只报错不换快照**：`rescan` 对不存在的目录抛 `IllegalArgumentException`（API 层回 400），已扫到的血缘留在原处；`run()` 里的启动扫描失败只 warn，服务照样起来跑空快照，方便单独验证静态页——失败原因与扫描进度记在 `scanError`/`scanPhase`，由 `/api/overview` 下发给页面（§8.5）。
- **`Snapshot` 额外持有 `jobId → 原文` 索引**，`/api/edge/column` 与 `/api/table/{t}/columns` 据此回显 `sqlText`。账本与图都在扫描期算成不可变值，读侧不保留任何 Lombok 可变 DTO。

配置（`application.yml`，M3 后仍就这三个键，静态资源键不需要加——`src/main/resources/static` 是 Boot 默认映射）：

```yaml
server: { port: 8080 }
lineage:
  scan-dir: sql          # 语料目录，可传参覆盖
  rescan-on-start: true
```

配置：`application.yml` 已在 §2.0 精简为以上三键（原来那三段 mysql/mybatis/jsqlparser 死配置已删）。持久化不引 `spring-boot-starter-jdbc`，因此不会触发 `DataSourceAutoConfiguration` 因缺数据源而启动失败。

### 7.3 REST 契约（已实现，参数以下表为准）

| 方法 | 路径 | 用途 | 关键参数 |
|---|---|---|---|
| GET | `/api/overview` | 统计：文件数/语句数/表数/字段边数/表边数/parseError/UNRESOLVED/STAR/最大层/环 + `scanPhase`/`scanError`（空快照的自解释，见 §8.5） | — |
| GET | `/api/tables` | 表列表（含上下游度数、所在层、字段数） | `q`（大小写不敏感**子串**）、`layer`、`onlyUnresolved` |
| GET | `/api/graph/table` | 表级分层 DAG | `root`（空=全量）、`direction=up\|down\|both`、`depth`（默认 2，上限 8） |
| GET | `/api/table/{table:.+}/columns` | 该表字段清单 + 每字段直接来源（带证据）与去向 | — |
| GET | `/api/graph/column` | 字段级链路子图（**前端已改走 `/api/sqlflow/graph`**，此端点仍由单测与 curl 覆盖） | `table`、`column`（可空=整表）、`depth`（默认 3） |
| GET | `/api/edge/column` | 单条边的证据（同上，前端不再调它，证据改由 sqlflow 响应里的 `relationships` 给） | `from`、`to` → `[{jobId,ordinal,engine,derivation,transform,hops[],sqlText,parseError,confidence}]` |
| GET | `/api/issues` | 质量视图 | `type=all\|parseError\|unresolved\|star\|noLineage\|orphan` |
| POST | `/api/parse` | 贴 SQL 试解析（只读、不落库） | `{sql}` → `{statements,graph,columnEdges}` |
| POST | `/api/scan` | 重扫目录，原子换快照 | `{dir?}` → 新统计 |
| GET | `/api/sqlflow/graph` | **字段级血缘的唯一出图入口（快照）**：一次给回数据模型 + 关系 + 落位结论（层号 + 同层序号，**不含像素**） | `table`、`column`（可空=整表）、`focus`（可空，列标识）、`depth`（默认 3） |
| POST | `/api/sqlflow/graph` | 同一形状，喂页面贴进来的 SQL（试解析，不入快照） | `{sqltext\|sql, focus}`——`focus` 只能是**列**标识，没有"按表重定心"这条路 |


与原计划的三处偏离，都是实现期判断，记录在此以免被当成漏做：

- `q` 只做子串不做正则：表名里的 `.` 是正则元字符，用户输入的 `dwd.t` 会匹配得莫名其妙。
- `/api/graph/table` 去掉了 `layer` 参数：邻域图再按层切一刀只会留下断开的点，层过滤归 `/api/tables`。
- `/api/edge/column` 返回**数组**：同一对端点可能有不同 derivation 的多条边（构建层按 `from>to>derivation` 去重），只回一条等于替用户挑了个答案。
- 表名/列名一律大小写不敏感寻址，短名唯一时补全；短名冲突直接报"不唯一，请用全名"，不猜。

响应统一 `{ "success": bool, "data": ..., "message": ... }`（HTTP 状态码与之对齐），前端只认一种信封。

图 JSON schema（Cytoscape 直接可喂）。表级与字段级共用外层形状，字段级视图的节点是列：

```json
{ "nodes": [ {"id":"dws_dw.dws_x","name":"dws_x","db":"dws_dw","layer":3,
              "kind":"physical","colCount":42} ],
  "edges": [ {"id":"t:dwd_dw.dwd_a>dws_dw.dws_x","from":"dwd_dw.dwd_a",
              "to":"dws_dw.dws_x","jobId":"job_03.sql#7"} ],
  "ranks": {"dwd_dw.dwd_a":1}, "cyclic": ["ods_x"] }
```

```json
// /api/graph/column：nodes 为列，edges 为折叠后的字段边
{ "nodes": [ {"id":"dws_dw.dws_x.emp_cnt","column":"emp_cnt","table":"dws_dw.dws_x",
              "name":"dws_x","local":false,"layer":3,"kind":"column"} ],
  "edges": [ {"id":"c:..>..#AGGREGATE","from":"dwd_dw.dwd_a.emp_id","to":"dws_dw.dws_x.emp_cnt",
              "derivation":"AGGREGATE","confidence":0.8,"transform":"count(emp_id)",
              "hops":["dwd_dw.dwd_a.emp_id","[job_03.sql#7]#sub1.c","dws_dw.dws_x.emp_cnt"],
              "engine":"FLINK","jobId":"job_03.sql#7","ordinal":0} ],
  "ranks": {...}, "cyclic": [] }
```

`/api/sqlflow/graph` 回的是**对方（SQLFlow）那套形状 + 落位结论**，一个信封里装四块（逐字段口径见 §8.13）：

```json
{ "sqlflow": {"dbobjs": {"servers": [...]}, "processes": [...], "relationships": [
                {"id":"43","processId":"42","effectType":"select","type":"fdi",
                 "sources":[{"id":"3","parentId":"1","parentName":"kafka_user_app_data","column":"eventbodylist",
                             "qualifiedName":"kafka_user_app_data.eventbodylist",
                             "transforms":[{"code":"app_name","type":"expression","coordinates":[{"x":3,"y":5}]}]}],
                 "target":{"id":"7","parentName":"unnest1","qualifiedName":"[..sql#1]#unnest1.app_name"},
                 "derivation":"EXPRESSION","confidence":0.85,"engine":"FLINK","ordinal":0,
                 "transform":"app_name","hops":["kafka_user_app_data.eventbodylist","..","user_app_data.app_name"],
                 "qualifiedName":"kafka_user_app_data.eventbodylist>user_app_data.app_name"} ]},
  "graph":   {"elements": {"tables": [{"id":"b0","name":"kafka_user_app_data",
                                        "qualifiedName":"kafka_user_app_data","type":"table","local":false,
                                        "layer":0,"slot":0,"modelId":"1",
                                        "columns":[{"id":"b0_c0","name":"eventbodylist",
                                                     "qualifiedName":"kafka_user_app_data.eventbodylist",
                                                     "modelId":"2"}]}],
                             "edges":[{"id":"e0","sourceId":"b0_c0","targetId":"b2_c6","synthetic":false}]},
                       "listIdMap": {"b0": ["1"]}, "relationshipIdMap": {"e0": ["43"]}},
              "summary": {"table":2,"view":0,"column":38,"process":1,"relationship":23,
                          "mostRelationTables":[{"table":"user_app_data","relationCount":27}]}},
  "sessionId": "6589a3871bf26b3e",
  "metaInfo":  {"source":"D:\\project\\flinkLearn\\sql","focus":null,"boxCount":3,"rowCount":25,"modelRows":38,
                "segmentCount":36,"drawn":true,"limit":14,"droppedRows":13,"droppedSegments":13,
                "ranks":{"kafka_user_app_data":0,"[..sql#1]#unnest1":1,"user_app_data":2},
                "cyclic":[], "parseErrorJobs":[], "warning":"1 个中间站…已展开成盒子；13 列未画…"} }
```

`columns[].id` 是 `盒id_c行序号`（`b0_c0`），边的 `sourceId/targetId` 用的就是它（预算没收进行盒的列，端点退化成盒id，线连盒不连空位）；`relationshipIdMap` 是"这条线背后有哪几条关系"，右栏逐跳证据按它去 `relationships` 里取，**前端不再打第二个请求**。标识全部取 `b0 / b0_c0 / e0` 这种形状：它们要当 DOM `id` 和 SVG 端点用，含 `.` `::` 的名字在 `querySelector` 里会炸（§8.13）。


证据字段的四条口径（前端实现期踩过，写死在这里）：

- `ordinal` 是 **0 起**的 SELECT 下标（`app_name=0`、`app_version=1`），界面要显示"第几位"必须 +1。
- `unresolvedSource` 是 `/api/table/{t}/columns` 每个字段的布尔位：`sourceCount=0` 有两种可能——真是源表起点，或来源没绑上、边被 `ColumnGraphBuilder` 丢在图外。图分不出来，只有扫描期的 `ScanReport.unresolvedTargets` 分得出，所以这个标记必须由账本下发，面板不许把 `↑0` 直接说成"链路起点"。同理 `/api/tables?onlyUnresolved` 的脏表集也要并上这份名单。
- `parseError` 只出现在 `/api/edge/column` 与 `/api/table/{t}/columns` 的 `sources` 里：它来自快照的 `Snapshot.isParseError(jobId)`（`CorpusScanner` 扫描期把 `lineage.isParseError()` 的语句标识收成一个 `Set<String>` 存进快照），图 JSON 里没有——裁剪层不关心可信度，展示层才关心。为这个字段专门扩了 `LineageStore.replace(...)` 的第 5 个参数。
- `/api/parse` 回的是**未折叠**的 `TableLineage`，其 `ColumnRef` 只有 `qualifier/column/boundTable/rawText/resolved`：`nodeId()` 不是 getter，Jackson 不下发，所以前端试解析面板按同一口径自己拼 `boundTable||qualifier` + `.` + `column`。

### 7.4 M3 验收实测（2026-09-21）

- `mvn -o clean test` **245 绿**（M2 结束时 228；本轮 +16 Web 单测：`GraphAssemblerTest` 12 + `CorpusScannerTest` 4，+1 `LineageStoreTest` 的 SQL 索引用例）。**离线仓库没有 `spring-boot-test`/`spring-test` 任何版本**，所以 `@SpringBootTest` 这条路走不通：Web 层的验证口径 = 纯单测（裁剪/扫描器契约）+ 真起服务 curl 全端点。
- `mvn -o package -DskipTests` → `java -jar target/flinkLearn-0.0.1-SNAPSHOT.jar` 起在 8080，repackage 正常（21MB 可执行 jar）。
- `sql/` 语料实测 `/api/overview`：16 文件 / 26 语句 / 17 表 / 13 张有字段 / 2329 字段边 / 6 表边 / maxLayer 1 / 0 UNRESOLVED / 0 STAR / 3 parseError（就是 §2.0 里那三段建表 DDL）——与 M2 基线逐项一致，说明引入服务层没有动到血缘。
- 边证据实测 `kafka_user_app_data.eventbodylist → user_app_data.app_name`：`jobId=ck/user_app_data_ck_dml.sql#1`、`hops` 里带着折掉的 `[...sql#1]#unnest1.app_name`、`sqlText` 回显原文；扫描日志 0 条"没有产出边可折"告警。
- `GET /` 当时 404（M4 才放 `static/index.html`），M4 已闭合，见 §8.4。

## 8. 前端层（M4 已按此落地）

### 8.1 目录与资产

```
src/main/resources/static/
  index.html                     三栏骨架 + 顶栏统计/工具（深度、语料目录、重扫、两个导出）+ 空快照告警条 + 图头（标题/告警位/方向下拉）+ 中栏两套画布：表级 cytoscape、字段级 DOM 图（`.slf-stage` 一张纸）＋钉在右上角的动作条
  css/app.css                    单文件；深色仪器台调色板变量（:root 一处定义，见 §8.6）+ derivation 徽标配色 + mark 高亮色
  js/api.js         9 个调用封装，统一解 {success,data,message} 信封（非 JSON 或 success=false 直接抛）+ 在途计数驱动顶栏进度条；字段级只打 /api/sqlflow/graph（服务端另两个旧端点见 §7.3）
  js/badges.js      **调色板唯一出口**：读 app.css 的 :root 变量导出 ink/EDGE_STYLE/LAYER_COLORS + derivation 中文提示、置信度分档
  js/graph.js       挂载与样式表、nodePaint()（表级节点画法，字段图已不用 cytoscape）、setHot()（选中态）、**按 layer 自算布局 runLayout()**（见 §8.7，只管表级图）、fit()（可读字号下限 + 放不下就告警）、NODE_CAP
  js/graphTable.js  表级 DAG
  js/sqlflowModel.js 把 /api/sqlflow/graph 的 dbobjs/relationships 索引成右栏要的形状（列卡、实体卡、逐跳证据）
  js/sqlflowView.js  字段链路图：**DOM 盒 + 一层 SVG 边**，服务端只给网格（层号 + 同层序号），像素在这里量（见 §8.13）
  js/sqlflowBar.js   钉在画布右上角的可拖动动作条：`只看这一列` / `列出字段` / `复位` / `×`，只管显示与拖动，动作由调用方给
  js/parsed.js      把 `/api/parse` 的 payload 拼成与快照同形的视图/详情形状，让试解析能直接画图（见 §8.8）
  js/detailPanel.js 右栏：表详情 / 字段来源 / 边证据 / 试解析结果
  js/main.js        选区状态机 + 全部事件绑定 + URL hash + 导出
  vendor/cytoscape.min.js 3.32.1(431508B) + LICENSE-cytoscape.txt + README.md（版本、sha256）
```

获取方式照原计划：`npm pack` 取 `dist/*.min.js` 拷进 `vendor/`，README 记版本与 sha256 便于审计；**无 CDN、无 webjars、无打包器、无 TS、无框架**，`<script type="module" src="/js/main.js">` 直接跑。

`index.html` 里只剩一处 `<script src="/vendor/cytoscape.min.js">`，且必须在模块标签之前（模块拿不到 `window.cytoscape` 就没法建图）。dagre 与 cytoscape-dagre 已在 §8.7 里下线，加载顺序不再是约束。

**vendored 只剩一件**这件事由 `StaticAssetIntegrityTest` 兜着：它逐字符扫 `static/` 下的 JS 找没闭合的块注释，顺带核 `import` 目标、`index.html` 的资源引用和 `getElementById` 的 `id` 是否都存在——前端没有构建步骤，这类错误只会在浏览器里表现为整站白屏。

### 8.2 三栏交互

- **左栏**三个 tab：表 / 质量 / 试解析。
  - 表：搜索框 250ms 防抖 + 层过滤 `<select>`（选项按 `/api/overview.maxLayer` 生成——**图的 rank 是唯一可信的分层来源，不按 ODS/DWD 前缀猜**，前缀识别留在 §13 范围外）+ "只看有质量问题"（对应 `onlyUnresolved`）+ 列表项 `层 n · ↑u ↓d · c 字段`。
  - 质量：5 组"计数 + 明细"（parseError SQL / UNRESOLVED 边 / STAR 边 / 没出血缘的文件 / 孤岛表），空组绿底，首次打开才发请求。
  - 试解析：textarea → `POST /api/parse` → 逐语句卡片（输入表、插入模式、CTE/WINDOW/TEMPORAL 标记、未折叠字段边 + derivation 徽标、原文）。只读，不动快照。按钮是「解析并画到图上」：卡片之外还会把血缘画到中栏，见 §8.8。
- **中栏**的 tab 共享同一份选区 `state.{view,table,column,parsedColumn}`：快照两个（表级 / 字段）+ 试解析两个（§8.8）。
  - 表级 DAG：按 `layer` 分层，节点底色 = 层色；点表节点 = 以它为中心重裁（root + direction + depth）并让右栏出字段清单；点边出"来源表/目标表/jobId"；点空白清选区回全量。
  - 字段链路：**一表一盒、一盒一行一字段**（不是每列一个飘在画布上的节点）。盒顶固定一条标题带单独写表名，被折掉的中间关系（CTE/子查询/展开）画成中间的暗底虚线盒，边按 derivation 上色上型（UNRESOLVED 红虚线、STAR 橙点线、AGGREGATE 加粗金…），hover 出 derivation 标签，当前定位列打 `hot`。**服务端只下发网格（`layer` = 第几层、`slot` = 同层第几个）＋行标识，像素由浏览器量**：盒宽行高字号交给 CSS，Java 不再算 `x/y/width/height`。为什么从 §8.11 的"坐标进响应"退回来，见 §8.13。
  - 悬停一行 = **只在原地染色**（整条通路淡色，不动布局、不发请求）；点一行 = 选中并钉出右上角**动作条**：`只看这一列` / `列出字段` / `复位` + ✕，选中态与右栏证据同时跟上。**动作条不跟着光标或那一行走**——§8.11 的跟随式浮层有两处静默失效都出在"贴位置"上，钉在角落之后位置与图的内容无关（§8.13）。
  - 图头的「方向」下拉是**子图取数**方向（双向 / 只看来源 / 只看下游，喂给 `/api/graph/table` 的 `direction`），不是画法方向；画法方向由 `runLayout()` 在 LR / TB 里自动挑放缩更大的那个，详见 §8.7。字段级图不参与这个选择：它横向永远是层号，缩放是 `transform: scale()` 放大整张纸，字号恒定（§8.13）。
  - 性能闸门：表级图是前端的 `NODE_CAP=300`，超限**直接不画**，只回告警 + 保留右栏（语料里 783/787 列的表整表展开是 1568 节点，画出来必卡）；字段级的闸门在 §8.11 之后搬进了服务端（行预算 + `BOX_CAP`），前端拿的就是裁好的那一片。右栏字段清单另外截到 80 行。
- **与原计划的四处实现期取舍**，记下来免得被当成漏做：
  1. 不做右键上下文菜单。表级图仍是「点节点即重裁 + 右栏按钮 + 图头方向下拉」；字段级换成了**钉在角落的动作条**——比右键菜单便宜（不用跟光标、不用处理出界），又比埋在右栏的按钮就近。
  2. 字段链路不做 swimlane 泳道。字段图确实是一列一层、纵向摞行，但**行内的摆法交给浏览器排版**，后端只定层号（§8.13）。
  3. 边不按 `process_type` 上色，按 `derivation` 上色。字段级面板要回答的是"这条边多可信"，`process_type` 在表级边的证据里给。
  4. 布局不引第三方分层引擎，`layer` 已经在响应里。自己摆反而更准，代价是节点尺寸得手工调，见 §8.7；字段级只把 `layer/slot` 换成像素，仍然没有布局引擎（§8.13）。
- 无框架、无构建，全部 ES module 相对导入。

### 8.3 证据块、导出与定位

一条边的证据块从上到下（对应原计划右栏 5 条）：

1. `to ← from` + derivation 徽标（STAR/UNRESOLVED 带 tooltip 说明 v1 约定）；
2. parseError 红条（有才出）："此边来自语法错误恢复的部分结果，不可信"；
3. hops 逐跳链（**>2 跳才画**，只有两端时没有信息量）；
4. `加工方式 / 置信度 / 表达式 / 引擎 / 语句 jobId / SELECT 第几位`；
5. SQL 原文 + 来源、目标列高亮。

- 高亮实现：拿 `from`/`to` 的**末段列名**在原文里正则匹配（`escapeRegExp` + `\b`），目标 `mark.target` 蓝底、来源黄底；来源列与目标列同名时不区分两处出现（v1 不做词法定位）。全程 `createElement`/`createTextNode` 拼 DOM，**绝不用 innerHTML 塞 SQL**——语料文本是不可信输入。
- 导出：PNG 用 `cy.png({full:true,scale:2,bg:ink.canvasBg})` + `<a download>`（原计划写的 `toBlob` 不是 cytoscape 的 API；底色从 §8.6 的调色板变量取，不能再给一张白纸）；子图 JSON 把 `{view,table,column,graph}` 打成 Blob 下载，`graph` 就是最近一次服务端下发的子图原文，可离线复核。
- URL hash：`#view=table|column&table=<id>&column=<table.column>`，用 `history.replaceState` 同步（不推历史栈），外部改 hash 走 `hashchange` 恢复选区，链接可直接分享定位。

### 8.4 M4 验收实测（2026-09-21）

- `mvn -o clean test` 首轮 **245 绿**（M4 主体没动 Java 逻辑；M3 收尾时补进去的 `parseErrorJobs` 插管走的是既有 `LineageStoreTest` 用例）。边界补验后又改了 3 个 Java 文件，末轮 **246 绿**，见下。
- `mvn -o package -DskipTests` → jar 内 `BOOT-INF/classes/static/**` 齐 21 个文件；`GET /` 200（3122B），`/js/*`、`/css/*`、`/vendor/*` 全 200。
- 真浏览器（browser-use 驱动的 CDP 页面）实测，console 除 cytoscape 的 wheelSensitivity 提示外**零 error**，逐项：
  - 顶栏 8 组统计 = 16 文件/26 语句/17 表（13 有字段）/2329 字段边/6 表边/maxLayer 1/错 3·未解析 0·星号 0；全量 DAG 标题 `17 表 / 6 边`；左栏 17 行、图例 2 层色块。
  - 过滤链路：搜 `g05` → 8 张表，清空 → 17 张；层=1 → 4 张；`只看有质量问题` → 0 张（本语料 0 UNRESOLVED/0 STAR，与账本自洽）。
  - 点表节点 → 图重裁、hash 写 `#view=table&table=user_app_data`、左栏行高亮、右栏 27 行字段清单（每行 `↑n 来源 · ↓m 去向`）。
  - 点字段行 → 进字段链路视图并**留在该字段**：BOTH depth 2 展开 10 节点 / 9 边，`user_app_data.app_name` 打 `hot`，边类名 `EXPRESSION`。
  - 边证据：`kafka_user_app_data.eventbodylist → user_app_data.app_name`，hops 三段（含折掉的 `[ck/user_app_data_ck_dml.sql#1]#unnest1.app_name`）、`85%（EXPRESSION）`、`FLINK`、`SELECT 第 1 位`、942 字符原文里 3 处 `<mark>`。
  - 点字段边 → `/api/edge/column` 面板 `命中 1 条边`；点表级边 → `jobId=ck/g05_ck_dml.sql#1`。
  - 边界：整表展开 `g05_new_ck_kafka_enterprise_tel_ul_tx` → 服务端 1568 节点 → 前端拒画并提示"超过 300 上限"（635 列表同 644 节点）；连续 tap 全部 17 个节点无异常、选区自洽。
  - parseError 红条：语料那 3 段 DDL 不产出字段边（全 17 张表 `parseError:true` 计数均 0），于是在 git-ignored 临时目录放一条语法残缺但可错误恢复的 `INSERT ... WHERE (a > 1`，`POST /api/scan` 换快照后边面板顶部出红条，再点`重扫目录`换回 `sql/`（统计回到 16/26/2329）——临时目录已删，涉密语料规则未破。
  - 试解析：`WITH s AS (…) SELECT SUM(s.amount)…` → 4 条未折叠边（`dwd.pay.amount → s.amount`、`s.amount → dws.sum_cnt.emp_cnt [AGGREGATE]`），输入表只有 `dwd.pay`（CTE 名没漏成表）。
  - 传不存在的列（`column=nope`）→ 图保持上一帧不白屏，告警位与右栏出 `表 kafka_table 没有字段 nope`。
- 浏览器验证揪出并修掉的 7 个前端缺陷（都是纸面看不出来的）：字段清单点进去不切视图；`column` 参数误传整条 `table.column` 标识（服务端要裸列名）；cytoscape 边数据只有 `source/target`，拿 `data.from` 拼请求 → 缺参 400；`hashchange` 没 await 导致失败被吞成 unhandled rejection；超限标题谎报"0 边"；`ordinal` 是 0 起下标却按"真值才显示"渲染成"少一位"（`0` 直接消失）；`/api/parse` 的 `sources` 里没有 `nodeId`（不是 getter，Jackson 不下发），面板一度显示 `undefined`。
- 星号/未解析两条边界用 §14.5 的方式补验（临时目录 `D:/tmp-m4star`，在仓库外所以不可能被 `git add`，验完即删），**又揪出两个真缺陷**：
  - STAR 侧没问题——`dwd.star.*` 的徽标 `STAR · 星号未展开`、`30%（STAR）`、原文高亮、`语句 star_unresolved.sql#1` 全出，图例也有星号色。
  - UNRESOLVED 侧是**语义谎言**：来源没绑上的边根本进不了图（`ColumnGraphBuilder` 造不出指向 null 的端点），于是字段面板说"没有解析到来源：这是链路起点（源表字段）"，而 `dwd.amb` 明明是 INSERT 目标；`/api/tables?onlyUnresolved=true` 也漏掉它（脏表集合是从图边推的，边已经被丢了）。修法：`ScanReport` 另存 `unresolvedTargets`（目标列标识），读侧据此①把表并进脏表集，②给字段清单下发 `unresolvedSource`，面板改出红条"它是解析缺口，不是链路起点"，字段行挂 `UNRESOLVED` 徽标。顺带把 `/api/issues` 明细里漏出来的 `ColumnRef(...)` Lombok toString 换成裸列名。
  - 复验：临时语料下 `onlyUnresolved` 从 2 张变 3 张（补回 `dwd.amb`），issues 明细变成 `dwd.amb.amount[UNRESOLVED] <- [amount]`，浏览器里红条与徽标都渲染出来；换回 `sql/` 后统计仍是 16/26/3/0/0/17/13/0/2329/6/maxLayer 1，`onlyUnresolved` 空——本语料 0 未解析，与账本自洽。新增 `LineageStoreTest.unresolvedTargetsStayInTheLedgerAfterTheGraphDropsTheirEdges`，`mvn -o clean test` **246 绿**。
- **仍未验到的**：in-app Browser 没给可见 surface（viewport 0x0），所以像素级观感（节点是否重叠、配色对比度、PNG 成品长什么样）只有模型层断言撑着（dagre 确实跑了：17 节点 x 跨 2029、y 跨 122）。人眼过一遍仍是 §14 的待办。

### 8.5 「页面什么都没有」的三种成因与启动竞态（2026-09-21 修复）

表级/字段级血缘在解析层是好的（CLI `run-lineage.bat sql` 与 `/api/overview` 同一份账本），页面空白却另有其因。逐个排除：

1. **服务没在跑**。上一轮验收结束我 `taskkill` 掉了 8080 进程，浏览器于是连不上。这属操作遗留，不是缺陷。
2. **语料目录指错**。`lineage.scan-dir` 是相对路径时按**服务进程的工作目录**解析，从别的目录 `java -jar` 就指到了别处 → 顶栏 `0 文件 / 0 语句`，而原因（`目录不存在: …`）只落在服务日志里，页面上一句解释都没有。
3. **真缺陷：端口先于扫描就绪**。Spring Boot 在 `refreshContext` 阶段就绑定端口开始接受请求，而 `CorpusScanner` 是 `ApplicationRunner`，跑在其后。浏览器只要抢在这段窗口里打开，拿到的就是空快照，且**页面不会自己回头取数**——语料越大窗口越长：`sql/`（15 文件）约 1.5s，仓库外的合成复制语料（12 文件 / 3168 语句 / 49 MB）**39.4s**。第 3 条才是"每次都空白"的那个成因，前两条只是让它更难归因。

修法只加状态、不改时序（把扫描挪到 `ServletContextInitializer` 之前会让启动更慢，且并不解决"空目录"那种永远等不到的情况）：

- `CorpusScanner` 增两个 `volatile` 字段：`scanPhase`（`running` → `ready`/`failed`，`lineage.rescan-on-start=false` 时为 `skipped`）与 `scanError`（启动扫描的失败原因，成功 `rescan` 后清空）。二者经 `/api/overview` 下发。
- 页面顶部一条 `#scan-error` 横幅（`role=alert`），四态：**红**=`failed`（"启动时没扫成：{原因}"）或 `ready` 但 `fileCount=0`（"{目录} 里没有 .sql 文件"），两者都附一句"相对路径按服务进程的工作目录解析：可在顶栏「语料目录」填绝对路径再点「重扫目录」，或用 `--lineage.scan-dir=<绝对路径>` 起服务"；**蓝**=`running`（"语料还在扫（端口先于扫描就绪，属正常），扫完自动出图"）与 `skipped`；**不出**=`ready` 且有数据。
- `waitScan()`：`scanPhase === 'running'` 时每 500ms 重问 `/api/overview`，最多 60s（120 次），超时才改口"已等 60 秒，扫完刷新页面或点「重扫目录」"。空快照从此不再伪装成"这份语料没有血缘"。

浏览器实测又揪出 4 个纸面看不出来的缺陷（都已修，最后一条纯观感）：

1. **换目录得靠弹窗，而弹窗在这类页面里根本不可用**。先按 `window.prompt` 实现"重扫换个目录"，页面直接 `TypeError: prompt() is not supported.`（自动化/embedded webview 禁模态框）。改成顶栏一个 `#scan-dir` 文本框 + 原按钮读它的值（留空=服务配置的目录），服务端不用动（`/api/scan` 本来就收 `dir`）。
2. **重扫成功后图不跟着重画**：handler 只 `loadOverview()+loadTables()`，于是出现过"左栏 14 行、图标题还写着 `全量表级 DAG（0 表 / 0 边）`"的自相矛盾帧 → 补 `await refresh()`。
3. **窄窗口把工具区压碎**：多出的目录框让 `.tools` 里的按钮缩成 `36x94`（文字竖排）→ `.topbar`/`.tools` 改 `flex-wrap:wrap`、按钮 `white-space:nowrap`；531px 视口下顶栏 516x179、按钮回到 78x31。
4. **空快照的层过滤有个假选项**：`maxLayer=-1` 时 `fillLayerFilter` 仍画"第 0 层" → 改成按真实 `maxLayer` 收（空快照只剩"全部"，与 §8.2"分层只认图的 rank"口径一致）。

最终一轮实测（browser-use，仓库外临时目录 `D:/tmp-probe`，验完即删；三个实例分别造出三种状态）：

- `--lineage.scan-dir=D:/nope-such-dir` → `scanPhase:"failed"`、`scanError:"目录不存在：…"`，横幅红底可见（`rgb(253,236,234)`/`rgb(179,38,30)`、高 74px、`role=alert`），层过滤只有"全部"、左栏 0 行；服务照常 200 起，读端点按契约应答（无表可查回 400 而不是 500）。
- `--lineage.scan-dir=D:/tmp-probe/empty`（空目录）→ 横幅 `D:\tmp-probe\empty 里没有 .sql 文件。…`，与上一态措辞可区分。
- **同一个页面实例**（`performance.now()=96070ms`，全程没刷新）在「语料目录」填 `D:/project/flinkLearn/sql` 点「重扫目录」后自愈：横幅 `hidden`、左栏 14 行、标题 `全量表级 DAG（14 表 / 6 边）`、层过滤补出"第 0/1 层"、右栏 `重扫完成：D:\project\flinkLearn\sql → 15 文件 / 22 条语句 / 2327 条字段边，用时 1601ms`。`running` 态的蓝条与同源自愈在上一轮用慢语料单独验过（49 MB/12 文件 39.4s）。
- 健康实例回归一遍没坏：顶栏 `15 文件/22 语句/14 表（12 有字段）/2327 字段边/6 表边/层级 1/错 0·未解析 0·星号 0`；点表 → 右栏 783 字段截到 80 行；点 `vin` 行 → `#view=column…&column=….vin`、标题 `…vin 的字段链路（5 节点 / 4 边）`、图例 8 项、console 只剩 cytoscape 的 wheelSensitivity 提示。**表级与字段级血缘本来就都解析正常**，页面空白纯属上面三条。
- 一处工具侧的假象，记下来免得下轮再追：browser-use 的坐标点击在这个 531px 视口上没落到按钮（`elementFromPoint(按钮中心)` 返回的正是该按钮，`element.click()` 则照常生效），是自动化侧的坐标问题，不是页面缺事件。
- 新增 `CorpusScannerTest` 2 例（启动失败留原因、`rescan-on-start=false` 自成一态），`mvn -o clean package` **254 绿**。

### 8.6 深色仪器台改造（2026-09-22，只动样式与交互层）

方向由用户选定：**深色数据控制台**。硬约束三条，全部守住——零构建、无框架、**不引 web 字体**（离线跑，为一个字形多一次外链请求不值），以及**REST 契约与解析层一行不改**（本轮 `src/main/java` 零改动，`mvn -o clean test` 仍 **255 绿**）。

字体只用系统里真实存在的：标题 `Bahnschrift SemiBold Condensed`（Windows 自带，等宽压缩的工程感），正文/数据 `Cascadia Mono`→`Consolas`，配 `font-variant-numeric: tabular-nums` 让统计条的数字对齐。

**调色板只有一个来源**：色值全写在 `app.css` 的 `:root`，`badges.js` 用 `getComputedStyle(document.documentElement).getPropertyValue()` 把同一份读出来，导出 `ink` / `EDGE_STYLE` / `layerColor()`，读不到再退内置兜底值。CSS 与画布各存一份色值是"图例说第 0 层是青色、节点画成蓝色"这类事故的成因。**图例改为遍历 `EDGE_STYLE` 的键生成**，不再手抄一份清单。

浏览器实测（真页面、真接口）揪出两个纸面看不出来的缺陷，都已修：

1. **`UNNAMED` 有语义、没颜色也没图例**。仓库语料里 `kafka_user_app_data` 的字段边含 `IDENTITY/EXPRESSION/UNNAMED` 三种，而 `EDGE_STYLE` 没有 `UNNAMED` 键 → 画布按默认边色画，图例里根本没有这一项，用户无从知道那条线是什么意思（§12 早就承诺"`POSITIONAL`/`UNNAMED` 显式标注"，这轮才算兑现到图上）。修法：新增 `--edge-unnamed: #e3cb90`（与既有 `.badge.UNNAMED` 同色）+ `EDGE_STYLE.UNNAMED`（点线），并把图例改成从 `EDGE_STYLE` 生成，从此加一种 derivation 自动进图例。
2. **选中态整条规则是死的**。Cytoscape 的**直接样式（`ele.style(k,v)`）优先级高于样式表**，而 `drawTable` 给每个节点逐个写了 `overlay-*` 同色发光与 `border-color` → 样式表里的 `.hot`/`:selected` 永不生效。实测读数：打上 `hot` 后解析出的仍是该节点自己的 `overlay-color rgb(182,227,74)`（层色）与 `border-color rgb(4,8,14)`，选中点和没选中点长得一样。修法：两张图的节点基础外观合并成 `graph.js#nodePaint()`（物理=层色牌 + 近黑字，中间关系=暗底虚线框），基础态**不再占用 overlay**；高亮走同一直接样式路径由 `setHot(cy, id)` 写/清（3px 酸绿描边，取消选中时重新调 `nodePaint()` 复原，而不是 `removeClass` 了事）。顺带定见：**暗底上同色 overlay 发光本来就看不见**（酸绿叠在柠檬绿的牌子上），选中该用描边而不是光晕，故 `.hot`/`:selected` 两条样式表规则删除，机制只留一处。

另一条踩过的坑值得留下：cytoscape **3.32.1 没有 `shadow-*`**，写了会在控制台按节点数刷出 68 条 `The style property 'shadow-blur' is invalid`。暗底"浮起来"的效果要用受支持的 `overlay-*`/`border` 实现。

**同轮收尾回归又扫掉三类残留**（都不影响功能，但会慢慢烂回去）：`graph.js` 样式表里两个硬编码色值绕开了调色板（`#33475d` 根本没有对应变量、`#05070c` 与 `--ink-0` 重复）→ 补 `--node-line` 并改用 `ink.nodeLine`/`ink.canvasBg`；`ink.arrow`、`FALLBACK.faded`、`confidenceClass()` 与样式表里的 `.faded` 规则全是零引用死代码 → 删（置信度分档早就不在前端重新推断，§8.2 口径一致）。核对方式：把 7 个 js 源文件在页面里抓出来，正则提取所有 `getElementById`/`querySelector` 的 id 逐个查 DOM——24 个引用全命中，无悬空接线。

窄视口（本轮唯一改动是新增顶栏状态条导致挤高）：`.grid` 的 `grid-auto-rows` 先按 `minmax(220px, auto)` 写，实测 `#graph` 被 tabs/图例挤到 **516x112**——等于没有图；改 `minmax(300px, 46vh)` + 单列时整页 `overflow:auto`，复测三行 `300px 300px 300px`、`#graph` 516x192。

**验收方法记下来，下轮不必重新摸索**：in-app Browser 表面处于 `visibilityState=hidden` 时 `take_screenshot` 直接拒（`NATIVE_BROWSER_VIEWPORT_UNAVAILABLE`），且 rAF 被冻结——`getImageData` 读实时画布全 0，那不是"渲染坏了"。可行的像素取证路径是：用页面自己的 `create()`/`drawTable()`/`drawColumns()` 把一个容器渲染到屏外（`position:fixed;left:-3000px`，给到宽高即可），再取 `cy.png()`（**同步绘制**，不受 rAF 影响）→ `fetch(dataURI).blob()` → **`createImageBitmap`**。注意别用 `new Image().decode()`：隐藏页里它不 resolve，会把整段脚本拖到 15s 超时。

像素读数（`kafka_user_app_data` 字段视图，29 节点 / 27 边，PNG 3381x102）：

- 画布底 `#05070c` 占 **263545/344862 ≈ 76%**，深色是真的而不是 CSS 变量写了就算。
- 层色牌：层 0 青 `#4fd6c4` 3628px / 层 1 柠 `#b6e34a` 45207px，节点数 2 与 27 对得上。
- 边色与图例逐颗对照：`IDENTITY #7e93a8` 5585px（17 边）、`EXPRESSION #6aa7ff` 1421px（9 边）、`UNNAMED #e3cb90` 52px（1 边），本视图没有的 `AGGREGATE/CONSTANT/STAR/UNRESOLVED` 全为 0px——**图例不再有说谎的项**。
- 中间关系（本语料 `localRelationCount=0`，只能按契约形状喂合成视图验证）：节点解析出 `background rgb(17,26,37)`、`border-style dashed`、`border rgb(91,118,145)`，像素上 `#111a25` 998px / 边线 49px 确实在画布上。
- 选中态 A/B：同一视图 `#cbf24a` 像素 **0 → 1256 → 0**（标记前/后/取消后），且取消后层色底与虚线框都复原；把 `hot` 从中间关系挪到物理表，`localLine` 像素 0 → 44 精确复原。
- 交互链一遍照旧全绿：顶栏 8 组 `15 文件/22 语句/14 表（12 有字段）/2327 字段边/6 表边/层级 1/错 0·未解析 0·星号 0`、状态条 `D:\project\flinkLearn\sql · 就绪 · 1119ms`、左栏行前圆点色 = 画布层色、表详情 783 字段截 80 行、字段证据块（`EXPRESSION · 表达式` 徽标 + 置信度 85% + 引擎 FLINK + `第 1 位`）、SQL 原文高亮目标酸底/来源橙底、点边出 `字段边…命中 1 条边`、质量页空组带 acid 左边框、试解析 `1 条语句 → 2 条字段边 → 3 张表`。
- `api.js` 的在途计数经直接调用验证：并发两请求时先完成一个 `body.busy` 仍为 `true`，全部完成才 `false`，接口报错同样释放（不卡进度条）。`.mark.live`（青色脉冲）按设计只在 `scanPhase=running`/`scanError` 时挂上，就绪态是稳的酸绿点。
- console 全程只剩 cytoscape 那条 `wheelSensitivity` 提示（有意调低滚轮灵敏度所致），**零 error、零 invalid-style**。

### 8.7 布局改为按 `layer` 自算：dagre 下线（2026-09-22）

**症状**：`sql/` 目录扫完，顶栏统计条明明写着 `14 表 / 6 表边`，中栏却像"什么都没有"。接口侧先排除了数据问题——`/api/graph/table` 实返 14 节点 / 6 边，6 条边对这份语料是算术正确的（7 条 INSERT，一对端点重复被并掉，2 张 DDL-only 表没有边）。问题在画法。

**根因**：dagre 按**连通分量**自己重排 rank，不看服务端下发的 `layer`。语料的边稀疏（14 节点只有 6 条边、互不相干的分量多），于是被摊成 **8 列 1653×200**——长宽比 8.3:1。fit 到中间栏只剩 **0.39 缩放**，节点上的字缩到 4.3px、边 0.59px，人眼看就是空白。更糟的是它把 layer 1 的节点画到了 layer 0 左边，和图例「第几层」直接矛盾。

**改法**（`js/graph.js#runLayout()`，签名去掉了方向参数）：

1. `groupByLayer()`：以 `node.data('layer')` 为**列号**，非整数（旧快照/手工构造）落到第 0 列，列按层号升序。服务端 `LayeredDagBuilder` 的 Tarjan+Kahn 已经是权威分层，前端没有理由再排一次。
2. `orderWithinColumns()`：列内按重心排序——第 0 列保持原序，其后每列按其入边来源**已定行号的均值**稳定排序，让相连的节点在跨轴上尽量靠近，减少交叉。
3. `measure()` / `place()`：按节点实际 `width`/`height` 累加 `GAP`（LR：rank 110 / node 16；TB：rank 80 / node 26），短列在跨轴居中。
4. `zoomOf('LR')` 与 `zoomOf('TB')` 各估一次容器放缩，**取更大的那个方向**摆坐标。稀疏图（宽扁）自然走 TB，深链自然走 LR，不需要用户猜。

不再有 `breadthfirst` 降级分支：自算坐标不可能"布局引擎没注册"，try/catch 只是把错误藏起来。

**改前 / 改后**（中间栏画布按 708×637 仿真，`cy.fit(24)` 后取实际 extent 与 zoom）：

| | extent | 长宽比 | fit zoom | 字高 | 边宽 | 列数 | 层序 |
|---|---|---|---|---|---|---|---|
| 表级 14 节点 · dagre | 1653×200 | 8.3:1 | 0.394 | 4.3px | 0.59px | 8 | layer 1 在 layer 0 **左边** |
| 表级 14 节点 · layer | 529×476 | 1.11:1 | **1.34** | 14.7px | 2.01px | 2 | x：层 0 = 95 / 层 1 = 395，单调 |
| 字段级整表 29 节点 · dagre | 1961×339 | 5.8:1 | 0.33 | — | — | — | — |
| 字段级整表 29 节点 · layer | 1283×1154 | 1.11:1 | **0.55** | — | — | — | — |

像素侧复核：表级画布非背景像素占比 **29.5%**（层 0 底色 71134px / 层 1 底色 50100px / 边 1634px）——图是真的画在框里，不是"接口有数据但画布空着"。

**代价**：dagre.min.js(283803B) 与 cytoscape-dagre.js(12665B) 及其两份 LICENSE 从 `vendor/` 删除，vendored 依赖只剩 cytoscape 一件。节点尺寸从此由 `graph.js` 手工维护，加节点形态要顺手确认 `sizeAlong/sizeCross` 取到的样式值。

### 8.8 试解析入图：贴 SQL → 中栏画出血缘（2026-09-22）

改造前「试解析」只出左栏文字卡片，用户要自己从卡片里读出表名再去图里找——`POST /api/parse` 的响应里本来就有 `graph`（未落快照的临时大图）和 `columnEdges`，只是没画。

**分层**：`js/parsed.js` 是唯一的新模块，职责是把 `/api/parse` 的 payload **适配成快照视图的形状**，`graphTable.js` / `graphColumn.js` / `detailPanel.js` 因此一行未改：

- `parsedTableDetail(payload, table)` → 与 `/api/table/{t}/columns` 同形的 `{table,name,layer,local,columns[]}`（每列的 `sourceCount`/`consumerCount` 由 `columnEdges` 现数）。
- `parsedColumnsView(payload)` → 端点集去重成 `{nodes:[{id,name,table,layer,local:false}], edges:[…]}`，`layer` 取其所属表的层，于是 §8.7 的布局对临时图同样成立。
- `parsedColumnDetail(payload, columnId)` → `{sources[],consumers[]}`，并给每条边补 `sqlText = payload.rawSql`（`main.js` 在发起时把原始输入挂到 `state.parse.rawSql`），右栏的原文高亮才有东西可高亮。
- **中间关系（CTE/子查询）不进临时图**：`/api/parse.graph` 已经把它们折掉了，前端再从 `columnEdges` 反解一遍只会和快照口径打架；要看逐跳证据仍在边的 `hops` 里。

**状态机**：`state.view` 增 `parsed-table` / `parsed-column` 两态，`state.parse` 存 payload、`state.parsedColumn` 存选中的临时列。三条不变量：

1. **临时视图不写 hash**。`#view=parsed-*` 刷新即失效（payload 只在内存），分享出去的链接必然坏，所以视图页签点击时 `if (!view.startsWith('parsed-')) syncHash()`。重扫目录会清空 `state.parse` 并退回表级。
2. **选区互斥**。点左栏任意表/列（`selectTable`/`selectColumn`/`resetSelection`）先 `leaveParsed()` 回快照视图，避免"右栏是快照字段清单、画布是临时图"的错配。
3. **空 payload 不画空图**。没解析过就点到禁用外的入口（手工改 hash、解析失败残留）→ `switchView('table')` 后重新 `refresh()`。

两个新页签在未解析时 `disabled`（`.tabs button:disabled` 走 `--dimmer` + `cursor:not-allowed`，`.tabs` 加 `flex-wrap:wrap` 防四个页签挤爆），进入临时视图时右栏明确提示「这张图来自输入框里的 SQL」，图例同样带这句话——不让用户把试解析当成已落库的事实。

**实测**（合成 2 条语句、5 张表、4 条表边、7 条折叠后字段边）：解析后中栏 3 层单调（仿真框内 y = 17 / 131 / 245）、zoom 2.22；点表节点 → 右栏 4 行字段清单 + `hot` 描边；「试解析·字段」→ 11 节点 / 7 边、图例 11 项；点列 `dt` → 标题 `dt（t_order_sel）`、SQL 原文 4 处 `<mark>`、`EXPRESSION · 表达式` 徽标；点字段边 → `ods.kafka_order.order_id → dwd.t_order_sel.order_id：命中 1 条边` + 3 处高亮 + 粘贴的原文。点左栏列表项退回快照（标题 `表级链路：user_app_data`），翻回临时页签 payload 仍在，`location.hash` 全程没出现过 `parsed`。

### 8.9 图上「看不到字」：标签根本没被画出来 + 可读字号下限（2026-09-22）

**症状**：点节点右栏出表信息、画布也确有方块，但**读不出字**。用户的第一反应是配色问题——不是：`--node-ink: #04080e` 画在 `layerColor` 的亮底上，对比度实测充分。

**根因一条半，都在画法层**：

1. **节点样式表从未写过 `label`**。cytoscape 3.32.1 里 `node` 的 `label` 默认值解析成空串（最小实例复核：`cy.nodes()[0].style('label') === ''`），也就是**一个字都没有画过**——不是画得小，也不是颜色淡。修在 `graph.js` 的样式表：`label: 'data(label)'`；`graphTable.js` 给短表名、`graphColumn.js` 只给列名（`table.` 前缀每个节点都一样，它吃掉的宽度正好把 fit 缩放压没，完整标识在右栏给）。于是"画出来的串"和 `replace()` 里量宽度用的串必须是同一个 `data('label')`——改标签口径要两处一起改。`parsed.js` 的 `name` 也顺手统一成"所属表的短名"。
2. **画布文字随 zoom 缩放**（这半条在 1 修好之后才会显形）：中间栏 516×273 时整图 fit 只有 0.372 倍，11px 标签被压成 4.09px。所以低于可读下限就不保整图、改保字：`fit()` 里 `MIN_LABEL_PX = 10`，缩放不足时放大到下限并**左上对齐**（链路起点先进眼，被裁掉的那一头交给拖动画布/滚轮缩小），同时把结论写成右下角告警；`watchResize()` 在容器尺寸变化后重新 framing。

**顺带查明的二级缺陷**：`#graph` 与 tabs / 图头 / 图例同在一个 flex 列里，任何兄弟节点的行数变化都会从画布身上抢高度，而 `fit()` 是对着当时的高度算的。四处收口：图例与标题在**画图之前**落 DOM；告警条改成 `.canvas` 的绝对定位覆盖层（实测有它没它画布都是 516×272）；`.graph-head` 禁换行，标题超长省略号截断、全文挂 `title` 供 hover；`.legend` 高度封顶（宽屏两行 48px、窄屏一行 30px，其余纵向滚）。做完仍残留 15px 偏差，最后定子是 **cytoscape 自己缓存容器尺寸**：DOM 从 500×300 改成 700×400 之后 `cy.width()` 照旧报 500×300，只有显式 `cy.resize()` 才刷新，而 `fit()` 读的就是这个缓存。现在 `fit()` 第一件事就是 `cy.resize()`（实测它不动 zoom/pan）。

**像素复核的方法学坑**：隐藏标签页里 rAF 冻结，live canvas 取不到像素；而且**同一个实例上改 `label` 再 `cy.png()` 拿到的是上一帧缓存**（改前/清空/改回三次直方图逐字节相同）。所以标签存在性的 A/B 只能开**两个新实例**、样式表只差 `label` 一项。

**实测**（屏外实例走 `graph.js` 真实的 `replace/nodePaint/runLayout/fit`，数节点内框的标签色像素）：

| 用例 · 画布 | zoom | 屏上字高 | 可见节点 / 有标签像素 | 标签像素合计 | 对照组（`label:''`） |
|---|---|---|---|---|---|
| 表级 14 节点 · 516×273 | 0.909（下限） | 10.0px | 11 / 11 | 10924 | 44 |
| 表级 14 节点 · 708×330 | 0.909（下限） | 10.0px | 13 / 13 | 13470 | 60 |
| 字段链路 5 节点 · 516×273 | 0.959 | 10.5px | 5 / 5 | 7038 | 16 |
| 字段链路 5 节点 · 708×330 | 1.353 | 14.9px | 5 / 5 | 14663 | 72 |

对照组剩下的 16–72px 是描边抗锯齿，即"没有字"。表级那两个用例触发了可读下限：516×273 下有 3 个节点整块落在 273px 之外（正是告警讲的那件事，拖动画布可见），5 个字段节点则全部在框内。告警里报的画布尺寸与 `getBoundingClientRect()` 逐项相等（516x273 / 708x330），说明 framing 用的就是最终盒子。超限路径不变：整表 1568 列仍拒绝渲染并说明原因。`mvn -o clean package` **259 绿**，真浏览器走完表级/字段级/试解析两视图 + 页签切换，console **零 error**。

> 本节最后那句"整表 1568 列仍拒绝渲染"到 §8.10 就不成立了——盒/行模型把它救回来了，代价是每盒只画有边的前若干列并明说剩下多少没画。

### 8.10 字段级血缘重做：一表一盒、一行一列、线连字段行（2026-09-22）

**症状**。§8.9 修完"读不出字"之后，用户对字段视图的评价是"显示的很差劲，没办法看出血缘关系来"。量出来才服气：`sql/` 语料里 `g05_new_ck_kafka_enterprise_tel_ul_tx` 的字段视图下发 **1568 节点 / 786 边**，旧画法是一千多个飘着的列方块，`capNotice(300)` 直接拒画（空白画布）；能画出来的中小链路里，`kafka.eventbodylist → user_app_data.app_name` 这种"直连"中间其实还折了一层 UNNEST 伪表，它只活在 `edge.hops` 里，画布上没有它的位置——**链路看着断了，而断点恰恰是最需要解释的地方**。

**新画法**（读法对齐 SQLFlow 那类字段级血缘图）：

- 一张表一个盒子（`kind=tableBox`），盒顶一条实色标题行写表名（`kind=headerRow`），盒里一行一个字段（`kind=columnRow`），**边连字段行不连盒子**。
- 盒子**不是** cytoscape 的 compound 父节点：自算布局本来就掌握全部坐标，compound 反而要把坐标换成"相对父节点"的口径、还要跟父框自动外扩较劲。盒子宽高按行数直接写进样式（`HEADER_H + rows*ROW_H + BOX_PAD`，常量在 `graph.js`），列行是同层级普通节点，由 `stackRows()` 从盒子左上角推坐标填进去。
- 表名不写在盒子身上：写在盒子上要么压住第一行列行，要么靠 `text-margin-y` 猜位置。标题行自成一个节点，色块本身就是分隔线，还能单独挂点击（点标题=以那张表为中心展开）。
- z 序：盒 0 / 边 1 / 行 2 / 标题 3。cytoscape 会把边的端点裁到节点边界，所以线只在盒体上走明路，不会被色块盖掉半截。
- 层号取色只给"实体"：盒体永远是最暗的容器（`--box-fill`），层号色写在标题行上，图例的"第几层"照旧读得出来；中间跳盒用虚线暗底。

**撑起可读性的三件事**（都在 `graphColumn.js`）：

1. **单列链路** `chainOf()`：选中某列时沿边正、反两向 BFS，只留它自己的通路。服务端本来就按邻域裁剪，前端不再收这一道的话，"点一个字段"和"展开整表"看起来没区别。焦点不在图里（换过表、hash 里留着旧字段）时返回 null，按整表展开画，而不是画一张空图。
2. **中间跳显形** `flatten()`：`hops.length>=3` 且首尾正好是这条边的两端时，把中间伪列摊成真的 `columnRow`（`local+hop`），边拆成 N 段（id 为 `edge.id>序号`，带 `step="1/2"`），层号按路径长度在两端之间插值——伪表因此落在上下游之间，而不是叠在某一端身上。端点对不上就不猜（UNRESOLVED 边本来就带不出可信路径），保持直连。跨边重复的 hop id 用 `known` 集合去重（cytoscape 撞 id 会直接抛）。每段仍带 `realFrom/realTo`，点任意一段打开的都是**原边**的逐跳证据。
3. **整表展开退化**：`limit = max(1, min(ROWS_CAP=14, floor(ROW_BUDGET=600 / 盒数)))`——表少时每盒 14 行，表多时按整图预算摊薄，宁可字小也不要空白画布。盒内按"焦点列 → 有边的列 → 列名"排序后截断（孤岛列画进来只是多一行没线的槽位，而截断一条链路会让因果断在半空），尾巴折成一行"还有 N 列未画"（`capped`，点了不响应）；两端没画上的段数另外报出来。中间跳的行不是真字段，拿它的标识去打 `/api/column` 只能得到 400，所以只让它当视觉锚点、不挂 `onColumn`。

**实测**（真 jar + 真浏览器；屏外实例走 `graph.js` 真实的 `replace/nodePaint/runLayout/fit`，画布 516×273；下发=服务端 payload，画出=cytoscape 元素）：

| 用例 | 下发 | 画出 | 告警条 |
|---|---|---|---|
| 整表（最宽那张） | 1568 节点 / 786 边 | 34 节点（2 盒 + 2 标题 + 30 行）/ 4 段边，zoom 0.909 | `1540 列未画（2 张表，每表最多 14 列）：点字段行走单列链路，或点表名以该表为中心裁剪；782 段链路因两端列未画而暂不显示` |
| 点列 `vin` | 6 节点 / 5 边 | 8 节点（2 盒 + 2 标题 + 4 行）/ 3 段边，zoom 0.997 | 空 |
| 试解析嵌套子查询 `ods.t_person → #sub1 → dws.t_final` | 3 节点 / 2 边（`hops` 长 3） | 10 节点（3 盒含 1 个虚线 `sub1` + 3 标题 + 4 行）/ 4 段边（`step` 1/2、2/2），TB 三层 y=36/178/310 | `1 个中间跳（CTE/子查询/UNNEST 折出来的伪表）已按边的 hops 显形` |

1540 = 783 + 785 − 14 − 14；4 + 782 = 786，一段都没被悄悄丢掉，只是明说了哪些没画。合成语句是 `INSERT INTO dws.t_final (full_name) SELECT c.x FROM (SELECT CONCAT(a.first_name,' ',a.last_name) AS x FROM ods.t_person a) c`，任何人贴进「解析新 SQL」都能复现第三行。

**顺带修掉的两处口径谎话**：

1. 图例里那句"链路里出现的中间关系仍折在边的 hops 里，点边看逐跳"——hops 已经显形了，它在试解析·字段视图里是句反话，删。
2. `capNotice()` 数的是盒子却报"节点 N 个"。改成单位由调用方给（表级 `节点`、字段级 `表盒`）：判断用什么单位，报错就得用什么单位，否则"N 个"对不上用户看得见的东西。

**一个只有大画布才暴露的缺陷**：告警条被 `watchResize` 整条覆盖。容器一变宽，`fit()` 说"放得下了"、返回空串，回调就把"1540 列未画"一起抹成空条——2700×1055 的窗口里明明只画了 28 行，页面一句话都不说，正是"看不出少了东西"那一类。现在告警条分两半存：`framing`（每次重新 framing 重算）与 `modelWarning`（画图那一次产生），`showWarning` / `paintFraming` / `setWarn` 三个入口各司其职；重扫完成那句 `ui.warn.textContent = ''` 一并删掉，它会把刚画出来的那张图的说明擦干净。实测：同一张整表视图，修前 `#graph-warn` 的 `textContent` 为空、`display:none`，修后是上面那 66 个字符（单行 636px，`max-width:70%` 不截断）。

**验证方法的坑（本轮新踩）**：后台标签页会被 Chrome **冻结**——`document.dispatchEvent(new Event(...))` 收不到自己刚挂上的监听器（实测 detached 元素与 `window` 上的监听器都计到 0），`requestAnimationFrame` 不回调，`ResizeObserver` 不投递。所以"点一下看响应"这类交互不能拿被冻结的那个标签页做。换 Qoder 内置浏览器（`browser-use` MCP）就通了：同一个页面里 `li.click()` 与 `cy.nodes(...).emit('tap')` 都能把处理程序叫起来（`evtFired` 计到 1），整链逐条重跑见下；但它 `visibilityState` 仍是 `hidden`，`take_screenshot` 直接拒绝（`NATIVE_BROWSER_VIEWPORT_UNAVAILABLE`），live canvas 读出来是一片黑（`getImageData` 只有 `0,0,0`）。像素改从 `cy.png()` 拿：它自己建离屏 canvas 同步画，`new Image()` 载入 dataURL 再 `getImageData` 数颜色，本轮整表退化图实测 620×328、1070 种颜色、盒底 `8,13,21` 96913 px、行底 `20,30,43` 63563 px、亮色文字像素 5830（70 种抗锯齿变体）、行标签 164×17 @11px——字是真画出来了，不是"我以为"。另外改完 jar 只换 hash 不够——ES module 有缓存，必须整页重新导航，否则会拿着旧模块的渲染结果下结论。

**画图顺序的竞态（回归时抓到的真 bug）**。`refresh()` 一路 `await` 取数，每个 `await` 都是让出线程的点，于是**先点的那次可能后画**：慢响应回来时谁都不认谁，画布、右栏、`state.payload` 会被一次早已过期的点击接管。本轮就是实测撞上的——切进「试解析·字段」时自动聚焦首列，那一次取数在后台落了地，把我随后点的整表退化图连画带右栏一起换成了两行的小链路。新画法让这条更常触发：整表展开要摊 28+ 行、跑一次布局，比旧的三五个方块慢得多。修法是一个模块级意图序号 `renderSeq`：`refresh()` 进门领号，`drawTableGraph/drawColumnGraph/showTableDetail/showColumnDetail` 取数回来后先比号，不是自己就整段收手（`state.payload`、`state.columns` 都在守卫之后才写，不留半次更新）；「方向」下拉只重画中栏，也领一个号。试解析那三条路径不打下游、同步画完，天然没有这个窗口。验证是造出来的：把 `table=g05_new_ck…` 那一条响应人为拖 700ms，然后连点大表 → `kafka_table` → `hive_table`，最终画布、右栏、hash 三者都停在最后一次点击，过期那次一个字都没往 DOM 里写。

**回归**：`mvn -o clean package` 两遍 **259 绿**（改序号守卫后又 clean 跑了一遍）；`run-lineage.bat sql` 与 §11.1 基线逐项相等（15 文件 / 14 输出表 / 0 WARN / 2327 字段边，四类泄漏探针与双向引用全 0）；表级 DAG 与试解析·表级不受影响（`runLayout` 在没有盒子时排的就是节点本身）；`/api/*` 契约与解析层零改动。交互整链在内置浏览器里逐条走：左栏点表→出盒子（2 盒 + 2 标题 + 6 行 / 3 边）、点字段行→单列链路（2 行 / 1 边，hash 加 `&column=`）、点段边→原边证据、点标题行→回到整表展开、点中间跳行/跳盒→按设计不响应；冷启动直接吃 `#view=column&table=…` 深链也能画出退化图并说明缺了什么；console 除 vendored cytoscape 自带的 `wheelSensitivity` 提示外零 error。

### 8.11 字段级血缘再改：布局进服务端 + 跟随式动作浮层（2026-09-23，**已被 §8.13 推翻**）

> 本节记录的两件事仍然成立：**语义结论要在 Java 侧可断言**（层号、预算、自环、告警——§8.13 全部留着），以及那两处**静默失效**的教训。被推翻的只有"像素也进响应"和"动作条跟着行走"这两条，理由见 §8.13。下面的盒高/浮层坐标读数按当时的口径读。

**要求**（用户原话）："字段血缘看起来还是不优雅，模仿对方返回数据的接口方式。还有模仿对方的 ui 字段展示方式"。两个口径由 AskUserQuestion 定死：

- **接口幅度 = 连布局一起照抄**：一次请求不只回数据模型，还要回**画出来长什么样**——每个盒、每一行的 `x/y/width/height` 连同标签字号字体全部服务端算好下发，前端只做"左上角 → cytoscape 中心点"的换算。原有 REST 端点一条不动，前端只换打哪个。
- **字段 UI = 跟随式浮层 + 三按钮**：点一行字段，在它旁边贴出动作条（`只看这一列` / `列出字段` / `复位` + `×`）；画布只留这一列的通路，每个盒只留参与这条链路的行；右栏继续放边证据与字段清单。

**为什么布局非搬到后端不可**：这套图的尺寸口径（盒宽 162、行距 16、标题带 21.96875、盒底留白 25.96875）一旦只有前端懂，"这张图长什么样"就没法在单测里断言，也没法被第二个消费方（导出、截图、别的客户端）复用；前端再算一遍就是一份会走样的副本。§8.10 那版画在 `graph.js` 里、算在浏览器里，Java 用例对它无能为力——这就是它两轮里各自翻过一次车（读不出字、整表拒画）的结构性原因。

**后端**（新包 `com.bigdata.lineage.web.sqlflow`，5 个类）：

| 类 | 职责 |
|---|---|
| `SqlFlowStatement` / `SqlFlowChain` | 对方形状的骨架：`dbobjs`（表/列两层的模型对象）+ `processes` + `relationships`，语句切分成"链"，一条链是一段可画的通路 |
| `SqlFlowLayout` | 布局与度量：`BOX_W=162 ROW_H=16 ROW_W=160 ROW_TOP=21.96875 BOX_PAD=25.96875 RANK_GAP=220 BOX_GAP=22`，字号 `12`、字体 `Cascadia Mono, Consolas, monospace`，字符宽 `7.2` 用来截标签。列号 = 层号（`rankIndex*(BOX_W+RANK_GAP)`），同列内按行数摞，`PlacedBox/PlacedEdge/Plan` 三层产物 |
| `SqlFlowAssembler` | 一次请求装配出一份"数据模型 + 布局"：`ROWS_CAP=14`、`ROW_BUDGET=600`（整图行预算按盒数摊薄）、`MODEL_ROWS_CAP=200`（进模型的列上限，超出只报数不登记）、`BOX_CAP=300`（超限整图不画，`drawn=false` + `warning` 原样送到前端） |
| `SqlFlowContext` | 一次请求的解析上下文（表/列标识 → 模型对象、被折掉的关系） |

`LineageApiController` 加两个端点（GET 走快照、POST 走页面贴的 SQL，**同一个装配器、同一份口径**）：`GET /api/sqlflow/graph?table&column&focus&depth`、`POST /api/sqlflow/graph {sqltext|sql, focus}`。响应形状与逐字段口径见 §7.3——`data.{sqlflow,graph,summary,sessionId,metaInfo}`，`graph.elements.tables[].columns[]` 就是"盒里的一行"，`columns[].id` 是 `盒id::行序号`。

**前端**：`graphColumn.js` 删了，换成三个模块——`sqlflowModel.js`（把 `dbobjs/relationships` 索引成右栏要的列卡/实体卡/逐跳证据）、`sqlflowView.js`（照抄坐标画盒/画行/连线 + 裁剪后重排）、`sqlflowPop.js`（浮层只管贴上去、别溢出画布，坐标由调用方给）。两条约束值得钉在文档里：

1. **前端不抄一份尺寸常量**。`measureGeometry()` 从响应里**量回**行距、标题带、盒底留白（行距 = 相邻两行 `y` 之差；标题带 = 首行 `y` − 盒顶；盒底留白 = 盒高 − 全部行高），`gapOf()` 再取同列相邻两盒之间**最小**的那条缝。常量一旦进前端就是第二份真相，服务端改了它就漂，而漂的表现是"行错位"这种没人会报警的样子。
2. **`runLayout()` 只管表级图**（§8.7 那套按 `layer` 自算 + LR/TB 挑向），字段级不再走它——坐标已经是现成的，前端排一遍就是把服务端结论覆写一遍。

**"只看这一列"是纯客户端裁剪**，不发第二次请求：这次响应给的就是这一片的全部结论，再打一次只会拿到同一份。`chainOf()` 沿边正向到下游、反向到源头收出通路（端点可能是行也可能是盒，所以只按标识收集，不假设类型），`restack()` 让留下的行从盒顶往下重摞、盒高跟着行数变、同列往上挤——**只有纵向动**，横向（哪一列）一律不动，因为列号就是层号，是这套图的语义本身，没有理由在用户点一下之后把"第 2 层"挪到"第 1 列"的位置上。

**回归时抓到的两处静默失效**（都是真浏览器点出来的，281 个 Java 用例全绿照样带着走）：

| 症状 | 根因 | 为什么静默 |
|---|---|---|
| 点字段行**只出右栏，浮层永远不出现** | `main.js#placePop()` 写了 `cy.renderedPosition(node)`——cytoscape 的 **Core 上根本没有这个方法**，只有元素有 `node.renderedPosition()` | `guard()` 把异常转成 `console.error` + 告警条一行字 + 右栏错误块，页面不崩、不弹栈，而 `pickRow()` 里 `renderColumn()` 在 `showPop()` 之前，右栏照常刷新——只看现象会以为是浮层样式没生效。**这是本轮唯一真正卡住的缺陷** |
| 点"只看这一列"：行确实重摞了，**盒子和标题带还停在原尺寸**（大盒套小行，看着像裁剪没生效） | `sqlflowView.js#interactiveView()` 的 `base` 条目少存了 `id`，`place()` 里 `cy.getElementById(undefined)` 拿到**空集合** | cytoscape 对空集合写 `position()` / `style()` 是**静默 no-op**，不抛、不警告。定位办法是给元素原型上的 `style`/`position` 打桩，日志里看见 `this.id() === undefined` 才认出来——"没抛异常"在 cytoscape 里不等于"写进去了" |

两处都是同一个教训：**这个前端没有 JS 测试，`StaticAssetIntegrityTest` 只钉得住文本层面的事（注释闭合、import 目标、DOM id），空集合写入和被 `guard` 吞掉的异常都不在它射程内**。所以"真起 jar + 真浏览器点一遍"不是仪式，是这道闸门本身——本轮每条读数：

- 快照字段视图（`sql/` 语料，画布 516×273）：31 元素 / 23 边，其中 2 `tableBox` + 1 `hopBox` + 3 `headerRow` + 25 `columnRow`；告警条四段拼接齐全（放不下 28 个节点 / 1 个中间站 / 13 列未画 / 13 段链路两端未画）。
- 画布上对首行 `n0::n0` 派发真实 `MouseEvent`（不是 `.emit('tap')`，走的是命中测试那条路）→ 浮层可见，`left 106.958px top 93.9167px`（宽 274，不出 516×273 的界），标题 `kafka_user_app_data.operation（点动作条外任意处可收起）`，四个按钮齐，该行上 `hot`。
- 点"只看这一列" → `n0` 高 **41.96875**、`n2` 高 **169.96875**（= `BOX_PAD + 行数×ROW_H`，即 1 行和 9 行），`n1` 整个隐掉，两个盒子的标题带**跟着各自的盒走**，可见行 9 / 可见边 9；`cy.png({full:true})` 出来的整图 930×174（修前约 250 高——空盒子占着没缩的那截），盒底边线像素分别读到 `#4fd6c4`（层 0）与 `#ffcb52`（层 1），缩下去的盒底下 +8/+40px 是 `rgba(0,0,0,0)`（真没画东西，不是被盖住），行内底色 `rgb(20,30,43)` = `--row-fill`。
- 点"列出字段" → 实体卡「字段数 27 / 画进盒子 14 / 27」；点"复位" → 三盒高度回到 57.97 / 169.97 / 249.97，25 行 23 边全在；点 `×` → 浮层收起，`hot` **故意留着**（收起的是动作条，不是"取消选中"）。
- 在边的中点（`edge.renderedMidpoint()`）派发点击 → 右栏字段边证据：命中 1 条边、`IDENTITY · 直通`、置信度 95%、引擎 FLINK、语句 `ck/user_app_data_ck_dml.sql#1`；点标题带 → 该表实体卡；点空白 → 只收浮层。
- 试解析（合成 `INSERT INTO demo_sink SELECT a.user_id, a.amount + b.rate AS total FROM kafka_src a JOIN dim_src b ON a.user_id = b.user_id`）→ 「切分 1 条语句 / 折叠后 3 条字段边 / 3 张表」；试解析·表级 3 节点 2 边；试解析·字段 3 盒 5 行 3 边；点行同样弹浮层（`kafka_src.user_id`），裁剪后同样缩到 41.97 且标题带跟随；点标题带出实体卡，非实体表的「铺开整表字段链路」重新 POST 后按不裁剪重画，**无 400**（§8.10 那个"拿中间跳行的标识去打 `/api/column`"的坑在新形状里不存在：中间站是盒不是列，行只登记真字段）。
- console **0 error**；84 条 warn 只有两类：vendored cytoscape 自带的 `wheelSensitivity` 提示，和 `paintSqlFlow()` 逐元素创建时带 `style` 的 "Setting a `style` bypass at element creation"——后者**留着**：尺寸本来就得按响应里每个盒/每行各写一份，改成批量样式表反而要把服务端坐标再翻译一遍。

`/api/graph/column` 与 `/api/edge/column` 保留但前端不再调用（契约与单测还在，见 §7.3）；vendored 依赖仍是 cytoscape 一件，没借"照抄布局"之名把 dagre 请回来。

**验证方法的两个坑**（补充 §8.10 那一段）：内置浏览器（`browser-use`）的 `click` 对这个隐藏标签页里的应用内按钮**会返回"Successfully clicked"却什么都不做**，所以 DOM 走 `evaluate_script` 里的 `.click()`、cytoscape 走真实 `MouseEvent` 派发；`evaluate_script` 只认 `waitForStableDom`，传 `waitForStablePage` 会被 additionalProperties 拒掉。

**回归**：`mvn -o clean package` **281 绿**（§8.10 的 259 → 本轮新增 `SqlFlowStatementTest` 与 `SqlFlowAssemblerTest`：形状、坐标、行预算、超上限不画全在 Java 侧断言——这正是"布局进服务端"换来的东西）；`/api/overview` 与 §11.1 基线逐项一致；表级 DAG、试解析·表级、右栏证据块不受影响。

### 8.12 回归这一轮：上一节落地后改出来的五处缺陷（2026-09-24）

用户原话「**回归代码是否有 bug 再检查一下字段血缘渲染**」。做法沿用 §11 那道闸门的老顺序：逐行复核本轮 diff（后端 `web/sqlflow` 五个类 + 前端三个模块）→ clean 构建 + 语料基线 → 真浏览器把字段血缘整条链再点一遍。**复核出五处，全修**；两处判为设计内、不动（写在后面，免得下一轮又被当 bug 捞出来）。

| 症状（用户看得见的） | 根因 | 现在的钉子 |
|---|---|---|
| 点盒顶的表名**没有以那张表重开一片**，只是把字段清单列出来——而图例明写"点表名以那张表为中心展开" | `SqlFlowLayout.boxJson()` 压根没往几何里写 `qualifiedName`，而 `sqlflowView.js` 读的是 `box.qualifiedName` → `data.table` 恒为 `undefined` → `pickBox()` 的 `if (!data.local && data.table)` 永远不成立 | 后端补 `qualifiedName`（盒与行都补），`SqlFlowAssemblerTest#geometryCarriesTheAddressableRelationName` 用两条断言钉住：**每个盒的 `qualifiedName` 必须是 `metaInfo.ranks` 的键**（能被打进请求的名字才算名字），**每行的必须以其盒的关系标识 + `.` 开头** |
| 字段链路里点了"只看这一列"，再切到**表级 DAG → 整张画布白掉**，只剩一句过期告警 | 换视图只换了图，没清字段视图留下的句柄：`state.sqlflow.view` 还指着一张已经不算数的图，浮层也还挂在 `.canvas` 上，点它就是把**旧裁剪**盖到新图上——`crop()` 第一步是 `cy.elements().style('display','none')`，再拿旧标识往回找，新图上谁也找不到 | `main.js#dropSqlFlow()`（清 `sqlflow`/`popTarget` + 收浮层）在 `drawTableGraph()`、`drawParsedTable()` 与"没选表"分支里各调一次；`sqlflowView.js#crop()` 开头加 `if (!cy.getElementById(rowId).nonempty()) return view`——**空集合在 cytoscape 里是静默 no-op，但"全部隐藏 + 找不到要显回来的"不是**，所以这一层守卫是必需的而不是防御性冗余 |
| 在邻域里点了**别人家的字段行**，再改深度 → 请求变成 `table=X&column=<Y 的列名>`，服务端在 X 里找不到那列，焦点丢了 | `pickRow()` 只更新 `state.column`（一片邻域仍以选中的表为中心），而 `drawColumnGraph()` 以为二者同源 | 不同源就交完整标识：`sameTable` 判 `state.column` 是否以 `state.table + '.'` 开头，是则拆短名给 `column=`，否则整条给 `focus=`（邻域照 X 裁、焦点保那列）。顺手删了因此变成死代码的 `columnNameOf()` |
| 超上限"不画"时标题照念 `metaInfo.boxCount`：**"602 表盒 / 3 000 字段行"配一张白画布** | 标题只读 meta，不知道 `drawn` 是 false | `meta.drawn === false` 时标题改口"（没画：只给数据模型，见下方说明）"，数字留给告警条 |
| 自环（`INSERT INTO t SELECT ... FROM t`）那几段**凭空消失**，既不画线也不说为什么 | 布局层两端同行的段两处 `continue` 只跳过不记账 | `Plan.skipped` 计数 + 装配层一句「N 段两端落在同一个盒子或同一行（自环），不画线」；`SqlFlowAssemblerTest#selfDependencyDrawsNoLoopButSaysSo` 断言边为空**且**告警里有"自环" |
| （收紧）浮层藏着时程序化点"列出字段"会 `state.sqlflow.index` 空指针 | `chain`/`reset` 两个动作都判了 `state.sqlflow`，只有 `columns` 漏了 | 三个动作口径一致：都判 `state.popTarget && state.sqlflow` |

**判为设计内、这一轮没动的两处**：① 同一个中间跳的两头本来就该是两条关系（`groupsOf()` 按 `segment.getTo()` 分组，所以一条折过来的两跳会共享 `qualifiedName`）——这是 SQLFlow 那个形状本身，画布上每条线各自开得出自己那半的证据；② 控制器不替调用方规整 `focus` 原文——填错焦点的表现是"不裁剪"，而前端本来就先用 `rowIdOfColumnId()` 试过才裁，不会因此画歪。

**这一轮实测读数**（新 jar、`sql/` 快照，画布 516×273、缩放 0.83）：

- 加载即 31 节点 / 23 边全可见，三盒高 **57.97 / 169.97 / 249.97**（= 2 / 9 / 14 行）；标题 `user_app_data 全部字段 的字段链路（3 表盒 / 25 字段行 / 23 条关系）`。
- 点中间站那一行 `n1::n1` → 浮层标题 `[ck/user_app_data_ck_dml.sql#1]#unnest1.app_id`，右栏 `app_id（unnest1）`：**中间站是点得着、也叫得出全名的**。
- "只看这一列" → 可见 9 节点 / 2 边，三盒都缩到 **41.97**（各留 1 行），告警条尾巴是"当前只画选中字段的通路（点浮层「复位」还原整片）"；"复位" → 回到 31 / 23 与 57.97 / 169.97 / 249.97，裁剪那句跟着消失；`×` 只收浮层。
- **白屏回归专测**：开着浮层切"表级 DAG" → 浮层 `display:none`、表级 2 节点 1 边全可见；此时**程序化强点**已经没意义的"只看这一列"和"列出字段" → 画布一个元素都不掉，右栏老实回答"`…#unnest1.app_id` 没有出现在字段清单里"，页面不抛。
- **盒顶表名手势**：点 `n0#h` → `location.hash` 变 `#view=column&table=kafka_user_app_data` 并重画（修前它只会列字段、hash 不动）。
- **跨表焦点**：选中 `user_app_data.mcu_software_ver`（别人家的列）后把深度从 2 改到 3 → 请求 `/api/sqlflow/graph?table=kafka_user_app_data&focus=user_app_data.mcu_software_ver&depth=3`，画布按 kafka 那张表裁邻域、焦点保住那列，链路是 `kafka_user_app_data.operation → user_app_data.mcu_software_ver`（`e17`，非 synthetic）。
- **几何 ↔ 模型身份核对**：25 行逐条比 `qualifiedName` 与按 `modelId` 反查到的模型列 `qualifiedName`，**0 处错位**（点一行反查到别的列这种错没发生）。
- 试解析（同一条合成 JOIN）→ 3 盒 / 5 行 / 3 边，盒按 rank 左→右排开、两个源同列上下堆叠（x 81 / 81 / 463）；`dim_src.rate` 裁到 `n1::n0 → n2::n0` 两点一边。
- console **0 error**；warn 仍是 §8.11 记过的那两类（`wheelSensitivity` + 逐元素 `style` bypass）。
- **后台标签页的坑更新**：这一轮页面在后台（`document.visibilityState === 'hidden'`），对 `<canvas>` 派发真实 `MouseEvent` / `PointerEvent` **一次 `tap` 都没进命中测试**，只剩 `cy.getElementById(id).emit('tap')` 能用——§8.11 那次能派发真实事件是因为标签页在前台。**读数口径不变，事件入口要按前台/后台分开记**。

**回归**：`mvn -o clean package` **283 绿**（281 + 上表两条新断言）；`run-lineage.bat sql` 与 §11.1/§14 基线**逐项一致**（原始字段边 2369 / 图字段边 2327 / 表边 6 / 节点 14 / 有字段的表 12 / maxLayer 1 / 环 `[]` / 四类探针与伪节点全 0）；`/api/overview` 仍是 `D:\project\flinkLearn\sql`（15 文件 / 22 语句 / 14 表）。

### 8.13 字段级血缘第三次重做：像素退回浏览器，后端只给网格（2026-09-25）

**要求**（用户原话）："**全部重新实现 当前的版本 字段血缘实现的 非常不好**"。这节记录为什么把 §8.11 的"坐标进响应"退回来、退到哪一步为止，以及重做后的实测。解析层（三个提取器、`ColumnLineageEngine`、`SqlFlowChain` 的切链与折边）一行没动——语料基线逐项相等就是这条边界的证明。

**§8.11 那个赌注方向对、对象错**。它说"尺寸口径一旦只有前端懂，这张图长什么样就没法在单测里断言"，于是把像素搬进 Java。可单测的从来不是像素，是**语义结论**：谁在第几层、同层谁在前、哪些行进盒子、这条线是不是自环、超上限画不画。像素进响应换来三件坏事：

1. **字号字体抄了两份**：后端拿 `Cascadia Mono 12px` + 字符宽 `7.2` 估标签宽度，浏览器再真排版一次。估错的表现是"标签被截/行错位"，两边各有其理由，谁也说不服谁。
2. **cytoscape 的节点是中心点 + 显式尺寸**，盒高得手工加总 `HEADER_H + rows×ROW_H + BOX_PAD`，行要 `stackRows()` 从盒顶推。上一轮"图上看不到字"那 350 行几何兜底就是从这里长出来的——canvas/cytoscape 上的字必须跟着缩放走，缩放又得 fit 容器，于是永远在"放不下"和"字太小"之间二选一。
3. **两套落位口径在同一张图上打架**：服务端给绝对坐标，前端"裁剪后纵向重排"，谁覆写谁没有规则可言。

**本轮三个口径**（开工前定死，不再回头问）：

- **渲染 = HTML `<div>` 摆盒 + 一层 SVG 走线**，字段视图彻底不碰 cytoscape（表级 DAG 照旧用它）。
- **交互 = 悬停只在原地染色，`只看这一列` 才裁剪**；动作条从"贴在刚点的那一行旁边"改成**钉在画布右上角、可拖动**。
- **响应 = 数据模型 + 证据 + 网格（`layer` / `slot`）**，不含任何像素。

**后端瘦下来的分界线**（`SqlFlowLayout` 删掉 `BOX_W / ROW_H / ROW_W / ROW_TOP / BOX_PAD / RANK_GAP / BOX_GAP` 与整套标签度量）：

| 留在 Java 的判定 | 谁钉着它 |
|---|---|
| `layer`（层号，`LayeredDagBuilder` 的 rank）+ `slot`（同层序号，`gravity()` 取上下游平均位置排完，最小者落 `slot=0`） | `placementIsAGridAndCarriesNoPixels`、`downstreamBoxSitsInALaterLayer` |
| 行标识 `盒id_c序号` 与反查键 `modelId` | 同上（逐行断言 `id == box.id + "_c" + i` 且 `modelId != null`） |
| 行预算 `ROWS_CAP=14` / `ROW_BUDGET=600` / `MODEL_ROWS_CAP=200` / `BOX_CAP=300`、`drawn=false`、四类告警、自环 `skipped` | 原有用例一条不动 |
| `qualifiedName`（盒与行都带，点盒顶要拿它打请求） | `geometryCarriesTheAddressableRelationName` |

**新加的钉子是反向的**：`placementIsAGridAndCarriesNoPixels` 断言盒子里**没有** `x/y/width/height`、格子里**没有** `x/y`。后端哪天顺手多给一个像素，这个用例就该红——§8.11 的教训不该只靠文档记住。原来那条 `boxGeometryFollowsRowCount`（盒高 = 行数×行距）随之删除，用例总数不变（`SqlFlowAssemblerTest` 仍是 19 条）。

**标识形状一起换掉**：`n0` / `n0::n0` 是 cytoscape 时代的产物，DOM 里 `.` 和 `::` 在 `querySelector` 中都是要转义的雷。现在是 `b0` / `b0_c0` / `e0`，可以直接当 `id`、当 SVG 路径端点用。

**前端**（`sqlflowView.js` 从"照抄坐标"变成渲染层本体，`sqlflowPop.js` 删除、换 `sqlflowBar.js`）：

1. **两趟量法**：先把盒的 `left` 落下去、保持可见读 `offsetHeight/offsetTop/offsetLeft`；**全部读完**再成批写 `top`；线在第二次落位之后才画，那时才有端点。读写交替会每一格触发一次强制重排，28 行的图就卡在那儿。
2. **缩放 = `transform: scale(z)` 作用在 `.slf-stage`**，外层 `.slf-sizer` 按 `w*z × h*z` 占位喂滚动条。**字号恒定**：放大是"把整张纸放大"，不是"把字压小"。fit 的可读下限 `READABLE_Z=0.8`（12px×0.8=9.6px，再小就不是能读而是有个形状）。
3. **`.slf-sizer` 只在整图放得下时居中**（margin 居中）。真超出容器时留 0——超出还居中，左边那截就成了滚不到的黑边。
4. **`coneOf()` = 祖先闭包 ∪ 后代闭包，不是无向可达**：共用同一个来源的两个兄弟列会被无向连通成一片，那不是血缘，用户照着那片染色读出来的因果是假的。
5. **悬停 `preview()` 与选中 `select()` 分层**：`selected` 非空时 `preview` 直接返回（不覆盖既有染色），`unpreview()` 回到"选中态的染色"而不是擦干净。染色只切 `.on/.soft` class，**一个元素都不挪位**。
6. **`crop()` 的先判存在留着**（§8.12 那条）：否则就是"全部隐藏 + 再也找不到要显回来的"。
7. **字段视图是 `position:absolute; inset:0` 盖在 `#graph` 上，而不是把 `#graph` `display:none`**：cytoscape 会缓存容器尺寸，藏一次就得 `resize()` 一次，overlay 根本不碰它。
8. **PNG 导出没有 `cy.png()` 可用了**：`view.png()` 是手写 canvas 重画（`snapshot()/paintBox()/roundRect()/clipText()`），用的就是刚量出来的那批数字。**没画盒子时不再偷偷导表级图**——`main.js#exportPng()` 在字段视图挂着而画布为空时直接告警返回（§8.12 那一类"说谎的导出"）。
9. **`graph.js#fit()` 跟着简化**：它现在只服务表级图，可读下限直接按 `NODE_FONT_PX` 换算，不再遍历可见节点取最小字号，`cy.nodes(':visible')` 那个选择器也一并去掉——字段图不再往 cytoscape 里塞东西，"可见"这个概念回来了。

**动作条为什么钉在右上角**：§8.11 那两处静默失效都出在"贴位置"上（`renderedPosition` 不存在、空集合写坐标）。钉死在角落后位置与图的内容无关；代价是它会盖住东西，于是四个角分工写进 `app.css` 注释——**左上=链路起点、右上=动作条、左下=缩放、右下=告警**。真要看右上角那块就拖 `.bar-grip`。**`×` 走 `clearRowSelection()` 而不是 `bar.hide()`**：只藏条子、画布上那一列还亮着，用户就不明白自己在看什么。**标题单行省略号 + 全文挂 `title`**：第一版让它贴着行走时被实测成换行六行的巨条（`grid-template-columns: auto minmax(0,1fr)`，把手占两行）。

**回归时抓到的两处**（都是真浏览器点出来的，Java 侧全绿照样带着走）：

| 症状 | 根因 | 修法 |
|---|---|---|
| 冷启动深链 `#view=column&table=X&column=vin`（裸列名）→ 图画出来了、标题写着"vin 的字段链路"，但**选区为空、不染色、动作条不出现** | `showSqlFlow(…, focus=state.column)` 把裸名 `vin` 喂给只认 `table.column` 的 `rowIdOfColumnId()` → 反查不到行标识，`pickRow()` 那一步整个跳过。又是"点了没反应"而不是报错那一类 | `drawColumnGraph()` 分两路：给服务端的 `column=` 用裸名，给画布的焦点补全成 `table.column`；hash 事后规整回全名 |
| 字段视图挂着、画布为空时点「导出 PNG」→ **导出了一张根本没在看的表级图** | `exportPng()` 拿不到字段视图的盒子就顺着往 `cy.png()` 走 | 明确告警"这一片没画盒子，没有可导出的字段图"并返回，绝不替用户决定他导的是哪张图 |

**实测读数**（真浏览器，`sql/` 快照，画布 2700×1085）：

- 载荷核对：盒的键集是 `{id,name,qualifiedName,type,local,layer,slot,modelId,columns}`，边是 `{id,sourceId,targetId,synthetic}`，**一个像素都没有**。
- 冷启动深链（裸名与全名各一次）→ `.sel=b0_c0`、锥内 4 行 `.on`、3 条边上色、`.slf-stage` 带 `tracing`、动作条可见、hash 规整成 `table.column`。
- 悬停（无选中时）→ `.soft` 落在锥内 4 行 + 1 条边，**所有盒与行的 `left/top` 一个数都没变**；`pointerout` 擦干净。
- 点行 → 选中 + 右栏列卡 + hash；`只看这一列` → 1 行隐藏、stage 重排、告警"当前只画选中字段的通路（点动作条「复位」还原整片）"；`复位` → 行全回来、告警消失；`列出字段` → 实体卡；`×` → 染色与条子一起撤。
- 缩放 −/＋/适应读数 `0.75 / 0.9 / 1.0`，Ctrl+滚轮 `0.84`，普通滚轮只滚容器不动图，`.slf-sizer` 始终等于 `w*z × h*z`；拖拽平移 `scrollLeft` 322→372，`.grabbing` 跟着切。
- 点线中点 → 右栏"字段边…命中 2 条边"；点盒顶表名 → 以那张表重开（28 行、动作条收起）。
- 导出 PNG 1048×728，**614 种颜色**，含选中行酸绿 `rgb(182,227,74)` 与层 0 边线 `rgb(79,214,196)`——手写 canvas 那套装得下盒、行、线、箭头和层色。
- 切回表级：cytoscape 画布在、`#sqlflow` 隐藏、动作条隐藏。
- 试解析（CTE + `SUM`）→ 4 盒 `orders L0 / picked L1（虚线本地盒）/ SUM L2（虚线本地盒）/ total L3`、7 行 5 边、2 条 `synthetic` 虚线、告警"2 个中间站（CTE、子查询、UNNEST 与聚合函数）已展开成盒子"。
- 动作条实测 `311×58` 钉在 `2678,153`（右上角），标题一行带省略号、`title` 挂全名；整图居中时 sizer margin `1088px / 465px`，两个盒落在 `1415,642` 与 `1697,632`。
- console **0 error**，warn 只剩 vendored cytoscape 自带的 `wheelSensitivity` 一条。§8.11 记的第二类（逐元素 `style` bypass）随 cytoscape 退出字段视图一起消失了。

**验收工具本身的坑（更新 §8.11 末段）**：内置 `browser-use` 浏览器本轮给的视口是 **0×0**（`take_screenshot` 直接回 `NATIVE_BROWSER_VIEWPORT_UNAVAILABLE … viewport=0x0, visible=false`），在它里面所有布局读数都没有意义——fit 会因为一个根本不存在的原因被夹到 0.8。**"这台机器上有浏览器"不等于"这个浏览器有视口"**，而 DOM 类断言在 0×0 里照样全绿。换 `kimi-webbridge`（真浏览器 + 真扩展）拿到 3432×1274 窗口后读数才作数；截图走 `curl` + `node -e` 解 base64（`screenshot.sh` 依赖 `jq`，本机没装），临时文件写 `$TEMP`（Windows 侧看不见 git-bash 的 `/tmp`）。

**回归**：`mvn -o -Dmaven.repo.local=D:/maven clean package` **283 绿**（总数与 §8.12 持平：删一条像素断言、换一条"不许有像素"）；`run-lineage.bat sql` 与 §11.1/§14 基线逐项一致；vendored 依赖仍只有 cytoscape 一件，**没有借"重做渲染"之名把 dagre 或任何布局引擎请回来**（网格是后端给的，浏览器只做落位）。`/api/overview` 仍是 `D:\project\flinkLearn\sql`。

### 8.14 回归 §8.13 这一轮：逐行复核 + 真浏览器复验抓出的九处（2026-09-26）

**要求**（用户原话）："**重新回归代码，是否有 bug**"。口径照 §8.12：先逐行读本轮 diff（后端 `sqlflow` 包 + 前端四个模块 + CSS + index.html + 用例），找静默失效、句柄残留、竞态、标识不匹配、死代码与泄漏；再把改过的东西在真浏览器里逐条读数。解析层一行没动。

**九处定性：全部修**。前七处出在逐行复核，后两处只有点得出来、读得出来，代码上看不出来：

| # | 症状 | 根因 | 修法 |
|---|---|---|---|
| 1 | 页面一打开动作条就带着空标题挂在右上角，且永远藏不掉 | `.bar` 的 `display:grid` 是作者样式，盖过 UA 的 `[hidden]{display:none}`——`hidden` 属性成了装饰。`.banner`、`#sqlflow` 早就各写了一条，本轮新增的 `.bar` 漏了 | `.bar[hidden] { display: none; }`。**读数口径是 computed `display`，不是 `el.hidden`**：上一轮只读 `hidden` 才让它出门的 |
| 2 | 拖过把手后切窗口，之后再动鼠标，条子还会跟着走 | `up()` 只解了 `pointermove/pointerup`，`pointercancel`（系统收走指针：切窗口、触摸被手势打断）那条路径没人清理 | 补 `grip.addEventListener('pointercancel', up)`；顺手删掉零引用的 `visible()`（它的消费者是 cytoscape 的 pan/zoom，随 cytoscape 退出字段视图一起没了） |
| 3 | 盒宽 190px 在 `app.css` 和 `sqlflowView.js` 各存一份，改一处另一处不知道 | 列间距 `COL_GAP` 是加在常量 `BOX_W` 上的，而真实宽度只有浏览器排版后知道 | 删 `BOX_W`，`layout()` 用实测最宽盒 `live.reduce(max box.w)` 算列距；CSS 那侧注释写明"宽度只写在这里，别去 js 里抄一遍数字" |
| 4 | `SqlFlowLayout` 四个访问器零引用 | `PlacedBox.getLayer/getSlot`、`PlacedEdge.getSourceId/getTargetId`——`boxJson()/edgeJson()` 同类直接读字段，绕过了它们 | 删（`Plan.getRanks()` 仍被 `SqlFlowAssembler:754` 用着，留着） |
| 5 | 字段视图挂着、容器一变尺寸，右下角告警报"画布 … 放不下 N 个节点"——讲的是用户根本没在看的表级图 | 字段视图不往 cytoscape 塞东西，但上一张表级图还在它里面，`watchResize(cy, …)` 照样 `fit(cy)` 照样报 | 回调加 `if (!state.sqlflow)` 静音；字段视图撤掉后自动恢复 |
| 6 | 解析器将来新增一种 `derivation`，每条未知边都要回读一次 CSS 变量 | `edgeInk()` 的兜底分支每次 `token()` 一遍 | 兜底换成一次性派生的默认对象：画默认色，而不是画不出来 |
| 7 | 裁剪态点 `×`：画布只剩裁过的三行、没有选区，告警却写着"点动作条「复位」还原整片"，而那个按钮已经不在了 | `clearRowSelection()` 只撤染色和条子，没管裁剪 | 裁剪算这次选中的一部分：`view.cropped` 时先 `view.reset()` 再收条子（实测 2 行 → 15 行，告警尾巴同步消失） |
| 8 | 手动缩放过（点 −/＋ 或 Ctrl+滚轮）之后拖窄容器，图被推到要空滚一千像素才够得着的地方 | `refit()` 的非 auto 分支只重报 framing，不重跑 `apply()`，于是 `marginLeft=987px / marginTop=379px` 这组旧居中距留在原地（auto 分支实测 `ml=0`） | 非 auto 分支也 `apply()`——它不动 `state.z`，用户定的倍率照旧作数，只有居中距跟着新容器走。main.js 那句注释同步改口"只重算 framing 和居中留白" |
| 9 | 深链带一个没画进盒子的列（标识不存在，或被 `ROWS_CAP=14` 裁在盒外）：标题写着"`X` 的字段链路"，画布上一行都不亮，告警只有"N 列未画"这种通用交代 | `showSqlFlow()` 在 `rowIdOfColumnId()` 反查不到时静默 `return` | 把这一列点名进告警："`X` 不在这次的画布上（标识不存在，或被行预算裁在盒外）：只画了邻域，没有可高亮的行" |

**判为设计内的几处**（写下来是为了下一轮不必再猜）：悬停在已有选区时不预览（`preview()` 直接返回，实测 `withSel soft=0 / noSel soft=5`，`pointerout` 擦干净）；`×` 之后 `state.column` 仍留着（它是请求参数，不是画布状态）；`entityCard()` 对每列跑一次 `columnCard`（O(列×关系)，既有口径，本轮没碰）；`graph.js` 表级节点宽度启发式的字面量属于 cytoscape 口径，不在"像素只活在一个地方"的射程里；`ResizeObserver loop completed with undelivered notifications` 只在"收窄容器"那一次出现 0~1 条、静置 3s 不再刷、当次读数全对——`.warn` 本来就 `position:absolute` 叠在 `.canvas` 里不参与布局（§8.13 第 7 条），所以不是应用自激，是窗口被遮挡时 rAF 只有 ~2.5fps 把投递节奏拖慢了，不为此加深度计数器。

**复验读数**（真浏览器，`sql/` 快照，画布 2700×1085，jar 01:15:10）：

- 冷启动 `#sqlflow-bar` computed `display:none`、宽 0。收窄 2700→500：`z 100%→80%`（`READABLE_Z` 夹住）、`ml 947px→0`、sizer `806→644.8px`、告警出现"整图 … 比画布大"；回宽 1200：`z=100% ml=197px`（=(1200−806)/2）。手动 − 到 90% 后再收窄：`z` 保持 90%、`ml→0`（第 8 条改完的读数）。
- 点行 → `.on=10`、`.sel=1`、动作条可见、标题 `kafka_user_app_data.op…`；拖把手位移生效、`pointercancel` 后 `.dragging` 撤掉且后续 `pointermove` 不再跟；`只看这一列` 25→10 行、`×` 后回 25 且告警不再提"复位"；点空白 → 染色 10→0、动作条收起。Ctrl+滚轮 `100%→112%`。
- 深链五种：裸名 `column=app_name` → `sel=1`；异表真列 `kafka_user_app_data.operation` → `sel=1`（`focus=` 这条路本身是通的）；本表全名 `user_app_data.app_id` → `sel=1`；不存在的 `column=` → 服务端 `success:false`「表 user_app_data 没有字段 no_such_col_xyz」原样进告警；不存在却走 `focus=` 的 → `sel=0` + 第 9 条那句点名。
- 裁剪在"单列邻域"里 19→19 **不是 bug**：点的是 `kafka_user_app_data.eventbodylist`，UNNEST 摊出的 9 列与下游 9 列全在它的锥内（逐盒读数 `b0 1/1 b1 9/9 b2 9/9`）。
- 导出 PNG：data URL 206KB，解出来 **1612×728**（=806×364 的 2 倍）、10% 像素非底色、149 个颜色桶——手写 canvas 那套装得下盒、行、线、箭头。试解析（贴 `sql/user_app_data` 那条 UNNEST DML）→ 3 盒 25 行 23 边、标题"试解析·字段（3 表盒 / 25 字段行 / 23 条关系）"、点行出选区与动作条。全程 console error 计数 **0**。截图复核：L0 青 / L1 虚线本地盒 / L2 橙，一行一字段全可读。

**验收工具本身的两个坑**（比缺陷更值得记，因为它们会造出假案）：

1. **后台/被遮挡标签页里 rAF 与 ResizeObserver 投递一起停摆**（`setTimeout` 还在跑，Chrome 隐藏几分钟后压到 1 次/分钟，`evaluate` 里几个 `await sleep()` 就能把整条调用挂到超时）。于是"改容器宽度→观察者不回调"看起来完全等于"代码不响应尺寸变化"——本轮第 8 号疑点（字段视图对 resize 毫无反应）就是这么造出来的**假案**，叫醒后同一探针立刻从 `cw=500 z=100% ml=947px` 变成 `z=80% ml=0`。**凡是 resize 类断言，先测 `document.hidden`、`visibilityState` 和 400ms 内的 rAF 帧数，再信读数**；叫醒用 §8.13 那两条 CDP。同类一条：`evaluate` 返回值过长会被传输截断（一条 5 行探针被砍成 3 行），断言要么短、要么分行回。
2. **后台"杀进程 + 重打包"会谎报完成**：那条命令回"已完成"却根本没跑 maven（jar 时间戳 00:42:29 早于源码 00:48:28，日志文件不存在），于是后面几轮复验读的其实是旧包。**每次复验前先 `curl /js/main.js | grep 本轮新注释` 证明服务的是这一版**，再看任何读数——这是 §11 那条"clean 构建"闸门在前端侧的等价物。

**回归**：`mvn -o -Dmaven.repo.local=D:/maven clean package` 两次 **283 绿 / 0 failure / 0 error / 0 skipped**（第 8 条改完一次、第 9 条改完一次）；语料基线在 §11.1 口径下逐项相等（15 文件 / 14 输出表 / 2369 原始列边 / 2327 图列边 / 6 表边 / 12 带列表 / maxLayer 1 / 环 0 / 四类泄漏探针 0 / 0 WARN）——九处全在样式层与前端渲染层，Java 侧只删了四个零引用访问器。vendored 依赖仍只有 cytoscape，`/api/overview` 仍是 `D:\project\flinkLearn\sql`。

### 8.15 第四轮：RelationRows 并进字段图 + 开场讲故事 + 定位搜索（2026-09-26）

**要求**（用户原话）："**大整改，大调整，要最优解的开发**"。背景：先把 SQLFlow 线上版（sqlflow.gudusoft.com）逐项摸了一遍——节点配色语义（table 绿 / view 深绿 / RS 红 / 算子灰）、边的语义分级（实线=数据流、虚线=键引用、黑色粗线=当前追踪、汇聚点圆点）、hover 预览 + click 固定、`RelationRows` 伪字段行、chip 工具条、`select to locate` 搜索框。对照后本轮只搬三件（§8.13 已把 hover 染色与选中闭包落掉了），并明确不搬两件。解析层一行没动。

**1. RelationRows：表级关系并进字段图**。原先"表 A、表 B 有表级链路但没有任何字段级血缘"的表对在字段视图里凭空消失——字段边都来自 SELECT 清单，join 键只进 ON/WHERE 的表一个列都不上路径，连盒都不会被建出来。这是字段视图的最大盲区，也是对方虚线边的另一半价值。

- 后端 `SqlFlowChain.expandRelations()`：遍历 `graph.getTableLinks()`，**两端都没有列级通路**（按列级段 BFS 可达性判，不按"有没有直接段"——折过来的间接通路也算已经把这对表连上了，再画表级线就是说两遍）的表对记一条 `RelSegment{from,to,jobId}`；伙伴表一个列都没上路径时**造一个只有关系行的盒**（`ensureBox`，上限 `REL_BOX_CAP=24`——大快照里一张中枢表可能挂几十个这种伙伴，放任下去把 `BOX_CAP` 顶爆、让原本画得好好的图整张拒绝出画）；语句内关系（CTE/子查询/函数盒）不是表级血缘的合法端点，整对放弃。表级段也进 `boxSuccessors()` 参与分层，dim 落在来源层而不是叠在目标身上。行键 `relation::relation`——列标识是 `table.column`，双冒号不可能撞上真列。
- 行预算（`planRows`）：**关系行不进排序池**——它按名字排（"RelationRows" 大写 R）会插在真列中间、把真列挤出 `ROWS_CAP`；预算有富余才追加在盒尾，真列的位置永远不能被它占。行被裁掉时虚线退到接在盒上（`SqlFlowLayout.relEndpoint()`），表级关系不因预算而静默消失。`entity()` 的 `columnCount` 与模型列清单都不收它——它不是列。
- 响应：表级边自带 `kind:"tableRel"` 与 `tableRel:{from,to,jobId,sqlText}`（不走 `relationshipIdMap`——它背后没有字段关系）；`metaInfo.tableRelSegments` 计数，>0 时告警点名"N 对表只有表级关系没有字段级血缘"。
- 前端：`.slf-row.rel` 斜体 + 虚线描边行（`RelationRows` 文案与对方同形，读法在图例与行 tooltip 里交代）；虚线用默认灰 `6 4` 长虚——它表达"发生过关系"的最低表述，不和任何加工方式的真因果撞色。点关系行 → 选中 + 右栏讲清"它不代表某一列，点虚线看语句" + 动作条（列清单按钮对它给"没有可列的字段"，诚实）；点虚线 → 右栏 `renderTableEdge`（端点 + jobId + 语句原文，`hasOwnProperty('sqlText')` 判断——表级 DAG 的老边没这字段，别给它们凭空加一块空 SQL）。
- 用例四条：`tableOnlyPairsGetRelationRows`（dim 盒只有关系行、虚线两端是关系行、`relationshipIdMap` 不收、告警点名）、`columnPathSuppressesRelationRows`（两端都有字段通路时不画、行不出现）、`relationRowsStayOutOfTheModel`（`columnCount` 与模型清单不收它）、`relationRowsAreDeterministic`；`placementIsAGrid`/`geometryCarries` 两条旧行级不变量对 `kind:"relation"` 豁免（modelId 为空、qualifiedName 不带列前缀都是设计内）。

**2. 开场自动讲故事**。冷启动不带 `focus` 进字段视图，原先是一张全灰的图——第一眼没有用法示范。现在 `storyRow()` 在这次请求给的关系里挑"既有人喂它、又往下传"的目标列走 `pickRow()` 同一条路（染色、右栏证据、动作条、hash 全都有）：口径固定否则"开场亮哪行"就成了玄学——AGGREGATE > EXPRESSION > IDENTITY > CONSTANT > POSITIONAL，能继续往下游传的加半档（0.5），同分取模型标识最小的；挑不出来（全图一条关系都没有）就保持全图无高亮。标题跟着换成那一列——图上亮的就是标题说的。

**3. 定位搜索**。画布左上叠一条 `datalist` 搜索框（动作条占了右上角，层 0 从左上起步，所以只留窄条不压第一列）：候选 = 画布上真有的盒（qualifiedName）与画出来的字段，600 行预算内全列。字段命中走 `pickRow()` 同一条路；盒命中 `view.revealBox()`（滚进视野 + `flash` 边框动画）——**不顺手选中**，因为"点表名=以那张表为中心重开"是图上另有约定的手势，搜索不替用户做这个决定。没命中不清输入：datalist 只列真有的值，留着文本用户好改。

**4. 死端点标注**。`/api/graph/column`、`/api/edge/column` 前端已不再调用，按"REST 契约不动"的约定保留，javadoc 补 `@Deprecated` 与去向说明。

**明确不搬的两件**（写下来免得下一轮再争论）：**汇聚点圆点**——对方在多列聚成一列的交点画实心圆，我们的函数盒（SUM 站）本身已经是汇聚的视觉标记，为它引入无语义的小节点破坏"节点=语义"原则；**hover 切换追踪**——对方钉选后悬停别的列会临时切走黑线，§8.13 已把悬停定为"只在原地染色、有选区时不预览"（复验读数 `withSel soft=0`），本轮不推翻。

**实测**（真浏览器 + `sql/` 快照 + 试解析单边 JOIN `INSERT INTO dwd.tgt SELECT s.a FROM ods.src s JOIN ods.dim d ON s.k = d.k`）：

- 试解析·字段：3 盒 4 行 1 条字段关系 + 1 条表级虚线（`e0`=src.a→tgt.a、`e1`=dim.RelationRows→tgt.RelationRows）；dim 盒只有一行 `RelationRows`（斜体虚线），层号 L0 与 src 同层、tgt L1——表级段参与分层生效；右下角告警"1 对表只有表级关系没有字段级血缘（RelationRows 行之间的虚线），点虚线看语句"。
- 点 dim 的关系行 → 右栏"它不代表某一列……点那条虚线看是哪条语句"、动作条标题 `ods.dim · RelationRows`；点 `e1` 虚线 → 右栏"表级边：来源表 ods.dim / 目标表 dwd.tgt / 语句 sd73cd807f366 + SQL 原文（JOIN 高亮）"。
- 快照冷启动 `#view=column&table=kafka_user_app_data`（不带 column）：自动选中 `app_func`（EXPRESSION、有下游）——标题"app_func 的字段链路"、通路亮、动作条与右栏证据齐出；试解析冷启动同理选中 `dwd.tgt.a`。
- 定位搜索输入 `kafka_user_app_data.operation` → `sel=1`、锥内 10 行亮；输入 `ods.src.a` → `sel=1`。
- 回归：hover 预览（无选中时 `soft` 2 行 1 边，`pointerout` 擦干净）、点行钉选、点空白清选区、导出 PNG 全部照旧。
- `mvn -o clean test` **287 绿 / 0 failure / 0 error**（283 + 新增 4）；语料快照全部表对都有字段级通路，`tableRelSegments=0`——RelationRows 只在它该出现的地方出现，不扰动既有出图。


## 9. 里程碑与工作量

| 阶段 | 内容 | 验收 | 估时 |
|---|---|---|---|
| **M0 语法缺口** | ✅ 已完成：§5 三处改造 + 探针验证 `WITH t(a,b)`、`LATERAL VIEW ... t AS c` | `mvn clean test` 全绿，`run-lineage.bat sql` 16 文件/17 输出表/0 纯输入表 | 0.5d |
| **M1 列级解析核心** | ✅ 已完成：`ColumnRef/ColumnEdge` + `QueryScope` 栈 + 三方言共用一份 `ColumnLineageEngine` | 207 测试全绿；语料 2376 条列边，幽灵列/UNRESOLVED/STAR 均 0 | 2d |
| **M2 归一化/图模型** | ✅ 已完成：`ColumnGraphBuilder` 折叠 + `ColumnEdge.targetInternal` 标记、`LocalRelation` 语句命名空间、`LayeredDagBuilder`（迭代 Tarjan+Kahn）、`LineageStore` 快照 | 228 测试全绿（新增 21）：环/自依赖、20000 节点长链、多 CTE 同名、别名解引用、hop 证据；全语料建图 17 节点/2329 列边/0 未折叠伪节点/0 缺层号 | 1d |
| **M3 服务层** | ✅ 已完成：starter-web 2.7.18 + `CorpusScanner`（jobId=文件#序号）+ `ScanReport` 账本 + 9 个端点 + `ApiResponse`/`ApiErrorAdvice` + `spring-boot-maven-plugin`（无需 profile，见 §7.1）。实测细节见 §7.4 | 245 测试全绿（新增 17）；真起 jar 逐端点 `curl` 通过，overview 与 M2 基线逐项一致；0 条折叠告警；无 slf4j 双绑定 | 1d |
| **M4 前端** | ✅ 已完成：vendor 三件套（cytoscape 3.32.1 + dagre 0.8.5 + cytoscape-dagre 2.5.0）+ 三栏 ES module（`main/api/graphTable/graphColumn/detailPanel/badges`）+ 表级 DAG 与字段链路两种图 + 右栏逐跳证据（hops/derivation/confidence/SQL 高亮/parseError 红条）+ PNG 与子图 JSON 导出 + hash 定位。实测细节见 §8.4。**（dagre 双包已在 §8.7 下线，vendored 只剩 cytoscape；试解析入图见 §8.8）** | `mvn -o clean test` **246 绿**；jar 内 static 21 文件、`GET /` 200；真浏览器（browser-use）表级/字段级金路径 + 300 节点超限 + 缺列 400 + parseError 红条 + 星号/未解析边界逐项通过，console 零 error；揪出 7 个前端缺陷与 1 处"把解析缺口说成链路起点"的语义谎言 | 2d |
| **M5 语料回归** | ✅ 已完成：子查询身份（别名只做查找键，`#subN` 才是身份）+ 无参关键字函数与 lambda 形参不再当列（§6）。实测细节与逐条定性见 §11.1 | 涉密语料：折叠告警 62→**0**、未折叠伪节点 0、幽灵列 0、UNRESOLVED 7→**2**（两条都是 CASE ELSE 的未限定列，两张未知表 ⇒ 按"宁缺勿假"口径保留，`ambiguousUnqualifiedColumnIsNotForcedOntoATable` 已锁这个语义）；`sql/` 基线：2376 原始列边不变，图列边 2329→**2327**（4 处 `current_timestamp` 假来源），0 告警。`mvn -o clean test` **252 绿** | 1d |
| **M6 文档与残留收敛** | ✅ 已完成（见 §2.0.4、§2.0.5）：README 重写、33 份历史 md 删除、死脚本清理、未跟踪垃圾清理，`superior-sql-parser-temp/` 按你确认删除（代价与教训记在 §2.0.4 末段），与血缘无关的编译日志/一次性脚本/MySQL 建表脚本再删一批。**无遗留待办**：`column_lineage` DDL 随"不落库"结论一起取消（§10） | 顶层文档不再把读者引向 Calcite / superior jar / Flink 作业路线；`sql/` 语料 0 解析错误 | 0d |

| **M7 字段级血缘重做**（§8.10 → §8.11，2026-09-22/23） | ✅ 已完成：先改成"一表一盒、一行一字段、线连字段行"，再把**布局整个搬进服务端**（新包 `web/sqlflow`：数据模型 + 关系 + 坐标 + 标签度量一次给全），前端换成 `sqlflowModel/sqlflowView/sqlflowPop` 三个模块 + **跟随式动作浮层**（`只看这一列` / `列出字段` / `复位`）。新端点 `GET/POST /api/sqlflow/graph`，旧 `/api/graph/column`、`/api/edge/column` 契约保留但前端不再打 | `mvn -o clean package` **281 绿**（§8.10 的 259 + `SqlFlowStatementTest` + `SqlFlowAssemblerTest`）；`/api/overview` 与 §11.1 基线逐项一致；真浏览器逐条读数见 §8.11（含裁剪后盒高 41.96875/169.96875 与盒底 `rgba(0,0,0,0)` 的像素对照），console **0 error**；揪出两处静默失效（不存在的方法被 `guard` 吞、空集合写坐标是 no-op）——正是它们证明了"能单测的判定该留在 Java"；§8.12 回归轮又揪出五处（盒顶表名静默降级、换视图残留裁剪句柄致白屏、跨表焦点参数错位、"不画"时标题说谎、自环不告警），**283 绿** | 1.5d |

| **M8 字段级渲染层重做**（§8.13，2026-09-25） | ✅ 已完成：**像素退回浏览器**——字段视图改成 HTML 盒 + 一层 SVG 边，彻底不碰 cytoscape；`/api/sqlflow/graph` 只回模型 + 证据 + **网格**（`layer`/`slot`/行标识），`SqlFlowLayout` 删掉整套尺寸与标签常量；标识换成 `b0 / b0_c0 / e0`；跟随式浮层换成**钉在右上角、可拖动的动作条**，交互改成"悬停只在原地染色、`只看这一列` 才裁剪"；`coneOf()` 明确用祖先∪后代闭包而不是无向可达；导出 PNG 改成手写 canvas 重画 | `mvn -o clean package` **283 绿**（新增反向断言 `placementIsAGridAndCarriesNoPixels`：后端多给一个 `x` 就该红）；`run-lineage.bat sql` 与 §11.1/§14 基线**逐项一致**，解析层零改动；真浏览器（`kimi-webbridge`，3432×1274）逐条读数见 §8.13——深链裸名/全名、悬停不动位置、裁剪与复位、缩放读数 `0.75/0.9/1.0`、导出图 1048×728 含 614 种颜色、试解析四站链路，console **0 error**；揪出两处（裸列名深链静默不选中、空字段视图导出说谎的 PNG）。**内置 browser-use 视口 0×0 这一事实同时被记进闸门口径**：DOM 断言在它里面全绿也不代表渲染对 | 1.5d |

合计约 9 人日（§2.0 的结构与文档清理已完成，不计入）；M7、M8 是落地后按使用者反馈重做的三轮（§8.10→8.11、§8.12、§8.13），另计 3d。M1 与 M3 可并行（解析层不依赖 Spring）。

## 10. 持久化：确认不做（结论与 DDL 只留作参考）

2026-09-21 定稿：**血缘结果不落库**。WebUI 的数据来源就是启动时（或 `POST /api/scan` 时）对目录的一次扫描，进程内一份不可变快照，没有数据库、没有迁移脚本、没有外部中间件。好处不只是"少一个依赖"：改一处语法 → `mvn -o clean package` → 重扫目录就能看到图上变化，整个回路只有编译。

仓库里的 `sql/init_lineage_db.sql`（`column_lineage` 等三张 MySQL 表）已随 §2.0.5 删除。下面这份 DDL 不再排期，只保留作"若真要落库时的正确形状"——原表缺的正是幂等与溯源所需的列：

```sql
CREATE TABLE IF NOT EXISTS column_lineage (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    job_id VARCHAR(128) NOT NULL COMMENT '文件+语句标识，如 job_03.sql#7',
    source_table VARCHAR(255) NOT NULL,
    source_column VARCHAR(255) NOT NULL,
    target_table VARCHAR(255) NOT NULL,
    target_column VARCHAR(255) NOT NULL,
    derivation VARCHAR(32) NOT NULL COMMENT 'IDENTITY/EXPRESSION/AGGREGATE/CONSTANT/POSITIONAL/STAR/UNRESOLVED',
    transform TEXT COMMENT '归一化表达式',
    ordinal INT COMMENT 'SELECT 列表位置',
    engine VARCHAR(16) COMMENT 'FLINK/SPARK/PRESTO',
    confidence DECIMAL(4,3),
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_col_lineage (job_id, target_table, target_column, source_table, source_column, derivation),
    INDEX idx_target (target_table, target_column),
    INDEX idx_source (source_table, source_column)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='字段血缘表';
```

要点：`UNIQUE KEY` 支持幂等重扫；`transform`/`ordinal`/`engine`/`confidence` 是 UI 展示与质量过滤的必需列，原 DDL 全缺。表级 `table_lineage` 同样缺 `confidence/has_cte/engine/parse_error`，届时一并补。

## 11. 测试与质量闸门

现有 4 条血缘泄漏 oracle 全部保留，**新增第 5、6 条针对列级**：

1. 源表不得出现无点号裸名（CTE/列名泄漏）。
2. 源表首段不得命中本语句 FROM/JOIN 别名或 CTE 名。
3. 声明的 CTE 名不得出现在源表里。
4. "幽灵表"：源表在归一化语句文本里字面不存在。
5. **列级字面性**：每条 `ColumnEdge` 两端的表名与列名，去反引号+小写后必须能在 `originalSql` 文本中字面找到；找不到即"编造列"，测试红。
6. **列级作用域**：`WITH a AS (...), b AS (SELECT ... FROM a)` 里，`a` 的内层别名不得出现在 `b` 的绑定结果里；断言 CTE 名与子查询别名在跨层用例中互不可见。

测试矩阵：

- 单测（新增 `ColumnLineageFlinkTest` / `ColumnLineageSparkTest` / `ColumnLineagePrestoTest` / `ColumnLineageChainTest`，合计 ≥45 例）：直通、别名、限定符、CASE、CONCAT 多来源、聚合、开窗、子查询多跳、CTE 链、`INSERT (...)` 列清单位置对齐、`SELECT *` 与 `t.*` 的 STAR、绑不上的 UNRESOLVED、Spark LATERAL VIEW 产出列、Presto UNNEST 列别名、Flink TVF/时态表。
- 语料：`sql/` 16 文件跑列级 JSON 导出工具（新建 `SqlDirColumnLineageTool`，与 `SqlDirLineageTool.java:24-110` 并列**不改它**），人工巡检 TOP 未解析边。
- Web（M3 已按此执行）：`GraphAssemblerTest`/`CorpusScannerTest` 钉住裁剪与扫描器契约，9 个端点全部真起 jar 用 `curl` 冒烟（见 §7.4）。`@WebMvcTest`/`@SpringBootTest` 都依赖 `spring-boot-test`+`spring-test`，**离线仓库一个版本都没有**，不要计划走这条路。
- 前端：**真实浏览器点一遍**（M4 验收），不做"我以为能渲染"的声明。
- 前端静态完整性（`web/StaticAssetIntegrityTest`，4 例）：没有构建步骤就没有编译器兜底，所以在 JUnit 里扫 `static/` 下的 JS——块注释逐字符配对（防 §8.8 那次让整站白屏的 `*/`）、`import './x.js'` 目标存在、`index.html` 的 `src`/`href` 资源存在、`getElementById` 的 id 在 DOM 里存在。四类失效都是"Java 测试全绿、页面全白"，只能这样钉住。
- 构建纪律（既有记忆约束）：报"通过"之前必 `mvn clean test`，不信陈旧 `target/`。
- 涉密语料纪律：任何来自 `D:\ai coding\...` 的 SQL 只在 git 忽略的 `tmpdb/` 复现，**不复制进仓库、不写进用例、不提交**；提交前删除 `tmpdb/`（本分支截至 2026-09-21 已清空，M5 复现时重新拷贝）。

### 11.1 M5 涉密语料巡检实测（2026-09-21）

语料：16 个文件 / 528K（某绩效监控主题的 Spark 作业目录），拷进 git 忽略的 `tmpdb/`，`SqlDirLineageTool` 带 `--columns` 跑，输出一律落到仓库外（`D:/tmpdb-audit*.txt`）。**修完即删 `tmpdb/`；本节按 §14 的涉密纪律只留同构形状，表名/列名一律脱敏，不落原文。**

修掉两个真缺口：

1. **子查询把别名当身份**（`ColumnLineageEngine`）。`) t1 … ) t1` 这种逐层套同名别名在真实语料里成批出现，两层被并成同一个节点后折叠自判成环、`log.warn` 放弃这一跳——涉密语料 **62 次折叠告警**，字段链路断在半截别名上。改为别名只做查找键、身份一律 `nextPseudo("sub")`。
2. **写法像列、其实不是列**。两种形状：`date_format(current_timestamp, 'yyyyMMddHHmmss') as 写入时间列`（无参关键字函数）与 `filter(split(某串列, '-'), x -> x like '%关键词%')`（lambda 形参）。三套 grammar 都没为前者立 token，后者本来也只是普通 `uid`，于是都进了来源清单。现在 `NILADIC_FUNCTIONS` + `lambdaScopes`（体内遮蔽同名表别名）在 `collect` 里一次摘掉。

逐条定性（7 条 UNRESOLVED → 2 条）：

- 4 条写入时间列 + 1 条高阶函数计数列：**解析缺口**，已修。前者变成 `CONSTANT`、零来源；后者只剩它真正读的那一列。
- 2 条同一个 CTE 里的 `before/after_*_flag` 对偶列：**真实语义，按约定不猜**。CASE 的 `ELSE` 分支未加限定，而该作用域里有两张都没报出列清单的物理表——没有列字典就是歧义，`ambiguousUnqualifiedColumnIsNotForcedOntoATable` 早就把这个口径钉成测试了。
- 1 个环上表：**真实自依赖**（排班日志类作业，`INSERT OVERWRITE` 的输入清单里就有它自己），不是解析产物；`LayeredDagBuilder` 按 SCC 同层处理并计入 `cyclic`，UI 用红色回边显示。

修复效果的口径：

- 涉密语料：折叠告警 62→0，未折叠伪节点 0，幽灵列 0，UNRESOLVED 7→2，节点 269（语句内关系 185 / 物理表 84）不变，图列边 4110→3951（−159：−153 来自套别名折叠恢复，链路合成一跳后不再经由被并节点的重复边；−6 来自假来源，按 `from>to>derivation` 去重后合并）。**原始列边 4012 一条不差**——没有丢血缘，只是不再伪造。
- `sql/` 公开语料：2376 原始列边不变，图列边 2329→2327（`current_timestamp` 假来源 4 处），0 告警、0 泄漏、0 未解析、17 节点/6 表边/maxLayer 1 全不变。
- `mvn -o clean test` **252 绿**（M4 收尾 246 + 套别名 1 + Spark 无参/lambda/形参遮蔽 3 + Presto lambda/括号形式 2）。

同一份涉密语料在 `lineage-only-webui` 分支上的复测（2026-09-21 收尾）：16 文件 / 38 语句 / **0 解析错误**，269 节点（185 语句内关系 + 84 物理表）、原始列边 4012、图列边 3951、表边 112、maxLayer 5、UNRESOLVED 2、环上表 1，四类泄漏探针与逐文件双向引用巡检（"文本里引用的表" ↔ "报告输出的表"）**丢 0 / 多 0**——与上面逐项相等，删 Flink 代码与后续 Web 改动没动到血缘。

**只有大语料才暴露的口径缺陷**：顶栏当时显示 `表 84（261 张有字段）`，261 > 84 是因为 `LineageGraph.tableCountWithColumns()` 遍历全部节点，把 185 个语句内关系一起数了进来，而 `tableCount` 只数物理表，两个数不同口径。现改为 `isPhysical()` 且带列才计数，`columnBearingCountSkipsStatementLocalRelations` 钉住（去掉 `isPhysical()` 守卫该用例从 2 变 4，不是空断言）。合成小语料真浏览器实测：`表 5（5 张有字段）· 中间关系 4`，同一份数据修前会显示 `9 张有字段`。

巡检口径的两条教训（比数字更有价值）：

- **"未折叠的伪节点"必须是 0 才算过关**这条此前是假绿：它按节点 id 里有没有 `#` 判定，而别名登记的子查询根本没有 `#`，于是报 0 的同时 62 次折叠失败无人看见。
- **UNRESOLVED 账本只看得见"绑不上"的**：`current_timestamp` 共出现 31 次，只有 4 次没绑上进了账本，另外 **27 次被错绑到真实表/CTE 上**（其中 23 次挂到同一张 CTE），是编造出来的来源列，账本、幽灵列巡检、UI 全都不报警。补的做法是对全语料的**来源列名多重集合**做修复前后 diff——本次 diff 只少掉 `current_timestamp` 与 `x` 两类共 30 个引用，其余一列未动，才敢说"没顺手删掉真血缘"。复现脚本口径：`grep -ao "column=[a-z0-9_]*" 审计报告 | sort | uniq -c` 两份一比即可，无需列字典。

## 12. 风险与权衡

| 风险 | 影响 | 处置 |
|---|---|---|
| 无列字典 → `SELECT *` 无法展开 | 星号边的下游字段来源不全 | 已按决策 ① 明确记为 `STAR` 边并打徽标；预留 `TableSchemaProvider` 接口但**不实现**，后续可接 information_schema 而不改契约 |
| 与 Flink/hive-exec 的依赖冲突 | ~~已消除~~ | §2.0 已把 Flink/Hive 依赖树整体删掉，pom 只剩 8 项。剩余唯一冲突点：starter-web 默认带 logback，与本项目 log4j2 绑定二选一 → 按 §7.1 排 `spring-boot-starter-logging` |
| 顶层文档仍在描述已删除的 Flink 路线，误导后来者 | 有人照 `README.md`/旧方案再引 Calcite | M6 收敛；本方案 §0 已声明两份旧列级文档作废 |
| 真实语料仍有语法错误恢复（历史上从 9→1 后仍有零星） | 部分列边缺失 | `parseError` 与 `confidence=0.2` 必须在 UI 显式呈现；`/api/issues` 专门列出来，绝不静默 |
| 匿名表达式列命名不确定（障碍 #4） | 目标列名可能与真实表结构不符 | `POSITIONAL`/`UNNAMED` 显式标注，永不伪装成确定结果 |
| Spark `lateralView` 标签化引入 ANTLR 歧义警告 | 解析行为漂移 | M0 用探针先验证，歧义则改显式子规则；grammar 卫生审计（`KW_` 双向 diff + 死规则）保持 0/0 |
| 大语料下字段节点数远超表（万级列） | 前端卡死 | depth 默认 3、后端裁剪而非全量下发；§8.11 之后这套闸门整个搬进了 `SqlFlowAssembler`：行预算 `ROW_BUDGET=600` 按盒数摊薄、模型登记 `MODEL_ROWS_CAP=200`、超 `BOX_CAP=300` 直接 `drawn=false` 并回 `warning`，前端拿不到"整表 1568 节点"这种 payload，也就没有画不画得动的分支逻辑 |
| vendored JS ~500KB 进 git | 仓库体积 | 明确一次提交锁定版本 + `vendor/README.md` 记来源，换取离线可运行 |
| 无前端构建链，手写 JS | 复杂度上限 | 只做"图 + 面板"两件事；一旦需要路由/状态管理，另立方案而不是偷偷加 webpack |
| **JS 侧的静默失效没有闸门兜** | 缺陷带着走完 CI 上线（§8.11 实测两处：`cy.renderedPosition(node)` 这个不存在的方法被 `guard` 转成一行告警、`getElementById(undefined)` 拿到空集合后写坐标是 no-op；§8.12 再补五处、§8.13 再补两处，其中四处的表现是"点了没反应"而不是报错） | `StaticAssetIntegrityTest` 只钉得住文本层面的事（注释闭合 / import 目标 / DOM id），管不到运行期。**判定进 Java、像素留浏览器**（§8.13 的分界线）：层号、同层序号、哪些行进盒子、预算、自环、告警全部在 Java 里断言，尺寸与字号交给唯一真正排版它的那一方；剩下的交互只能靠真起 jar + 真浏览器逐条读数，且要记住"没抛异常"在 cytoscape 里不等于"写进去了"。载荷里凡是前端要拿来当"地址"用的字段（关系标识、模型标识、DOM `id`）**必须在 Java 测试里断言它等于请求参数能认的那个名字**——§8.12 那处表名手势就是这么静默降级的；而"后端不许再塞像素"这件事，靠一条**反向断言**（`placementIsAGridAndCarriesNoPixels`）钉，不靠文档 |

## 13. 明确不做（v1 范围外）

- 不做 `SELECT *` 列展开、不接 information_schema。
- 不引 Calcite / Flink Planner / `RelMetadataQuery`（与既有约束一致）。
- 不做 MySQL 持久化读写，也不挂数据库依赖（§10 已定稿为"不落库"，那份 DDL 只是形状参考）。
- 不做权限/多租户/审计、不做血缘编辑与人工修正回写。
- 不做前端框架（React/Vue）、不做 npm 构建步骤进入 Maven。
- 不动三套 grammar 的表级规则（已稳定），除 §5 三处。
- **不拆多模块**：血缘与非血缘零交叉引用，且 Maven 要求聚合器必须 `pom` 打包（§2.0.1 实测），拆模块的收益可用"直接删掉 Flink 侧"拿到，成本却是搬家 61 个类和 kerberos 配置。血缘工程保持单模块。
- 不在本分支保留 Flink 作业/UDF 代码——它们在 `master`，需要时从那里取。

## 14. 验收标准（可打勾）

- [x] 1. **基线（M6 收尾后）**：`mvn -o clean package` **255/255**（M6 收尾 254 + 「有字段的表」只数物理表 1，见 §11.1；§8.7/§8.8 这轮再加 4 例静态资产检查 = **259**）+ boot jar 可执行、`run-lineage.bat sql` **15 文件 / 14 输出表 / 0 WARN**（原为 16/17/3，那 3 条 MySQL 建表 DDL 已随 §2.0.5 删出语料）、图列边 2327、0 折叠告警、`grep -r "org.apache.flink" src` 为空。功能落地后用例数只增不减，且这几条不被破坏。
- [x] 2. 起服务（`java -jar target/flinkLearn-0.0.1-SNAPSHOT.jar`，或 `mvn -o spring-boot:run`）后浏览器打开 `http://localhost:8080`，能看到按层排布的表级 DAG。→ 初版由 dagre 实跑（§8.4），但"看得到"不等于"看得清"：稀疏语料下它把图摊成 8:1、fit 到 0.39 缩放，实测被判定为"表血缘没有显示"。现已改为按 `layer` 自算，层序单调、fit 1.34，数字见 §8.7。画布像素也已核到（屏外渲染取 `cy.png()` 再数色，方法见 §8.6）；剩下"人眼觉得好不好看"这一条只能由使用者判。
- [x] 3. 点击任一有列级信息的表 → 字段链路图渲染出 2 跳以上链路（中间节点在 hops 里显形）：`kafka_user_app_data.eventbodylist → [ck/user_app_data_ck_dml.sql#1]#unnest1.app_name → user_app_data.app_name`。
- [x] 4. 选中字段边 → 右栏显示逐跳链路、derivation/confidence、原始 SQL 片段高亮，三者信息一致（`85%（EXPRESSION）` 对 `confidence:0.85`，3 处 `<mark>` 对两端列名）。
- [x] 5. `SELECT *` 边显示 `STAR · 星号未展开` 徽标 + 图例色块 + 30% 置信度；`UNRESOLVED` 边在 `/api/issues`、左栏质量过滤、字段清单徽标与右栏红条四处都可见——第 3、4 处是本轮补验时才修出来的，见 §8.4。
- [x] 6. 涉密语料巡检（M5）：两份语料（`sql/` 与临时语料）列级 6 条 oracle 泄漏计数均为 0，折叠告警 0，剩下 2 条 UNRESOLVED 已定性为"没有列字典就是歧义、按约定不猜"；临时目录已删除，文档与用例里的表名列名一律脱敏（实测见 §11.1）。
- [x] 7. 本方案入库为 `outputs/` 下唯一现行设计文档；两份旧 Calcite 路线文档已删除，作废理由保留在 §0。
- [x] 8. 空快照必须自己说清是哪一种：**没在扫 / 扫失败（目录指错）/ 扫到 0 个 .sql / 跳过启动扫描**四态在页面上可区分，且 `running` 态会在 60s 内轮询自愈成图（`/api/overview` 的 `scanPhase`/`scanError` + 顶部横幅，实测见 §8.5）。
- [x] 9. 深色仪器台（§8.6）：样式与交互层改完，**REST 契约与解析层零改动**（`mvn -o clean test` 255 绿）；调色板一处定义、CSS 与画布同源，图例由 `EDGE_STYLE` 生成——本视图里出现的每种边色都在画布上数得到像素、没出现的为 0px，选中态像素 0→1256→0 可打可消。
- [x] 10. 布局按 `layer`（§8.7）：`sql/` 语料的表级图 fit 后缩放 ≥1、层号与图上左右次序单调一致、图例不再有说谎项；dagre 两个包与其 LICENSE 已从 `vendor/` 删除，`index.html` 只剩一处 script 依赖。**不得**再引回第三方布局引擎。
- [x] 11. 试解析入图（§8.8）：页面贴 SQL → 中栏画出血缘，点表出字段清单、点字段边出逐跳证据与原文高亮；临时视图不写 hash、点左栏即退出、重扫即清空；`/api/parse` 之外零新增端点，快照四态与 §7.3 契约不变。
- [x] 12. 图上读得出字（§8.9）：每个**可见**节点都数得到标签像素（对照组 `label:''` 只剩描边），屏上字高 ≥10px；画布放不下时放大到可读下限并左上对齐，右下角同时说清"画布 WxH 放不下 N 个节点"，且报的尺寸与实测盒子一致。图头/图例/告警三条都不许改变画布高度，`fit()` 前必须 `cy.resize()` 刷新 cytoscape 的容器尺寸缓存。
- [x] 13. 字段级血缘读得出因果（§8.10）：一表一盒、一行一字段、线连字段行不连盒子；点字段行只剩这一列的通路；`hops` 长 ≥3 的边必须把中间伪列摊成虚线盒（层号插值落在上下游之间），点任意一段仍打开原边的逐跳证据；整表 1568 节点那种量级**不再拒画**，而是每盒 ≤14 行 + "还有 N 列未画" + "M 段链路因两端列未画而暂不显示"，且 N/M 与下发数字对得平（1540+28=1568 列、4+782=786 段）。告警条的"放不下"与"N 列未画"两半分开存，容器变大只准重算前一半。
- [x] 14. 后点的必须后画（§8.10）：凡 `await` 取数之后还要往画布、右栏或 `state` 写的渲染路径，进门领一个意图序号、回来先比一次，过期就整段收手。人为拖慢大表那一条响应、连点三张表，最终画布 / 右栏 / hash 必须都停在最后一次点击。
- [x] 15. 字段级的布局与展示一次给全（§8.11）：`GET/POST /api/sqlflow/graph` 同形状回"数据模型 + 坐标 + 标签度量"，`SqlFlow*Test` 在 Java 侧断言坐标，前端 `sqlflowView.js` 不再有一份尺寸常量（行距/标题带/盒底留白从响应量回）；点字段行弹跟随式浮层，三按钮逐个实测：`只看这一列` 后盒高必须等于 `BOX_PAD + 留下行数 × ROW_H`（实测 1 行 41.96875 / 9 行 169.96875）且标题带跟着盒走、盒底以下像素为 `rgba(0,0,0,0)`，`列出字段` 出「字段数 27 / 画进盒子 14」，`复位` 回到 57.97/169.97/249.97。`mvn -o clean package` **281 绿**、console **0 error**、`/api/overview` 与 §11.1 基线逐项一致。**（本条的"坐标与标签度量进响应"已被 §8.13 退回：那些盒高读数随像素口径一起作废，留下的只有"语义结论必须在 Java 侧可断言"这一条要求。）**
- [x] 16. 字段血缘的四个手势与两处记账都得自己说清（§8.12）：点盒顶表名**必须换 hash 并重画**（`data.table` 来自几何里的 `qualifiedName`，Java 侧断言它是 `metaInfo.ranks` 的键）；换视图必须把字段级的浮层与裁剪句柄一起清掉，且旧裁剪对不上新图时**一个元素都不许多隐藏**（`crop()` 先判 `nonempty()`）；选中别人家的列后改深度，请求得把完整标识交给 `focus=` 而不是 `column=`；`drawn=false` 与自环两处不许静默——标题改口"没画"、告警报"N 段两端落在同一个盒子（自环），不画线"。跨表那次的实测链路是 `kafka_user_app_data.operation → user_app_data.mcu_software_ver`（`e17`，非 synthetic），25 行几何与模型标识 **0 处错位**。`mvn -o clean package` **283 绿**、console **0 error**、语料基线逐项一致。
- [x] 17. 字段图的像素只活在一个地方（§8.13）：`/api/sqlflow/graph` 回**网格不给像素**（盒 `{id,name,qualifiedName,type,local,layer,slot,modelId,columns}`、边 `{id,sourceId,targetId,synthetic}`），Java 里由**反向断言**钉住——盒不许有 `x/y/width/height`、格子不许有 `x/y`；渲染是 DOM 盒 + 一层 SVG 边，**字段视图不再创建 cytoscape 元素**，缩放是 `scale()` 放大整张纸所以字号恒定（fit 下限 0.8×12px）；量法必须两趟（成批读 → 成批写 → 再画线）；`只看这一列` 的染色必须是祖先∪后代闭包而不是无向可达；动作条钉右上角、可拖动，`×` 必须连染色一起撤（`clearRowSelection`）而不是只藏条子；空字段视图导出 PNG 必须告警而不是回落到表级图。`SqlFlowAssemblerTest` 19 条、总数 **283 绿**，语料基线逐项一致，真浏览器（有视口的那个）逐条读数见 §8.13。
- [x] 18. 尺寸与状态改完必须自己说清（§8.14）：凡新增带 `display` 的浮层，`[hidden]{display:none}` 得跟着补一条，验收读 computed `display` 而不是 `el.hidden`；拖拽类监听三条退出路径（`pointerup/pointercancel/失去捕获`）都得解；容器变窄时**手动倍率下的居中距必须重算**（实测 `z` 保持 90%、`ml 987px→0`），且此时不许由看不见的表级图来报"放不下 N 个节点"；`×` 在裁剪态必须连裁剪一起撤；深链要的列没画进盒子时**必须点名**，不许只留一句"N 列未画"。总数 **283 绿**、语料基线逐项一致、真浏览器 console **0 error**；复验前先证明服务的是本轮的包（`curl /js/main.js | grep 新注释`），resize 类断言前先证明标签页没被冻结（rAF 帧数 > 0）。
