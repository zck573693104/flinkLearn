# 字段级血缘 + WebUI 技术方案

> 分支：`lineage-only-webui`（本方案实施分支，血缘专用、无 Flink 依赖）
> 派生自：`feat-column-lineage-webui` @ `546ae69`
> 日期：2026-09-21
> 状态：**结构清理已完成，功能设计待评审**（本方案批准前不动解析/UI 代码）

## 0. 文档定位

本方案是字段级（列级）血缘 + WebUI 展示的唯一现行设计。

历史上两份 Calcite 路线的文档（`outputs/列级血缘实现方案.md`、`outputs/列级血缘实现总结.md`）**已随 §2.0 的文档清理删除**，需要时可从 `master` 取回。它们作废的理由记录在这里，避免后来者重走：

| 旧文档 | 作废原因 |
|---|---|
| `列级血缘实现方案.md` | 基于 Calcite `RelMetadataQuery.getColumnOrigins()`（Route B），依赖 Flink Planner 的 `RelNode`。该路线在当前代码里是占位符且已被明确移除（提交 `8df7442`），与"血缘关系只依赖 antlr4 解析"的约束冲突 |
| `列级血缘实现总结.md` | 描述的是上述 Calcite 方案的"已完成"状态，与现状不符 |

当前仓库**没有任何列级血缘代码**（已核查：`parser/model/` 与 `model/` 两套 DTO 全部是表级字段，无 `Column*` 结构）。本方案是新建，不是改造遗留物。

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
  api/LineageApiController.java  下列 9 个端点
  api/ApiResponse.java           {success,data,message} 信封
  api/ApiErrorAdvice.java        @RestControllerAdvice：IAE/IO → 400，其余 → 500
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
| GET | `/api/graph/column` | 字段级链路子图 | `table`、`column`（可空=整表）、`depth`（默认 3） |
| GET | `/api/edge/column` | 单条边的证据 | `from`、`to` → `[{jobId,ordinal,engine,derivation,transform,hops[],sqlText,parseError,confidence}]` |
| GET | `/api/issues` | 质量视图 | `type=all\|parseError\|unresolved\|star\|noLineage\|orphan` |
| POST | `/api/parse` | 贴 SQL 试解析（只读、不落库） | `{sql}` → `{statements,graph,columnEdges}` |
| POST | `/api/scan` | 重扫目录，原子换快照 | `{dir?}` → 新统计 |

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
  index.html                     三栏骨架 + 顶栏统计/工具（深度、语料目录、重扫、两个导出）+ 空快照告警条 + 图头（标题/告警位/方向下拉）
  css/app.css                    单文件；CSS 变量 + derivation 徽标配色 + mark 高亮色
  js/api.js         9 端点封装，统一解 {success,data,message} 信封（非 JSON 或 success=false 直接抛）
  js/badges.js      derivation 中文提示、层级配色、置信度分档
  js/graph.js       挂载与样式表、dagre 布局 + breadthfirst 降级、节点尺寸、NODE_CAP
  js/graphTable.js  表级 DAG（LR）
  js/graphColumn.js 字段链路图（TB，derivation → 线型/颜色，中间关系灰底虚线）
  js/detailPanel.js 右栏：表详情 / 字段来源 / 边证据 / 试解析结果
  js/main.js        选区状态机 + 全部事件绑定 + URL hash + 导出
  vendor/cytoscape.min.js 3.32.1(431508B)  dagre.min.js 0.8.5(283803B)
          cytoscape-dagre.js 2.5.0(12665B) + 3 个 LICENSE + README.md（版本、sha256、加载顺序）
```

获取方式照原计划：`npm pack` 取 `dist/*.min.js` 拷进 `vendor/`，README 记版本与 sha256 便于审计；**无 CDN、无 webjars、无打包器、无 TS、无框架**，`<script type="module" src="/js/main.js">` 直接跑。

`vendor/README.md` 里写死的加载顺序是硬约束：dagre → cytoscape → cytoscape-dagre。UMD 包自己会 `register(cytoscape)` 注册 `dagre` 布局，**不要再手写** `cytoscape('register','layout','dagre',…)`（那是错误 API 用法）；`graph.js#runLayout` 用 try/catch 兜底，布局注册失败就降级 `breadthfirst`，图不会消失。

### 8.2 三栏交互

- **左栏**三个 tab：表 / 质量 / 试解析。
  - 表：搜索框 250ms 防抖 + 层过滤 `<select>`（选项按 `/api/overview.maxLayer` 生成——**图的 rank 是唯一可信的分层来源，不按 ODS/DWD 前缀猜**，前缀识别留在 §13 范围外）+ "只看有质量问题"（对应 `onlyUnresolved`）+ 列表项 `层 n · ↑u ↓d · c 字段`。
  - 质量：5 组"计数 + 明细"（parseError SQL / UNRESOLVED 边 / STAR 边 / 没出血缘的文件 / 孤岛表），空组绿底，首次打开才发请求。
  - 试解析：textarea → `POST /api/parse` → 逐语句卡片（输入表、插入模式、CTE/WINDOW/TEMPORAL 标记、未折叠字段边 + derivation 徽标、原文）。只读，不动快照。
- **中栏**两个 tab 共享同一份选区 `state.{view,table,column}`。
  - 表级 DAG：dagre LR，节点底色 = rank 层色；点表节点 = 以它为中心重裁（root + direction + depth）并让右栏出字段清单；点边出"来源表/目标表/jobId"；点空白清选区回全量。
  - 字段链路：dagre TB，节点是 `table.column`，被折掉的中间关系（CTE/子查询/展开）画成灰底虚线框，边按 derivation 上色上型（UNRESOLVED 红虚线、STAR 橙点线、AGGREGATE 加粗金…），hover 出 derivation 标签，当前定位列打 `hot`。
  - 性能闸门 `NODE_CAP=300`：超限**直接不画**，只回告警 + 保留右栏（语料里 783/787 列的表整表展开是 1568 节点，画出来必卡）。右栏字段清单另外截到 80 行。
- **与原计划的三处实现期取舍**，记下来免得被当成漏做：
  1. 不做右键上下文菜单。"看字段血缘 / 只看下游 / 展开整表字段" 改成「点节点即重裁」+ 右栏「铺开整表字段链路」按钮 + 图头方向下拉——少一层瞬时交互状态，能力一条没少。
  2. 字段链路不做 swimlane 泳道。自算 y 坐标的收益抵不过成本，改 dagre TB + 层色，纵向排布照样看得清链路。
  3. 边不按 `process_type` 上色，按 `derivation` 上色。字段级面板要回答的是"这条边多可信"，`process_type` 在表级边的证据里给。
- 无框架、无构建，全部 ES module 相对导入。

### 8.3 证据块、导出与定位

一条边的证据块从上到下（对应原计划右栏 5 条）：

1. `to ← from` + derivation 徽标（STAR/UNRESOLVED 带 tooltip 说明 v1 约定）；
2. parseError 红条（有才出）："此边来自语法错误恢复的部分结果，不可信"；
3. hops 逐跳链（**>2 跳才画**，只有两端时没有信息量）；
4. `加工方式 / 置信度 / 表达式 / 引擎 / 语句 jobId / SELECT 第几位`；
5. SQL 原文 + 来源、目标列高亮。

- 高亮实现：拿 `from`/`to` 的**末段列名**在原文里正则匹配（`escapeRegExp` + `\b`），目标 `mark.target` 蓝底、来源黄底；来源列与目标列同名时不区分两处出现（v1 不做词法定位）。全程 `createElement`/`createTextNode` 拼 DOM，**绝不用 innerHTML 塞 SQL**——语料文本是不可信输入。
- 导出：PNG 用 `cy.png({full:true,scale:2,bg:'#ffffff'})` + `<a download>`（原计划写的 `toBlob` 不是 cytoscape 的 API）；子图 JSON 把 `{view,table,column,graph}` 打成 Blob 下载，`graph` 就是最近一次服务端下发的子图原文，可离线复核。
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

## 9. 里程碑与工作量

| 阶段 | 内容 | 验收 | 估时 |
|---|---|---|---|
| **M0 语法缺口** | ✅ 已完成：§5 三处改造 + 探针验证 `WITH t(a,b)`、`LATERAL VIEW ... t AS c` | `mvn clean test` 全绿，`run-lineage.bat sql` 16 文件/17 输出表/0 纯输入表 | 0.5d |
| **M1 列级解析核心** | ✅ 已完成：`ColumnRef/ColumnEdge` + `QueryScope` 栈 + 三方言共用一份 `ColumnLineageEngine` | 207 测试全绿；语料 2376 条列边，幽灵列/UNRESOLVED/STAR 均 0 | 2d |
| **M2 归一化/图模型** | ✅ 已完成：`ColumnGraphBuilder` 折叠 + `ColumnEdge.targetInternal` 标记、`LocalRelation` 语句命名空间、`LayeredDagBuilder`（迭代 Tarjan+Kahn）、`LineageStore` 快照 | 228 测试全绿（新增 21）：环/自依赖、20000 节点长链、多 CTE 同名、别名解引用、hop 证据；全语料建图 17 节点/2329 列边/0 未折叠伪节点/0 缺层号 | 1d |
| **M3 服务层** | ✅ 已完成：starter-web 2.7.18 + `CorpusScanner`（jobId=文件#序号）+ `ScanReport` 账本 + 9 个端点 + `ApiResponse`/`ApiErrorAdvice` + `spring-boot-maven-plugin`（无需 profile，见 §7.1）。实测细节见 §7.4 | 245 测试全绿（新增 17）；真起 jar 逐端点 `curl` 通过，overview 与 M2 基线逐项一致；0 条折叠告警；无 slf4j 双绑定 | 1d |
| **M4 前端** | ✅ 已完成：vendor 三件套（cytoscape 3.32.1 + dagre 0.8.5 + cytoscape-dagre 2.5.0）+ 三栏 ES module（`main/api/graphTable/graphColumn/detailPanel/badges`）+ 表级 DAG 与字段链路两种图 + 右栏逐跳证据（hops/derivation/confidence/SQL 高亮/parseError 红条）+ PNG 与子图 JSON 导出 + hash 定位。实测细节见 §8.4 | `mvn -o clean test` **246 绿**；jar 内 static 21 文件、`GET /` 200；真浏览器（browser-use）表级/字段级金路径 + 300 节点超限 + 缺列 400 + parseError 红条 + 星号/未解析边界逐项通过，console 零 error；揪出 7 个前端缺陷与 1 处"把解析缺口说成链路起点"的语义谎言 | 2d |
| **M5 语料回归** | ✅ 已完成：子查询身份（别名只做查找键，`#subN` 才是身份）+ 无参关键字函数与 lambda 形参不再当列（§6）。实测细节与逐条定性见 §11.1 | 涉密语料：折叠告警 62→**0**、未折叠伪节点 0、幽灵列 0、UNRESOLVED 7→**2**（两条都是 CASE ELSE 的未限定列，两张未知表 ⇒ 按"宁缺勿假"口径保留，`ambiguousUnqualifiedColumnIsNotForcedOntoATable` 已锁这个语义）；`sql/` 基线：2376 原始列边不变，图列边 2329→**2327**（4 处 `current_timestamp` 假来源），0 告警。`mvn -o clean test` **252 绿** | 1d |
| **M6 文档与残留收敛** | ✅ 已完成（见 §2.0.4、§2.0.5）：README 重写、33 份历史 md 删除、死脚本清理、未跟踪垃圾清理，`superior-sql-parser-temp/` 按你确认删除（代价与教训记在 §2.0.4 末段），与血缘无关的编译日志/一次性脚本/MySQL 建表脚本再删一批。**无遗留待办**：`column_lineage` DDL 随"不落库"结论一起取消（§10） | 顶层文档不再把读者引向 Calcite / superior jar / Flink 作业路线；`sql/` 语料 0 解析错误 | 0d |

合计约 7.5 人日（§2.0 的结构与文档清理已完成，不计入）。M1 与 M3 可并行（解析层不依赖 Spring）。

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
| 大语料下字段节点数远超表（万级列） | 前端卡死 | depth 默认 3、节点上限 300、按需展开；后端裁剪而非全量下发 |
| vendored JS ~500KB 进 git | 仓库体积 | 明确一次提交锁定版本 + `vendor/README.md` 记来源，换取离线可运行 |
| 无前端构建链，手写 JS | 复杂度上限 | 只做"图 + 面板"两件事；一旦需要路由/状态管理，另立方案而不是偷偷加 webpack |

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

- [x] 1. **基线（M6 收尾后）**：`mvn -o clean package` **254/254** + boot jar 可执行、`run-lineage.bat sql` **15 文件 / 14 输出表 / 0 WARN**（原为 16/17/3，那 3 条 MySQL 建表 DDL 已随 §2.0.5 删出语料）、图列边 2327、0 折叠告警、`grep -r "org.apache.flink" src` 为空。功能落地后用例数只增不减，且这几条不被破坏。
- [x] 2. 起服务（`java -jar target/flinkLearn-0.0.1-SNAPSHOT.jar`，或 `mvn -o spring-boot:run`）后浏览器打开 `http://localhost:8080`，能看到按层排布的表级 DAG。→ dagre 实跑，17 节点分层见 §8.4。**像素级观感仍待人眼过一遍**（验证用的 in-app Browser 是 0x0 视口，只能读结构与文本）。
- [x] 3. 点击任一有列级信息的表 → 字段链路图渲染出 2 跳以上链路（中间节点在 hops 里显形）：`kafka_user_app_data.eventbodylist → [ck/user_app_data_ck_dml.sql#1]#unnest1.app_name → user_app_data.app_name`。
- [x] 4. 选中字段边 → 右栏显示逐跳链路、derivation/confidence、原始 SQL 片段高亮，三者信息一致（`85%（EXPRESSION）` 对 `confidence:0.85`，3 处 `<mark>` 对两端列名）。
- [x] 5. `SELECT *` 边显示 `STAR · 星号未展开` 徽标 + 图例色块 + 30% 置信度；`UNRESOLVED` 边在 `/api/issues`、左栏质量过滤、字段清单徽标与右栏红条四处都可见——第 3、4 处是本轮补验时才修出来的，见 §8.4。
- [x] 6. 涉密语料巡检（M5）：两份语料（`sql/` 与临时语料）列级 6 条 oracle 泄漏计数均为 0，折叠告警 0，剩下 2 条 UNRESOLVED 已定性为"没有列字典就是歧义、按约定不猜"；临时目录已删除，文档与用例里的表名列名一律脱敏（实测见 §11.1）。
- [x] 7. 本方案入库为 `outputs/` 下唯一现行设计文档；两份旧 Calcite 路线文档已删除，作废理由保留在 §0。
- [x] 8. 空快照必须自己说清是哪一种：**没在扫 / 扫失败（目录指错）/ 扫到 0 个 .sql / 跳过启动扫描**四态在页面上可区分，且 `running` 态会在 60s 内轮询自愈成图（`/api/overview` 的 `scanPhase`/`scanError` + 顶部横幅，实测见 §8.5）。
