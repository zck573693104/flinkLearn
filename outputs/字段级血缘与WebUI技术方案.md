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
| 运维脚本与残留 | `sh/**`（8 个 Hive/CK 部署脚本）、`flink-sql-lineage-parser/pom.xml`（空壳残留）、`superior-sql-parser-temp`（误提交的 mode 160000 gitlink、无 `.gitmodules`，仅移索引项、磁盘未动） |
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

**唯一保留项**：`superior-sql-parser-temp/` 是一个**独立的嵌套 git 仓库**（自带 `.git`，remote 为 `github.com/melin/superior-sql-parser`），其 HEAD `94fd069e`（分支 `flink-only-jdk17-dev`，"完成 Flink SQL 解析器 JDK 17 升级和模块精简"）**不存在于任何远端分支**，我们仓库历史上也只有一个 160000 gitlink 指针、没有它的对象。删掉即永久丢失该提交，故保留待你确认（可先 `git bundle` 导出孤立提交再删）。

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
| 现有 `sql/init_lineage_db.sql` 的 `column_lineage` 表**不足以支撑字段溯源** | `sql/init_lineage_db.sql:35-42` 只有 `id/lineage_id/source_column/target_column`，缺 source_table/target_table/job_id/derivation/confidence/engine/唯一键 |

结论：需要新建 Web 骨架 + 新建持久化模型；旧 `controller`/`service`/`model` 三件套是"表级 + 手写 Map 返回"的半成品，本方案**不复用它**（避免再造一套并行的血缘存储），而是在其上建立新的只读查询层，旧三件套列为 M6 清理项。

### 2.2 解析层：表级已稳定，列级只有缺口没有地基

- 入口：`MultiEngineSQLLineageParser.extractTableLineages(String)` → `:50`；引擎自动探测顺序 Flink→Spark→Presto `:110/:116/:122`；采纳条件 `isCleanLineage = !parseError && hasLineage()` `:148-150`；LRU 1000 缓存 `SqlCache.java:12-20`。
- 结果模型：`parser/model/TableLineage.java:17-62`（`targetTable/sourceTables/processType/insertMode/confidence/hasCte/hasTemporalJoin/hasWindowFunc/parseError/originalSql`）。
  ⚠ 已知坑：`confidence = 0.95` 没有 `@Builder.Default`，任何 `builder()` 不显式 `.confidence()` 得到 0.0。列级模型必须避开这个 Lombok 陷阱。
- 三方言 Visitor：`extractor/TableLineageExtractor.java`（Flink）、`SparkTableLineageExtractor.java`、`PrestoTableLineageExtractor.java`。三者结构对称，状态字段 `targetTable/sourceTables/cteNames/processType/insertMode/hasCte/...`，`addSourceTable` 用扁平 `cteNames` 过滤（Flink `:216`、Spark `:209`）。
- Grammar 规模：Flink parser 436 行 / 62 规则，Spark 537 / 75，Presto 469 / 68。
- 表级语料基线（不能破坏）：`run-lineage.bat sql` → 16 文件 / 17 输出表 / 3 WARN（MySQL 初始化脚本）；`mvn clean test` → 125/125。

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
    String jobId;            // 文件路径 + 语句序号，如 part3.sql#7；单独解析时为 null
    String engine;           // FLINK | SPARK | PRESTO，建边时一次写定，避免在缓存共享对象上补字段
    boolean targetInternal;  // 目标关系是不是引擎内部中间关系，图构建层据此折叠（§6）
}
```

`targetInternal` 是 M2 加的：带别名的子查询以别名登记，名字与 CTE、物理表同形，只有引擎知道它是中间产物，所以由边带标记而不是让下游猜名字。

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
  - 关键坑：**带别名的子查询以别名登记**（`ColumnLineageEngine.registerRef` → `subTarget = alias != null ? alias : nextPseudo("sub")`），名字与 CTE、物理表完全同形，靠 `#` 前缀判断会漏掉语料里绝大多数子查询。因此由引擎在产出边时打 `ColumnEdge.targetInternal` 标记（`internalRelations` 集合在 `registerRef`/`registerExpansion` 登记），图构建层只认这个标记。
  - 折叠后加工方式取**整条链置信度最低**的那一档（`weakest`）：外层 `IDENTITY` 套内层 `AGGREGATE` 只能是 `AGGREGATE`。
  - 标了内部却没有产出边可折（引擎漏边）时**丢弃该跳并 log.warn**，宁缺勿假；`SqlDirLineageTool` 的"未折叠的伪节点"一节把这类残留变成可见计数（全语料当前为 0）。
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
  起服务：`mvn spring-boot:run`（或 `java -jar target/flinkLearn-0.0.1-SNAPSHOT.jar`）。
- **不再需要 spring 族版本对齐**。`spring-expression:5.3.26` 已随 §2.0 的 pom 精简删除，spring 版本全部由 starter-web 2.7.18 带的 5.3.31 单点决定，不存在混版本。
- 仍需保留的一条：排 `spring-boot-starter-logging`。项目现在是 `slf4j-api:1.7.25` + `log4j-slf4j-impl:2.9.1`（`log4j2.xml` 在 resources），starter 默认带的 logback 会造成 slf4j 双绑定。若嫌麻烦，替代方案是反过来删掉本项目的 log4j2 三件套、改用 starter 默认 logback——**推荐前者**（改动面小，且 `log4j2.xml` 的 warn 级别策略沿用至今）。

### 7.2 组件

```
com.bigdata.lineage.web/
  LineageWebApplication.java     @SpringBootApplication(scanBasePackages="com.bigdata.lineage.web")
  CorpusScanner.java             遍历 lineage.scan-dir 下 .sql → SqlSplitUtils → MultiEngineSQLLineageParser
  LineageStore.java              AtomicReference<Snapshot> + 索引（表→节点、表→边、列→边）
  GraphAssembler.java            子图裁剪：direction(up/down/both) + depth + root
  api/LineageApiController.java  下列端点
  api/ApiErrorAdvice.java        @ControllerAdvice，统一 {success,message,data}
```

配置（写入 `application.yml`，把现有死配置真正接上）：

```yaml
server: { port: 8080 }
lineage:
  scan-dir: sql          # 语料目录，可传参覆盖
  engines: auto          # 交给 MultiEngineSQLLineageParser 自动探测
  rescan-on-start: true
```

配置：`application.yml` 已在 §2.0 精简为 `server.port` + `lineage.scan-dir` + `lineage.rescan-on-start`（原来那三段 mysql/mybatis/jsqlparser 死配置已删，`com.github.jsqlparser` 和 `com.bigdata.lineage.model` 早就不存在了）。M3 只需往里补 spring 的静态资源相关键，不需要再清理。持久化不引 `spring-boot-starter-jdbc`，因此不会触发 `DataSourceAutoConfiguration` 因缺数据源而启动失败。

### 7.3 REST 契约

| 方法 | 路径 | 用途 | 关键参数 |
|---|---|---|---|
| GET | `/api/overview` | 统计：文件数/表数/字段边数/失败语句数/未解析边数 | — |
| GET | `/api/tables` | 表列表（含上下游度数、所在层、是否有列级信息） | `q`（子串/正则）、`layer`、`onlyUnresolved` |
| GET | `/api/graph/table` | 表级分层 DAG | `root`、`direction=up\|down\|both`、`depth`（默认 2，上限 5）、`layer` |
| GET | `/api/table/{table}/columns` | 该表字段清单 + 每字段来源摘要 | — |
| GET | `/api/graph/column` | 字段级链路子图 | `table`、`column`（可空=整表）、`depth`（默认 3） |
| GET | `/api/edge/column` | 单条边的证据 | `from`、`to` → `{jobId,file,ordinal,derivation,transform,hops[],sqlText,confidence}` |
| GET | `/api/issues` | 质量视图：parseError 语句、`UNRESOLVED`/`STAR` 边、孤儿表 | `type` |
| POST | `/api/parse` | 贴 SQL 试解析（只读、不落库） | `{sql}` → 表级 + 列级 JSON |
| POST | `/api/scan` | 重扫目录，原子换快照 | `{dir?}` → 新统计 |

响应统一 `{ "success": bool, "data": ..., "message": ... }`，与既有 `TableLineageController` 的手工 Map 风格一致，前端只认一种信封。

图 JSON schema（Cytoscape 直接可喂）：

```json
{ "nodes": [ {"id":"dws_pms.dws_x","name":"dws_x","db":"dws_pms","layer":3,
              "kind":"physical|derived","colCount":42} ],
  "edges": [ {"id":"e:..","from":"dwd_pms.dwd_a.emp_id","to":"dws_pms.dws_x.emp_cnt",
              "derivation":"AGGREGATE","confidence":0.8,"jobId":"part3.sql#7"} ],
  "ranks": {"dwd_pms.dwd_a":1}, "cyclic": ["ods_x"] }
```

## 8. 前端层

### 8.1 目录与资产

```
src/main/resources/static/
  index.html
  css/app.css
  js/main.js  api.js  graphTable.js  graphColumn.js  detailPanel.js  badges.js   （ES module，无打包）
  vendor/cytoscape.min.js        3.32.x（~450KB）
  vendor/cytoscape-dagre.js      2.5.x
  vendor/dagre.min.js            0.8.5
```

获取方式（npm registry 已确认可达，`npm view cytoscape version` → 3.34.3）：
`npm pack cytoscape@3.32.1 dagre@0.8.5 cytoscape-dagre@2.5.0` → 解包取 `dist/*.min.js` 拷进 `static/vendor/`，并在 `vendor/README.md` 记录版本与来源，便于审计。**不使用 CDN**（离线演示与内网部署要求）。若 dagre 拿不到，退化为 cytoscape 内置 `breadthfirst` + 后端下发的 `ranks` 做手工定位（图构建层已算好 rank，这是天然兜底）。

### 8.2 三栏交互

- **左栏**：表搜索框 + 层级过滤（ODS/DWD/DWS/ADS/其他，按前缀识别）+ 质量开关（只看有解析错误、只看未解析字段）。列表项显示 `↑n ↓m` 度数和列级覆盖度。
- **中栏**：图，两个 tab 共享选区。
  - *表级 DAG*：LR 分层布局，节点色块 = 层级，边色 = `process_type`，点表节点出上下文菜单"看字段血缘 / 只看下游 / 展开整表字段"。
  - *字段链路*：以选中表的字段为泳道（swimlane）纵向排布，节点 `table.column`，CTE/子查询字段画成 `derived` 虚线框；星号边 `*` 徽标且不可再展开（明确提示"v1 未做星号展开"）。
  - 性能：默认 `depth=2`、节点上限 300，超限提示"请用根节点裁剪"，字段模式按需展开（点击才请求下一跳），不做全量渲染。
- **右栏（详情，回答"字段来源"的核心）**：选中字段边时显示
  1. 目标 `dws_x.emp_cnt` ← 来源列表（多来源全部列出）
  2. 逐跳链路 `dwd_a.amount →(SUM, agg) cte_sum.cnt →(IDENTITY) dws_x.emp_cnt`
  3. `derivation` / `confidence` / `jobId` / SELECT 第几位
  4. 原始 SQL 片段，把来源列和目标列各画一条高亮框
  5. 质量提示：parseError 时红条"此边来自语法错误恢复的部分结果，不可信"
- 无框架、无 TS、无构建：`<script type="module" src="/js/main.js">`。

### 8.3 可访问性与导出

PNG 导出（`cytoscape.toBlob`）、子图 JSON 导出、URL hash 携带 `root/table/column` 便于分享定位。

## 9. 里程碑与工作量

| 阶段 | 内容 | 验收 | 估时 |
|---|---|---|---|
| **M0 语法缺口** | ✅ 已完成：§5 三处改造 + 探针验证 `WITH t(a,b)`、`LATERAL VIEW ... t AS c` | `mvn clean test` 全绿，`run-lineage.bat sql` 16 文件/17 输出表/0 纯输入表 | 0.5d |
| **M1 列级解析核心** | ✅ 已完成：`ColumnRef/ColumnEdge` + `QueryScope` 栈 + 三方言共用一份 `ColumnLineageEngine` | 207 测试全绿；语料 2376 条列边，幽灵列/UNRESOLVED/STAR 均 0 | 2d |
| **M2 归一化/图模型** | ✅ 已完成：`ColumnGraphBuilder` 折叠 + `ColumnEdge.targetInternal` 标记、`LocalRelation` 语句命名空间、`LayeredDagBuilder`（迭代 Tarjan+Kahn）、`LineageStore` 快照 | 228 测试全绿（新增 21）：环/自依赖、20000 节点长链、多 CTE 同名、别名解引用、hop 证据；全语料建图 17 节点/2329 列边/0 未折叠伪节点/0 缺层号 | 1d |
| **M3 服务层** | starter-web + Scanner + 9 个端点 + `@ControllerAdvice` + `spring-boot-maven-plugin`（无需 profile，见 §7.1） | `curl` 逐端点验证；`mvn spring-boot:run` 起得来；日志无 slf4j 双绑定告警 | 1d |
| **M4 前端** | vendor 资产 + 三栏 + 两种图 + 详情证据 | **必须真浏览器点开验证**（走 browser-use），表级/字段级各跑一遍金路径 + 星号/未解析边界 | 2d |
| **M5 语料回归** | 用忽略的临时目录跑涉密语料列级质量巡检（只本地，不落仓库），修问题 | 泄漏巡检 0；列级"编造列"0 | 1d |
| **M6 文档与残留收敛** | ✅ 已完成（见 §2.0.4）：README 重写、33 份历史 md 删除、死脚本清理、未跟踪垃圾清理。剩余：`column_lineage` DDL 升级脚本（随 §10 持久化阶段出）、`superior-sql-parser-temp/` 待你确认删除 | 顶层文档不再把读者引向 Calcite / superior jar / Flink 作业路线 | 0d |

合计约 7.5 人日（§2.0 的结构与文档清理已完成，不计入）。M1 与 M3 可并行（解析层不依赖 Spring）。

## 10. 持久化（明确延后，但先给正确 DDL）

v1 是**内存快照 + 目录扫描**，启动即可用，无外部依赖，这符合"最小依赖"约束。若后续要落 MySQL，现有 `column_lineage`（`sql/init_lineage_db.sql:35-42`）必须升级为：

```sql
CREATE TABLE IF NOT EXISTS column_lineage (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    job_id VARCHAR(128) NOT NULL COMMENT '文件+语句标识，如 part3.sql#7',
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
- Web：每端点 `@WebMvcTest` 或 `curl` 冒烟；`/api/parse` 断言与 `MultiEngineSQLLineageParser` 直调结果一致。
- 前端：**真实浏览器点一遍**（M4 验收），不做"我以为能渲染"的声明。
- 构建纪律（既有记忆约束）：报"通过"之前必 `mvn clean test`，不信陈旧 `target/`。
- 涉密语料纪律：任何来自 `D:\ai coding\...` 的 SQL 只在 git 忽略的 `tmpdb/` 复现，**不复制进仓库、不写进用例、不提交**；提交前删除 `tmpdb/`（本分支截至 2026-09-21 已清空，M5 复现时重新拷贝）。

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
- 不做 MySQL 持久化读写（§10 只给 DDL 预案）。
- 不做权限/多租户/审计、不做血缘编辑与人工修正回写。
- 不做前端框架（React/Vue）、不做 npm 构建步骤进入 Maven。
- 不动三套 grammar 的表级规则（已稳定），除 §5 三处。
- **不拆多模块**：血缘与非血缘零交叉引用，且 Maven 要求聚合器必须 `pom` 打包（§2.0.1 实测），拆模块的收益可用"直接删掉 Flink 侧"拿到，成本却是搬家 61 个类和 kerberos 配置。血缘工程保持单模块。
- 不在本分支保留 Flink 作业/UDF 代码——它们在 `master`，需要时从那里取。

## 14. 验收标准（可打勾）

1. **基线（本分支当前已满足）**：`mvn -o clean test` 125/125、`run-lineage.bat sql` 16 文件/17 输出表/3 WARN、`grep -r "org.apache.flink" src` 为空。功能落地后用例数 ≥125+53，且这三条不被破坏。
2. `mvn spring-boot:run` 后浏览器打开 `http://localhost:8080`，能看到按层排布的表级 DAG。
3. 点击任一有列级信息的表 → 字段链路图渲染出至少一条 2 跳以上链路（CTE/子查询中间节点可见）。
4. 选中字段边 → 右栏显示逐跳链路、derivation/confidence、原始 SQL 片段高亮，三者信息一致。
5. `SELECT *` 边显示为 `STAR` 徽标且提示未展开；`UNRESOLVED` 边在 `/api/issues` 与 UI 中双向可见。
6. 涉密语料巡检：列级 6 条 oracle 泄漏计数为 0，`tmpdb/` 已删除。
7. 本方案入库为 `outputs/` 下唯一现行设计文档；两份旧 Calcite 路线文档已删除，作废理由保留在 §0。
