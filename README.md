# 多引擎 SQL 血缘解析（ANTLR4）

基于 ANTLR4 的 SQL 表级血缘提取工具，支持 **Flink / Spark / Presto** 三种方言，纯语法树遍历，不依赖 Calcite、Flink Planner 或任何外部 SQL 解析 jar。

## 环境

| 项 | 版本 |
|---|---|
| JDK | 17 |
| Maven | 3.8.x（依赖已缓存在本地仓库，可 `mvn -o` 离线构建） |
| ANTLR | 4.9.3（`antlr4-maven-plugin` 从本仓库 `.g4` 生成解析器） |

## 构建与测试

```bash
mvn -o clean test          # 125 个用例
```

`clean` 是必要的：解析器由 `.g4` 生成到 `target/generated-sources`，改过语法文件后不清理会用到陈旧产物。

## 批量解析一个目录下的 SQL

```bash
run-lineage.bat            # 解析 ./sql
run-lineage.bat D:\dw\sql  # 解析任意目录（Windows 路径不能含空格）
```

输出每条语句的 `[类型] 输出: X <- 输入: a, b`，末尾给去重汇总：输出表清单、纯输入表清单（未出现在任何输出表中的表）。

## 目录结构

```
src/main/antlr4/io/github/melin/superior/parser/
  {flink,spark,presto}/antlr4/*{Lexer,Parser}.g4    三套方言语法
src/main/java/com/bigdata/lineage/
  parser/MultiEngineSQLLineageParser.java          入口：逐语句自动探测引擎并采纳结果
  parser/FlinkSQLLineageParser.java                单引擎门面
  parser/SqlSplitUtils.java                        注释/引号感知的语句切分
  parser/SqlCache.java                             LRU 结果缓存
  parser/extractor/*TableLineageExtractor.java     三方言 Visitor，血缘提取核心
  parser/model/TableLineage.java                   血缘结果模型（含 confidence / parseError）
  tools/SqlDirLineageTool.java                     目录批量解析 CLI
src/test/java/com/bigdata/lineage/parser/          表级血缘与切分回归
sql/                                               离线数仓语料 + 建表脚本
outputs/                                           方案与总结文档
```

## 约定

- **每套语法只支持本引擎方言**：Spark 有 `LATERAL VIEW` / `DISTRIBUTE BY` / `QUALIFY` / `RLIKE`，Presto 有 `CROSS JOIN UNNEST`，时态表 JOIN 与窗口 TVF 只在 Flink。补语法缺口时不要跨引擎搬运规则。
- **血缘只依赖 ANTLR 解析结果**，不引入正则或字符串猜测式的旁路解析。
- 语法有错误时 ANTLR 会做错误恢复并产出部分结果，这类结果 `parseError=true`、`confidence=0.5`，调用方必须区分对待。
- 词法保留字要成对维护：`.g4` 里 Parser 引用但 Lexer 未定义 → 隐式 token 告警；Lexer 定义但 Parser 未引用 → 该词无法再作为标识符使用，会静默吞掉列名/表名。

## 下一步

字段级血缘 + WebUI 展示（表层级 DAG + 字段来源链路）的设计见
[outputs/字段级血缘与WebUI技术方案.md](outputs/字段级血缘与WebUI技术方案.md)。

## 历史

本分支是血缘专用分支，已移除全部 Flink 作业 / UDF / Hive / Kafka / MySQL 相关代码与依赖，只保留解析器实际用到的部分。需要那些代码请回到 `master` 分支。
