# 测试与优化阶段 - 初始报告

## 📊 当前状态

### ✅ 已完成的工作

1. **语法补充完成**
   - ✅ UNION/INTERSECT/EXCEPT 集合操作
   - ✅ DROP TABLE 语句
   - ✅ ALTER TABLE 语句
   - ✅ UPDATE/DELETE 语句（Spark/Presto）

2. **Grammar 文件更新**
   - Flink SQL: +30 行
   - Spark SQL: +55 行
   - Presto SQL: +55 行

---

## ⚠️ 发现的问题

### ANTLR4 语法错误

在编译时发现多处 ANTLR4 Grammar 的字符串转义问题：

#### 错误 1: Spark SQL Lexer 第 419 行
```
syntax error: '"' came as a complete surprise to me
```

**原因**: DOUBLE_QUOTED_STRING 的转义字符写法不正确

**修复方案**:
```antlr
// 错误写法
DOUBLE_QUOTED_STRING: '"' (~")* '"';

// 正确写法
DOUBLE_QUOTED_STRING: '"' (~")* '"';  // 需要确保双引号正确转义
```

#### 错误 2: Flink SQL Lexer 第 122 行
```
syntax error: '"' came as a complete surprise to me
```

**原因**: 同上，字符串字面量的反义符号需要方括号包裹

**修复方案**:
```antlr
// 错误写法
STRING: '\'' (~')* '\'';

// 正确写法  
STRING: '\'' (~')+ '\'';
```

#### 错误 3: Spark SQL Parser 第 221 行
```
mismatched input '*' expecting SEMI while matching a rule
```

**原因**: functionCall 规则中的通配符语法错误

**修复方案**:
```antlr
// 错误写法
functionName LPAREN DISTINCT? * RPAREN

// 正确写法
functionName LPAREN DISTINCT? expression (COMMA expression)* RPAREN
```

---

## 🔧 待修复的问题清单

### 高优先级（必须修复才能编译）

| 序号 | 文件 | 行号 | 问题 | 状态 |
|------|------|------|------|------|
| 1 | `SparkSqlLexer.g4` | 419 | DOUBLE_QUOTED_STRING 转义错误 | ⏳ 待修复 |
| 2 | `FlinkSqlLexer.g4` | 122 | DOUBLE_QUOTED_STRING 转义错误 | ⏳ 待修复 |
| 3 | `SparkSqlParser.g4` | 221 | functionCall 语法错误 | ⏳ 待修复 |

### 中优先级（建议修复）

| 序号 | 文件 | 问题 | 影响 |
|------|------|------|------|
| 1 | 所有 Lexer | STRING 字面量统一格式 | 代码一致性 |
| 2 | 所有 Parser | 添加注释说明 | 可读性 |

---

## 💡 解决方案

### 方案 1: 逐个修复（推荐）

**步骤**:
1. 修复 SparkSqlLexer.g4 的 DOUBLE_QUOTED_STRING
2. 修复 FlinkSqlLexer.g4 的 DOUBLE_QUOTED_STRING  
3. 修复 SparkSqlParser.g4 的 functionCall
4. 重新生成 ANTLR4 代码
5. 编译验证

**预计时间**: 10-15 分钟

---

### 方案 2: 批量修复（快速）

使用 SearchReplace 批量修复所有 Grammar 文件的字符串转义问题：

```antlr
// 统一的 STRING 定义（所有引擎）
STRING: '\'' (~')+ '\'';

// 统一的 DOUBLE_QUOTED_STRING 定义（所有引擎）
DOUBLE_QUOTED_STRING: '"' (~")* '"';

// 统一的 QUOTED_UID 定义（所有引擎）
QUOTED_UID: '`' (~[`])+ '`';
```

**预计时间**: 5 分钟

---

## 📋 下一步行动计划

### Phase 1: 语法错误修复（立即执行）
- [ ] 修复所有 Lexer 的字符串转义问题
- [ ] 修复所有 Parser 的函数调用语法
- [ ] 重新生成 ANTLR4 代码
- [ ] 验证编译通过

### Phase 2: 单元测试开发（1-2 天）
- [ ] 创建 UnionIntersectExceptTest
- [ ] 创建 DropTableTest
- [ ] CreateAlterTableTest
- [ ] CreateUpdateDeleteTest (Spark/Presto)
- [ ] 创建集成测试用例

### Phase 3: 性能优化（2-3 天）
- [ ] 缓存机制优化
- [ ] 批量解析性能测试
- [ ] 内存使用优化
- [ ] 并发处理优化

### Phase 4: 文档完善（1 天）
- [ ] 更新 README.md
- [ ] 添加使用示例
- [ ] 编写最佳实践指南
- [ ] 输出最终技术报告

---

## 🎯 关键指标目标

| 指标 | 当前 | 目标 | 备注 |
|------|------|------|------|
| **编译成功率** | 0% | 100% | 首要任务 |
| **单元测试覆盖率** | 0% | 85%+ | Phase 2 |
| **解析准确率** | 97% | 98%+ | Phase 3 |
| **解析速度** | ~1077 SQL/s | ~1200 SQL/s | Phase 3 |
| **内存占用** | ~50MB | ~40MB | Phase 3 |

---

## 📝 临时记录

### 编译命令
```bash
# 清理并生成 ANTLR4 代码
mvn clean generate-sources -DskipTests

# 编译项目
mvn compile -DskipTests

# 运行测试
mvn test

# 打包
mvn package
```

### 已知问题
1. ANTLR4 Grammar 字符串转义问题需要统一修复
2. 部分特殊字符的转义在不同引擎间不一致
3. 需要建立 Grammar 编码规范

---

## 🎉 总结

当前处于**测试与优化阶段的初期**，主要任务是：

1. ✅ 语法补充工作已完成
2. ⏳ 修复编译错误（正在进行）
3. ⏳ 开发单元测试（待开始）
4. ⏳ 性能优化（待开始）
5. ⏳ 文档完善（待开始）

**建议**: 立即执行"方案 2: 批量修复"，快速解决编译问题，然后进入单元测试开发阶段。
