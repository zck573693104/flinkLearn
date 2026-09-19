# Flink CEP 动态加载功能实现方案

## 📊 项目背景

### **开源版本** (master 分支)
- ✅ SQL 血缘解析系统
- ✅ ANTLR4 Grammar 语法定义
- ✅ 表级血缘提取
- ✅ 基础测试用例

### **商业版本** (feature/cep-dynamic-loading 分支)
- ⏳ Flink CEP 动态加载功能
- ⏳ 前端管理页面
- ⏳ 规则热更新机制
- ⏳ 可视化配置界面

---

## 🎯 需求分析

### **参考项目**: [ververica-cep-demo](https://github.com/RealtimeCompute/ververica-cep-demo)

**核心功能**:
1. CEP 规则动态加载（无需重启）
2. 规则管理 API
3. 实时告警推送
4. 监控面板展示

**技术栈**:
- Flink 1.14+
- CEP Library
- Spring Boot
- WebSocket
- Vue.js (前端)

---

## 🏗️ 架构设计

### **整体架构图**

```
┌─────────────────────────────────────────────────────────────┐
│                      前端管理页面                              │
│              (Vue.js + Element UI)                           │
│   - 规则配置界面                                              │
│   - 实时监控面板                                              │
│   - 告警历史记录                                              │
└──────────────────────┬──────────────────────────────────────┘
                       │ HTTP/WebSocket
                       ↓
┌─────────────────────────────────────────────────────────────┐
│                    Spring Boot Server                        │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ REST API Controller                                  │  │
│  │ - GET /api/rules     - 获取所有规则                   │  │
│  │ - POST /api/rules    - 新增/更新规则                  │  │
│  │ - DELETE /api/rules/{id} - 删除规则                   │  │
│  │ - GET /api/metrics   - 获取运行指标                   │  │
│  └──────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ CEP Rule Manager                                     │  │
│  │ - 规则热加载核心逻辑                                  │  │
│  │ - 状态管理                                            │  │
│  │ - 事件分发                                            │  │
│  └──────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ WebSocket Handler                                    │  │
│  │ - 实时推送告警                                        │  │
│  │ - 心跳检测                                            │  │
│  └──────────────────────────────────────────────────────┘  │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ↓
┌─────────────────────────────────────────────────────────────┐
│                    Flink CEP Engine                          │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Pattern Compiler                                     │  │
│  │ - 编译 CEP 模式定义                                    │  │
│  │ - 生成 Pattern 对象                                    │  │
│  └──────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Pattern Match Listener                               │  │
│  │ - 监听匹配结果                                        │  │
│  │ - 触发告警动作                                        │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## 📦 模块划分

### **1. cep-core - CEP 核心引擎**

**职责**: 
- CEP 规则定义
- Pattern 编译
- 模式匹配
- 告警触发

**关键类**:
```java
package com.bigdata.cep.core;

// CEP 规则接口
public interface CepRule {
    String getId();
    String getName();
    Pattern getPattern();
    AlertHandler getAlertHandler();
}

// Pattern 编译器
public class PatternCompiler {
    public Pattern compile(String patternDefinition);
}

// 告警处理器接口
public interface AlertHandler {
    void handle(CepEvent event, MatchResult match);
}
```

### **2. cep-loader - 规则加载器**

**职责**:
- 从数据库/文件加载规则
- 动态更新规则
- 规则版本管理

**关键类**:
```java
package com.bigdata.cep.loader;

// 规则加载器
public class CepRuleLoader {
    // 加载规则并应用到 CEP 引擎
    public void loadRule(CepRule rule);
    
    // 更新规则
    public void updateRule(CepRule rule);
    
    // 删除规则
    public void removeRule(String ruleId);
    
    // 重新加载所有规则
    public void reloadAllRules();
}
```

### **3. cep-api - REST API 服务**

**职责**:
- 提供规则管理 API
- 提供监控数据 API
- WebSocket 实时推送

**API 端点**:
```java
@RestController
@RequestMapping("/api")
public class CepRuleController {
    
    @PostMapping("/rules")
    public ResponseEntity<CepRule> createRule(@RequestBody CepRuleConfig config);
    
    @PutMapping("/rules/{id}")
    public ResponseEntity<CepRule> updateRule(@PathVariable String id, @RequestBody CepRuleConfig config);
    
    @DeleteMapping("/rules/{id}")
    public ResponseEntity<Void> deleteRule(@PathVariable String id);
    
    @GetMapping("/rules")
    public List<CepRule> getAllRules();
    
    @GetMapping("/metrics")
    public CepMetrics getMetrics();
}
```

### **4. cep-web - 前端管理页面**

**职责**:
- 规则配置界面
- 实时监控面板
- 告警历史记录

**技术栈**:
- Vue.js 3
- Element Plus
- ECharts (图表)
- Axios (HTTP 客户端)

**页面结构**:
```
/src
├── views
│   ├── RuleEditor.vue      # 规则编辑器
│   ├── Dashboard.vue       # 监控面板
│   ├── AlertHistory.vue    # 告警历史
│   └── Settings.vue        # 系统设置
├── components
│   ├── PatternBuilder.vue  # Pattern 可视化构建器
│   ├── EventPreview.vue    # 事件预览
│   └── MetricsChart.vue    # 指标图表
└── api
    └── cep.js              # API 调用封装
```

---

## 🔧 核心功能实现

### **功能 1: CEP 规则定义**

```java
// CepRule.java
@Data
public class CepRule implements Serializable {
    private String id;
    private String name;
    private String description;
    private String patternJson;  // JSON 格式的 Pattern 定义
    private AlertType alertType; // 告警类型：EMAIL, WEBHOOK, DATABASE
    private boolean enabled;
    private Date createdAt;
    private Date updatedAt;
}

// AlertType.java
public enum AlertType {
    EMAIL,           // 发送邮件
    WEBHOOK,         // HTTP Webhook
    DATABASE,        // 写入数据库
    KAFKA,           // 发送到 Kafka
    NONE             // 无操作
}
```

### **功能 2: Pattern 动态编译**

```java
// PatternCompiler.java
public class PatternCompiler {
    
    /**
     * 编译 Pattern 定义
     * @param patternJson JSON 格式的 Pattern 定义
     * @return Pattern 对象
     */
    public Pattern compile(String patternJson) {
        // 示例 Pattern: SELECT * FROM myTable WHERE price > 100 AND time > now() - interval '5' minute
        PatternDefinition definition = parsePatternJson(patternJson);
        
        Pattern.Builder builder = Pattern.<CepEvent>named("myPattern")
            .times(definition.getTimes())
            .timesMin(definition.getTimesMin())
            .of(getConditions(definition.getConditions()))
            .where(getWhereClause(definition.getWhereClause()))
            .allowNested(getNestedPatterns(definition.getNested()));
            
        return builder.build();
    }
    
    private SimpleCondition<CepEvent> getConditions(List<Condition> conditions) {
        return new SimpleCondition<CepEvent>() {
            @Override
            public boolean eval(CepEvent event, TimeTimestampExtractor timestampExtractor) {
                // 评估条件
                return true;
            }
        };
    }
}
```

### **功能 3: 规则热加载**

```java
// CepRuleLoader.java
@Service
public class CepRuleLoader {
    
    @Autowired
    private CepRuleRepository ruleRepository;
    
    @Autowired
    private PatternCompiler patternCompiler;
    
    private Map<String, StreamOperator<CepEvent>> operators = new ConcurrentHashMap<>();
    
    /**
     * 加载规则到 CEP 引擎
     */
    public void loadRule(CepRule rule) throws Exception {
        if (!rule.isEnabled()) {
            return;
        }
        
        // 1. 编译 Pattern
        Pattern pattern = patternCompiler.compile(rule.getPatternJson());
        
        // 2. 创建 CEP 流
        KeyedStream<CepEvent, String> stream = createCepStream(rule);
        
        // 3. 应用 Pattern
        PatternProcessFunction<CepEvent> processFunction = new PatternProcessFunction<>(pattern, rule.getAlertHandler());
        DataStream<String> resultStream = stream.process(processFunction);
        
        // 4. 注册到运算符映射
        operators.put(rule.getId(), stream);
        
        log.info("Successfully loaded CEP rule: {}", rule.getName());
    }
    
    /**
     * 更新规则（热更新）
     */
    public void updateRule(CepRule rule) throws Exception {
        // 1. 停止旧规则
        if (operators.containsKey(rule.getId())) {
            stopRule(rule.getId());
        }
        
        // 2. 加载新规则
        loadRule(rule);
        
        log.info("Successfully updated CEP rule: {}", rule.getName());
    }
    
    /**
     * 删除规则
     */
    public void removeRule(String ruleId) {
        stopRule(ruleId);
        operators.remove(ruleId);
        ruleRepository.delete(ruleId);
        log.info("Successfully removed CEP rule: {}", ruleId);
    }
}
```

### **功能 4: WebSocket 实时推送**

```java
// WebSocketHandler.java
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {
    
    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/ws").withSockJS();
    }
    
    @Override
    public void configureWebSocketTransport(WebSocketTransportRegistry registry) {
        registry.setMessageTimeout(30000);
        registry.setSendTimeLimit(10000);
        registry.setSendBufferSizeLimit(512);
    }
}

// AlertNotificationService.java
@Service
public class AlertNotificationService {
    
    @Autowired
    private SimpMessagingTemplate messagingTemplate;
    
    /**
     * 发送告警通知
     */
    public void sendAlert(CepRule rule, MatchResult match) {
        // 1. 保存到数据库
        AlertRecord record = saveAlertToDatabase(rule, match);
        
        // 2. WebSocket 推送给前端
        messagingTemplate.convertAndSend("/topic/alerts", record);
        
        // 3. 根据告警类型执行相应动作
        switch (rule.getAlertType()) {
            case EMAIL:
                sendEmailAlert(record);
                break;
            case WEBHOOK:
                sendWebhookAlert(record);
                break;
            case DATABASE:
                writeAlertToDatabase(record);
                break;
            case KAFKA:
                sendToKafka(record);
                break;
        }
    }
}
```

---

## 📋 开发计划

### **Phase 1: 核心引擎开发** (预计 3-5 天)

- [ ] 创建 cep-core 模块
- [ ] 实现 Pattern 编译器
- [ ] 实现规则加载器
- [ ] 单元测试覆盖

### **Phase 2: REST API 开发** (预计 2-3 天)

- [ ] 创建 cep-api 模块
- [ ] 实现 CRUD 接口
- [ ] 添加 Swagger 文档
- [ ] 集成测试

### **Phase 3: 前端页面开发** (预计 5-7 天)

- [ ] 创建 cep-web 模块
- [ ] 实现规则编辑器
- [ ] 实现监控面板
- [ ] 实现告警历史
- [ ] 响应式布局

### **Phase 4: 集成测试** (预计 2-3 天)

- [ ] 端到端测试
- [ ] 性能测试
- [ ] 安全测试
- [ ] 文档完善

---

## 🛠️ 技术栈选择

### **后端**
- Java 17
- Spring Boot 2.7+
- Flink 1.14+
- Maven
- PostgreSQL (规则存储)

### **前端**
- Vue.js 3
- Element Plus
- Vite
- TypeScript
- ECharts

### **部署**
- Docker
- Docker Compose
- Kubernetes (可选)

---

## 📝 代码结构

```
flinkLearn/
├── pom.xml
├── cep-core/                    # CEP 核心引擎
│   ├── src/main/java/com/bigdata/cep/core/
│   │   ├── CepRule.java
│   │   ├── PatternCompiler.java
│   │   ├── CepRuleLoader.java
│   │   └── AlertHandler.java
│   └── src/test/java/...
├── cep-api/                     # REST API 服务
│   ├── src/main/java/com/bigdata/cep/api/
│   │   ├── CepRuleController.java
│   │   ├── AlertNotificationService.java
│   │   └── WebSocketConfig.java
│   └── src/test/java/...
├── cep-web/                     # 前端管理页面
│   ├── src/
│   │   ├── views/
│   │   ├── components/
│   │   └── api/
│   └── package.json
└── docs/                        # 文档
    ├── API 文档.md
    ├── 部署指南.md
    └── 用户手册.md
```

---

## 🎯 里程碑

| 阶段 | 目标 | 预计时间 | 状态 |
|------|------|---------|------|
| **M1** | 完成核心引擎开发 | Week 1 | ⏳ 待开始 |
| **M2** | 完成 REST API 开发 | Week 2 | ⏳ 待开始 |
| **M3** | 完成前端页面开发 | Week 3-4 | ⏳ 待开始 |
| **M4** | 完成集成测试和文档 | Week 5 | ⏳ 待开始 |
| **M5** | 发布 v1.0 正式版 | Week 6 | ⏳ 待开始 |

---

## 📞 下一步行动

1. **初始化项目结构**
   ```bash
   # 创建 Maven 多模块项目
   mvn archetype:generate -DgroupId=com.bigdata.cep -DartifactId=cep-core
   mvn archetype:generate -DgroupId=com.bigdata.cep -DartifactId=cep-api
   npm create vite@latest cep-web -- --template vue
   ```

2. **实现 Phase 1**
   - 创建 cep-core 模块
   - 实现基本的 Pattern 编译和规则加载

3. **持续迭代**
   - 每个阶段完成后提交代码
   - 保持与 master 分支的同步

---

## 💡 注意事项

1. **向后兼容性**
   - 确保新功能不影响现有的 SQL 血缘解析功能
   - 使用独立的包名空间

2. **安全性**
   - 规则验证和沙箱隔离
   - API 鉴权和授权
   - 输入参数校验

3. **性能优化**
   - Pattern 缓存机制
   - 异步处理告警
   - 连接池管理

4. **可维护性**
   - 完善的日志记录
   - 异常处理机制
   - 单元测试覆盖

---

**开始实施！** 🚀
