# Flink CEP 动态加载管理系统

## 📖 项目简介

本项目是一个完整的 Flink CEP（Complex Event Processing）动态加载管理系统，提供规则管理、实时监控和告警推送功能。支持无需重启即可动态加载和更新 CEP 规则。

### ✨ 核心特性

- ✅ **动态加载**: 支持运行时动态加载 CEP 规则，无需重启服务
- ✅ **热更新**: 实时更新规则配置，实时生效
- ✅ **REST API**: 完整的 RESTful API 接口
- ✅ **WebSocket 推送**: 实时推送告警通知到前端
- ✅ **可视化界面**: Vue.js + Element Plus 管理页面
- ✅ **实时监控**: 仪表盘展示关键指标和图表
- ✅ **告警历史**: 完整的告警记录和查询功能
- ✅ **Swagger 文档**: 自动生成 API 文档

---

## 🏗️ 技术栈

### 后端
- **Java 17** - 编程语言
- **Spring Boot 2.7+** - Web 框架
- **Flink 1.14+** - CEP 引擎
- **Maven** - 构建工具
- **Swagger/OpenAPI** - API 文档

### 前端
- **Vue.js 3** - 渐进式 JavaScript 框架
- **Vite** - 下一代前端构建工具
- **Element Plus** - UI 组件库
- **ECharts** - 数据可视化
- **Axios** - HTTP 客户端
- **Pinia** - 状态管理
- **Vue Router** - 路由管理

---

## 📦 项目结构

```
flinkLearn/
├── cep-core/                    # CEP 核心引擎模块
│   ├── src/main/java/com/bigdata/cep/core/
│   │   ├── CepEvent.java        # 事件基类
│   │   ├── CepRuleLoader.java   # 规则加载器
│   │   └── PatternCompiler.java # Pattern 编译器
│   └── pom.xml
│
├── cep-api/                     # REST API 服务模块
│   ├── src/main/java/com/bigdata/cep/api/
│   │   ├── CepApplication.java      # Spring Boot 启动类
│   │   ├── CepRuleController.java   # 规则管理控制器
│   │   ├── RuleConfig.java          # 规则配置类
│   │   ├── config/
│   │   │   └── WebSocketConfig.java # WebSocket 配置
│   │   └── service/
│   │       └── AlertNotificationService.java # 告警服务
│   ├── src/main/resources/
│   │   └── application.yml          # 配置文件
│   └── pom.xml
│
├── cep-web/                     # 前端管理页面模块
│   ├── src/
│   │   ├── api/
│   │   │   └── cep.js             # API 调用封装
│   │   ├── components/           # 组件
│   │   ├── router/
│   │   │   └── index.js          # 路由配置
│   │   ├── views/
│   │   │   ├── Dashboard.vue     # 监控面板
│   │   │   ├── RuleEditor.vue    # 规则编辑器
│   │   │   ├── AlertHistory.vue  # 告警历史
│   │   │   ├── RuleList.vue      # 规则列表
│   │   │   └── Settings.vue      # 系统设置
│   │   ├── App.vue               # 根组件
│   │   └── main.js               # 入口文件
│   ├── index.html                # HTML 入口
│   ├── vite.config.js            # Vite 配置
│   └── package.json
│
└── docs/
    └── CEP_DYNAMIC_LOADING_IMPLEMENTATION_PLAN.md  # 实现方案
```

---

## 🚀 快速开始

### 前置要求

- Java 17+
- Maven 3.6+
- Node.js 16+
- npm 或 yarn

### 1. 启动后端服务

```bash
# 进入 cep-api 目录
cd cep-api

# 编译项目
mvn clean install

# 运行应用
mvn spring-boot:run

# 或者打包后运行
mvn package
java -jar target/cep-api-1.0-SNAPSHOT.jar
```

后端服务将在 `http://localhost:8080` 启动

### 2. 启动前端服务

```bash
# 进入 cep-web 目录
cd cep-web

# 安装依赖
npm install

# 开发模式运行
npm run dev

# 或生产模式构建
npm run build
```

前端服务将在 `http://localhost:3000` 启动

---

## 📝 API 文档

### REST API 端点

| 方法 | 路径 | 描述 |
|------|------|------|
| GET | `/api/cep/rules` | 获取所有活跃规则 |
| POST | `/api/cep/rules` | 创建新规则 |
| PUT | `/api/cep/rules/{id}` | 更新现有规则 |
| DELETE | `/api/cep/rules/{id}` | 删除规则 |
| GET | `/api/cep/metrics` | 获取运行指标 |

### Swagger UI

访问 `http://localhost:8080/swagger-ui.html` 查看完整的 API 文档

---

## 🔧 配置说明

### 后端配置 (application.yml)

```yaml
server:
  port: 8080

cep:
  rule:
    storage-path: ./data/rules    # 规则存储路径
    auto-refresh-interval: 30     # 自动重载间隔（秒）
  metrics:
    collection-interval: 10       # 指标采集间隔（秒）
    max-records: 10000            # 最大保留记录数
```

### 前端配置 (vite.config.js)

```javascript
server: {
  port: 3000,
  proxy: {
    '/api': {
      target: 'http://localhost:8080',
      changeOrigin: true,
    },
  },
}
```

---

## 🎯 使用示例

### 1. 创建 CEP 规则

```json
{
  "ruleId": "price_spike_001",
  "ruleName": "价格异常波动检测",
  "patternJson": {
    "name": "price_spike",
    "times": null,
    "timesMin": null,
    "of": {
      "first": {
        "type": "simple",
        "condition": {
          "field": "price",
          "operator": ">",
          "value": 100
        }
      }
    },
    "where": {
      "type": "strict",
      "duration": 5,
      "unit": "minutes"
    }
  },
  "enabled": true
}
```

### 2. 发送告警请求

```bash
curl -X POST http://localhost:8080/api/cep/rules \
  -H "Content-Type: application/json" \
  -d @rule.json
```

### 3. WebSocket 连接

```javascript
import SockJS from 'sockjs-client'
import Stomp from 'stompjs'

const stompClient = Stomp.over(new SockJS('/ws'))

stompClient.connect({}, () => {
  // 订阅告警主题
  stompClient.subscribe('/topic/alerts', (message) => {
    const alert = JSON.parse(message.body)
    console.log('收到告警:', alert)
  })
})
```

---

## 📊 功能清单

### Phase 1: 核心引擎 ✅
- [x] Pattern 编译器
- [x] 规则加载器
- [x] 事件基类
- [x] JSON 解析支持

### Phase 2: REST API ✅
- [x] Spring Boot 集成
- [x] CRUD 接口
- [x] WebSocket 配置
- [x] 告警推送服务
- [x] Swagger 文档

### Phase 3: 前端页面 ✅
- [x] 监控面板 (Dashboard)
- [x] 规则编辑器 (RuleEditor)
- [x] 告警历史 (AlertHistory)
- [x] 路由配置
- [x] API 封装

### Phase 4: 待完成 ⏳
- [ ] 数据库持久化
- [ ] 用户认证授权
- [ ] 完整测试用例
- [ ] Docker 部署
- [ ] Kubernetes 配置

---

## 🐛 已知问题

1. **热更新限制**: 当前版本的热更新需要重建流处理拓扑，完全的热更新需要更多开发工作
2. **Pattern 类型**: 目前仅支持 SimpleCondition，复杂 Pattern 类型待添加
3. **告警通道**: 仅实现了 WebSocket 推送，邮件/Webhook 等通道待添加

---

## 📄 License

本项目为学习用途，仅供技术交流使用。

---

## 👥 贡献指南

欢迎提交 Issue 和 Pull Request！

---

## 📞 联系方式

如有问题，请通过 GitHub Issues 联系。

---

**Happy Coding!** 🎉
