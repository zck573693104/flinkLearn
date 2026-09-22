# Vendored 前端依赖

内网/离线演示要求：页面不请求任何 CDN，也不请求任何字体，依赖随 jar 打包。

| 文件 | 包与版本 | 来源 | 许可证 |
|---|---|---|---|
| `cytoscape.min.js` | cytoscape 3.32.1 | `npm pack cytoscape@3.32.1` → `dist/cytoscape.min.js` | MIT（`LICENSE-cytoscape.txt`） |

sha256（升级时替换本行，便于确认部署包没被改动）：

```
4bd40d3d94234e2b3a7fbb35a9c51d626d3d60b31eb04d27b6cb6bca27aa6478  cytoscape.min.js
```

## dagre 已经下线

图布局不再依赖 `dagre` / `cytoscape-dagre`，改由 `js/graph.js` 的 `runLayout()` 自己摆坐标：
服务端 `LayeredDagBuilder` 下发的 `layer` 就是权威分层，直接拿它当列号，比让 dagre 再按连通分量
重排一次更贴合「第几层」图例的语义。被删的四份文件（两个 js、两个 LICENSE 及其 sha256 行）
在 git 历史里可查。

加载顺序也因此不再有约束：`index.html` 里只有 `cytoscape.min.js` 一处 script 标签，
且必须在各 ES module 之前——模块用 `import` 拿不到 `window.cytoscape`。
