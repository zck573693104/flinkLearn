# Vendored 前端依赖

内网/离线演示要求：页面不请求任何 CDN，三个库全部随 jar 打包。

| 文件 | 包与版本 | 来源 | 许可证 |
|---|---|---|---|
| `cytoscape.min.js` | cytoscape 3.32.1 | `npm pack cytoscape@3.32.1` → `dist/cytoscape.min.js` | MIT（`LICENSE-cytoscape.txt`） |
| `dagre.min.js` | dagre 0.8.5 | `npm pack dagre@0.8.5` → `dist/dagre.min.js`（含 graphlodash 与 pathjs 的全量包） | MIT（`LICENSE-dagre.txt`） |
| `cytoscape-dagre.js` | cytoscape-dagre 2.5.0 | `npm pack cytoscape-dagre@2.5.0` → `cytoscape-dagre.js`（未压缩 UMD，可读性优先） | MIT（`LICENSE-cytoscape-dagre.txt`） |

sha256（升级时替换本行，便于确认部署包没被改动）：

```
4bd40d3d94234e2b3a7fbb35a9c51d626d3d60b31eb04d27b6cb6bca27aa6478  cytoscape.min.js
62eb9787ccfdbdf4148d4d99d31dbf9ee4770eafee81e637d759b52aac22cd51  dagre.min.js
bf70fe402991dcbff33e05a7e4a5271c78020bb75e85d1c80ab7538e4157112e  cytoscape-dagre.js
```

加载顺序不能换：`cytoscape-dagre.js` 是 UMD，浏览器端从 `window.dagre` 取依赖，
所以必须是 `dagre.min.js` → `cytoscape.min.js` → `cytoscape-dagre.js`。
