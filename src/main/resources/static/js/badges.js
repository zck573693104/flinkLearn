/** 加工方式的中文提示：v1 不展开星号、只标置信度，不在前端重新推断语义。 */
const DERIVATION_HINT = {
  IDENTITY: '直通',
  EXPRESSION: '表达式',
  AGGREGATE: '聚合',
  CONSTANT: '常量',
  POSITIONAL: '按位置对齐',
  UNNAMED: '未命名列',
  STAR: '星号未展开',
  UNRESOLVED: '来源未绑定',
};

/**
 * 调色板只写在 app.css 的 :root 里，这里读同一份。
 *
 * 画布（Cytoscape）和 CSS 两边各存一份色值迟早会走样——图例说第 0 层是青色、
 * 节点却画成蓝色这类事故就是这么来的。读不到变量时退回内置值，不让样式表拖垮渲染。
 */
const FALLBACK = {
  layers: ['#4fd6c4', '#b6e34a', '#ffcb52', '#ff9a5a', '#f07bd0', '#8fa3b8'],
  nodeInk: '#04080e',
  edgeIdentity: '#7e93a8',
  edgeExpression: '#6aa7ff',
  edgeAggregate: '#ffc857',
  edgeConstant: '#b79ce0',
  edgePositional: '#5fd4c9',
  edgeUnnamed: '#e3cb90',
  edgeStar: '#ff9a5a',
  edgeUnresolved: '#ff5d6c',
  edgeDefault: '#38506b',
  nodeLine: '#33475d',
  localFill: '#111a25',
  localLine: '#5b7691',
  localText: '#9db2c8',
  boxFill: '#080d15',
  rowFill: '#141e2b',
  rowLine: '#26364a',
  rowText: '#dbe7f4',
  hot: '#cbf24a',
};

function token(name, fallback) {
  const value = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return value || fallback;
}

const LAYER_COLORS = FALLBACK.layers.map((color, index) =>
  token(`--layer-${index}`, color));

export const ink = {
  nodeText: token('--node-ink', FALLBACK.nodeInk),
  nodeLine: token('--node-line', FALLBACK.nodeLine),
  localFill: token('--local-fill', FALLBACK.localFill),
  localLine: token('--local-line', FALLBACK.localLine),
  localText: token('--local-text', FALLBACK.localText),
  boxFill: token('--box-fill', FALLBACK.boxFill),
  rowFill: token('--row-fill', FALLBACK.rowFill),
  rowLine: token('--row-line', FALLBACK.rowLine),
  rowText: token('--row-text', FALLBACK.rowText),
  hot: token('--acid', FALLBACK.hot),
  edgeDefault: token('--edge-default', FALLBACK.edgeDefault),
  /** 导出 PNG 的底色要跟屏上看的一致，不能再给一张白纸 */
  canvasBg: token('--ink-0', '#05070c'),
};

/**
 * 加工方式决定线的画法：低置信度的边必须在视觉上就和直通边区分开。
 *
 * 字段视图的线现在是 SVG（见 sqlflowView.js），所以这里给的就是描边口径：
 * 颜色、粗细、dash。图例从同一张表生成，屏上的线和图例块不可能各说各话。
 */
export const EDGE_STYLE = {
  IDENTITY: { color: token('--edge-identity', FALLBACK.edgeIdentity), width: 1.4, dash: '' },
  EXPRESSION: { color: token('--edge-expression', FALLBACK.edgeExpression), width: 1.6, dash: '' },
  AGGREGATE: { color: token('--edge-aggregate', FALLBACK.edgeAggregate), width: 2.2, dash: '' },
  CONSTANT: { color: token('--edge-constant', FALLBACK.edgeConstant), width: 1.6, dash: '6 4' },
  POSITIONAL: { color: token('--edge-positional', FALLBACK.edgePositional), width: 1.6, dash: '' },
  UNNAMED: { color: token('--edge-unnamed', FALLBACK.edgeUnnamed), width: 1.6, dash: '1 4' },
  STAR: { color: token('--edge-star', FALLBACK.edgeStar), width: 2, dash: '1 4' },
  UNRESOLVED: { color: token('--edge-unresolved', FALLBACK.edgeUnresolved), width: 2, dash: '6 4' },
};

/** 中间站那一跳是显示层的表达、不是解析出的事实：一律点线，不和真因果混在一起 */
export const SYNTHETIC_EDGE = { width: 1.3, dash: '2 4' };

/** 解析器新加一种加工方式而这张表还没跟上时，画默认色而不是画不出来 */
export function edgeInk(derivation) {
  return EDGE_STYLE[derivation] || { color: ink.edgeDefault, width: 1.5, dash: '' };
}

export function derivationLabel(derivation) {
  return DERIVATION_HINT[derivation] || derivation || '未知';
}

export function badge(text, cls) {
  const span = document.createElement('span');
  span.className = `badge ${cls || ''}`.trim();
  span.textContent = text;
  return span;
}

export function derivationBadge(derivation) {
  const span = badge(`${derivation || '?'} · ${derivationLabel(derivation)}`, derivation);
  if (derivation === 'UNRESOLVED') {
    span.title = '来源绑定失败：这条边的两端不保证真实存在，请配合 /api/issues 人工确认';
  }
  if (derivation === 'STAR') {
    span.title = 'v1 按约定不展开 SELECT *，字段级链路到此为止';
  }
  return span;
}

export function confidenceText(derivation, confidence) {
  const percent = Math.round((confidence || 0) * 100);
  return `${percent}%（${derivation || '?'}）`;
}

export function layerColor(layer) {
  const index = Number.isFinite(layer) ? Math.max(0, Math.round(layer)) : 0;
  return LAYER_COLORS[index % LAYER_COLORS.length];
}
