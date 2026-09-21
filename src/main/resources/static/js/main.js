/**
 * 三栏血缘 UI 的装配层：状态机 + 事件绑定。
 *
 * 这里不直接 fetch（全部走 api.js），也不画节点（走 graphTable/graphColumn），
 * 只负责"点了一下之后该重新取哪个接口、右栏显示什么"。
 */
import { api } from './api.js';
import { create, fit } from './graph.js';
import { drawColumns, markColumn } from './graphColumn.js';
import { drawTable } from './graphTable.js';
import { layerColor } from './badges.js';
import {
  renderColumn,
  renderEdge,
  renderEmpty,
  renderError,
  renderParse,
  renderTable,
  renderTableEdge,
} from './detailPanel.js';

const ui = {
  graph: document.getElementById('graph'),
  detail: document.getElementById('detail'),
  title: document.getElementById('graph-title'),
  warn: document.getElementById('graph-warn'),
  depth: document.getElementById('depth'),
  direction: document.getElementById('direction'),
  overview: document.getElementById('overview'),
  search: document.getElementById('search'),
  layerFilter: document.getElementById('layer-filter'),
  onlyUnresolved: document.getElementById('only-unresolved'),
  tableList: document.getElementById('table-list'),
  tableNote: document.getElementById('table-note'),
  issues: document.getElementById('issues'),
  parseSql: document.getElementById('parse-sql'),
  parseOut: document.getElementById('parse-out'),
  legend: document.getElementById('legend'),
  scanError: document.getElementById('scan-error'),
  scanDir: document.getElementById('scan-dir'),
};

const state = {
  view: 'table',
  table: null,
  column: null,
  /** 当前表的字段清单：点字段节点时优先复用，省一次往返 */
  columns: null,
  /** 最近一次下发的子图 JSON，导出与调试用 */
  payload: null,
  /** /api/overview 给的最大层号，图例按它画色块 */
  maxLayer: 0,
};

const cy = create(ui.graph);

function fail(error) {
  console.error(error);
  ui.warn.textContent = error.message || String(error);
  renderError(ui.detail, error);
}

function depth() {
  return ui.depth.value || '';
}

function direction() {
  return state.table ? ui.direction.value : '';
}

/**
 * 图里的列标识是 {@code table.column}，而 /api/graph/column 的 column 参数是裸列名。
 * 先按已知表名剥前缀：伪关系标识（[job]#unnest.col）也走这条路，不靠末段猜测。
 */
function columnNameOf(columnId, table) {
  const prefix = `${table}.`;
  return columnId.startsWith(prefix)
    ? columnId.slice(prefix.length)
    : columnId.slice(columnId.lastIndexOf('.') + 1);
}

/* ---------------- 顶栏 ---------------- */

async function loadOverview() {
  const data = await api.overview();
  state.maxLayer = data.maxLayer;
  ui.overview.textContent = '';
  [
    ['语料', `${data.fileCount} 文件 / ${data.statementCount} 语句`],
    ['表', `${data.tableCount}（${data.tableCountWithColumns} 张有字段）`],
    ['字段边', data.columnEdgeCount],
    ['表边', data.tableEdgeCount],
    ['层级', data.maxLayer],
    ['中间关系', data.localRelationCount],
    ['质量', `错 ${data.parseErrorCount} · 未解析 ${data.unresolvedCount} · 星号 ${data.starCount}`],
    ['耗时', `${data.durationMillis}ms`],
  ].forEach(([label, value]) => {
    const div = document.createElement('div');
    const dt = document.createElement('dt');
    dt.textContent = `${label} `;
    const dd = document.createElement('dd');
    dd.textContent = value;
    div.append(dt, dd);
    ui.overview.appendChild(div);
  });
  fillLayerFilter(data.maxLayer);
  showScanError(data);
  return data;
}

/**
 * 顶栏下面的告警条。空快照不会自己解释自己，而这几种"什么都没有"必须分清：
 * 还在扫（正常，等就好）、扫失败了（目录指错了）、扫到 0 个文件（目录对但没语料）、压根没扫。
 */
function showScanError(data) {
  const hint = '相对路径按服务进程的工作目录解析：可在顶栏「语料目录」填绝对路径再点「重扫目录」，'
    + '或用 --lineage.scan-dir=<绝对路径> 起服务。';
  let text;
  let info = false;
  if (data.scanError) {
    text = `启动时没扫成：${data.scanError}。${hint}`;
  } else if (data.scanPhase === 'running') {
    info = true;
    text = '语料还在扫（端口先于扫描就绪，属正常），扫完自动出图。';
  } else if (data.scanPhase === 'skipped') {
    info = true;
    text = '已按 lineage.rescan-on-start=false 跳过启动扫描，在顶栏「语料目录」填好路径再点「重扫目录」取数。';
  } else if (!data.fileCount) {
    text = `${data.source || '配置的目录'} 里没有 .sql 文件。${hint}`;
  } else {
    text = '';
  }
  ui.scanError.hidden = !text;
  ui.scanError.textContent = text;
  ui.scanError.classList.toggle('info', info);
}

/** 扫描没落地前不要把整页当"没有血缘"处理：每 500ms 问一次，最多 60s */
async function waitScan(data) {
  let latest = data;
  for (let i = 0; i < 120 && latest.scanPhase === 'running'; i += 1) {
    await new Promise((resolve) => setTimeout(resolve, 500));
    latest = await loadOverview();
  }
  if (latest.scanPhase === 'running') {
    ui.scanError.textContent = '语料还没扫完（已等 60 秒），扫完刷新页面或点「重扫目录」。';
  }
  return latest;
}

function fillLayerFilter(maxLayer) {
  const current = ui.layerFilter.value;
  ui.layerFilter.textContent = '';
  const all = document.createElement('option');
  all.value = '';
  all.textContent = '全部';
  ui.layerFilter.appendChild(all);
  for (let layer = 0; layer <= maxLayer; layer++) {
    const option = document.createElement('option');
    option.value = String(layer);
    option.textContent = `第 ${layer} 层`;
    ui.layerFilter.appendChild(option);
  }
  ui.layerFilter.value = current;
}

/* ---------------- 左栏 ---------------- */

async function loadTables() {
  const rows = await api.tables({
    q: ui.search.value.trim(),
    layer: ui.layerFilter.value,
    onlyUnresolved: ui.onlyUnresolved.checked,
  });
  ui.tableList.textContent = '';
  rows.forEach((row) => {
    const li = document.createElement('li');
    if (row.id === state.table) {
      li.className = 'active';
    }
    const name = document.createElement('span');
    name.className = 'name';
    name.textContent = row.db ? `${row.db}.${row.name}` : row.name;
    const meta = document.createElement('span');
    meta.className = 'meta';
    meta.textContent = `层 ${row.layer} · ↑${row.upstream} ↓${row.downstream} · ${row.colCount} 字段`;
    li.dataset.id = row.id;
    li.append(name, meta);
    li.addEventListener('click', guard(() => selectTable(row.id)));
    ui.tableList.appendChild(li);
  });
  ui.tableNote.textContent = rows.length ? `${rows.length} 张表` : '没有符合条件的表';
}

async function loadIssues() {
  const data = await api.issues();
  ui.issues.textContent = '';
  [
    ['语法错误 SQL', data.parseErrors],
    ['来源未绑定的边', data.unresolvedEdges],
    ['星号边（v1 不展开）', data.starEdges],
    ['没出血缘的文件', data.filesWithoutLineage],
    ['孤岛表（无上下游）', data.orphanTables],
  ].forEach(([label, items]) => {
    const li = document.createElement('li');
    if (!items.length) {
      li.className = 'clean';
    }
    const head = document.createElement('h3');
    head.textContent = `${label}：${items.length}`;
    li.appendChild(head);
    if (items.length) {
      const ul = document.createElement('ul');
      items.forEach((item) => {
        const sub = document.createElement('li');
        sub.textContent = item;
        ul.appendChild(sub);
      });
      li.appendChild(ul);
    }
    ui.issues.appendChild(li);
  });
}

async function runParse() {
  ui.parseOut.textContent = '';
  const sql = ui.parseSql.value;
  if (!sql.trim()) {
    renderEmpty(ui.parseOut, '先粘一段 SQL。');
    return;
  }
  try {
    renderParse(ui.parseOut, await api.parse(sql));
  } catch (error) {
    renderError(ui.parseOut, error);
  }
}

/* ---------------- 中栏 ---------------- */

async function drawTableGraph() {
  const view = await api.tableGraph({ root: state.table, direction: direction(), depth: depth() });
  state.payload = view;
  const result = drawTable(cy, view, {
    onTable: guard(selectTable),
    onTableEdge: guard((data) => renderTableEdge(ui.detail, data)),
    onBlank: guard(resetSelection),
  });
  ui.warn.textContent = result.warning;
  ui.title.textContent = state.table
    ? `表级链路：${state.table}（${ui.direction.options[ui.direction.selectedIndex].text}，深度 ${ui.depth.value}）`
    : `全量表级 DAG（${result.nodeCount} 表 / ${result.edgeCount} 边）`;
  markSelected();
  drawLegend('table');
}

async function drawColumnGraph() {
  if (!state.table) {
    ui.warn.textContent = '';
    ui.title.textContent = '先选一张表再看字段链路';
    cy.elements().remove();
    drawLegend('column');
    return;
  }
  const view = await api.columnGraph({
    table: state.table,
    column: state.column ? columnNameOf(state.column, state.table) : '',
    depth: depth(),
  });
  state.payload = view;
  const result = drawColumns(cy, view, {
    onColumn: guard((id, table) => selectColumn(id, table)),
    onEdge: guard((data) => showEdge(data.source, data.target)),
  });
  ui.warn.textContent = result.warning;
  ui.title.textContent = `${state.column || `${state.table} 全部字段`} 的字段链路（${result.nodeCount} 节点 / ${result.edgeCount} 边）`;
  markColumn(cy, state.column);
  drawLegend('column');
}

function markSelected() {
  cy.elements().removeClass('hot');
  if (state.table) {
    const node = cy.getElementById(state.table);
    if (node.nonempty()) {
      node.addClass('hot');
    }
  }
}

function drawLegend(kind) {
  ui.legend.textContent = '';
  const swatch = (color, label) => {
    const span = document.createElement('span');
    const box = document.createElement('i');
    box.style.background = color;
    span.append(box, document.createTextNode(label));
    ui.legend.appendChild(span);
  };
  const note = (text) => {
    const span = document.createElement('span');
    span.textContent = text;
    ui.legend.appendChild(span);
  };
  if (kind === 'table') {
    for (let layer = 0; layer <= state.maxLayer; layer++) {
      swatch(layerColor(layer), `第 ${layer} 层`);
    }
    note('点表节点：以它为中心裁剪，并在右栏看字段清单');
    return;
  }
  [
    ['#7d8794', 'IDENTITY 直通'],
    ['#5b7fd4', 'EXPRESSION 表达式'],
    ['#c08a2e', 'AGGREGATE 聚合'],
    ['#a89bc4', 'CONSTANT 常量'],
    ['#6fa8ae', 'POSITIONAL 按位置'],
    ['#e0a870', 'STAR 星号（v1 不展开）'],
    ['#e07a70', 'UNRESOLVED 来源未绑定'],
  ].forEach(([color, label]) => swatch(color, label));
  note('灰底虚线框 = 语句内中间关系（CTE/子查询），已折进边的 hops');
}

/* ---------------- 选区联动 ---------------- */

/** 按选区重画中栏 + 填右栏：换视图、换深度、点节点、启动都只走这一条路径 */
async function refresh() {
  if (state.view === 'table') {
    await drawTableGraph();
  } else {
    await drawColumnGraph();
  }
  if (state.column) {
    await showColumnDetail(state.column, state.table);
  } else if (state.table) {
    await showTableDetail(state.table);
  } else {
    renderEmpty(ui.detail, state.view === 'column'
      ? '先在表级视图点一张表，再进字段链路'
      : '点图中的表节点看字段清单，点字段边看逐跳来源证据。');
  }
}

async function showTableDetail(tableId) {
  const data = await api.columns(tableId);
  state.columns = data;
  renderTable(ui.detail, data, {
    onColumn: (column) => openColumnView(data.table, column.id),
    onWholeTable: (table) => openColumnView(table, null),
  });
}

async function showColumnDetail(columnId, table) {
  const owner = state.columns && state.columns.table === table
    ? state.columns
    : await api.columns(table);
  state.columns = owner;
  const column = (owner.columns || []).find((row) => row.id === columnId);
  if (column) {
    renderColumn(ui.detail, owner, column);
  } else {
    renderEmpty(ui.detail, `${columnId} 没有出现在字段清单里：它可能是被折掉的中间关系字段`);
  }
}

async function selectTable(tableId) {
  state.table = tableId;
  state.column = null;
  state.columns = null;
  syncHash();
  await refresh();
  await markActiveRow();
}

async function selectColumn(columnId, table) {
  state.table = table;
  state.column = columnId;
  syncHash();
  await refresh();
  await markActiveRow();
}

/** 左栏高亮跟着选区走：搜索/过滤之后列表会重画，所以复用同一次取数 */
async function markActiveRow() {
  ui.tableList.querySelectorAll('li').forEach((row) => {
    row.classList.toggle('active', row.dataset.id === state.table);
  });
}

async function showEdge(from, to) {
  try {
    renderEdge(ui.detail, await api.edge(from, to), `${from} → ${to}`);
  } catch (error) {
    fail(error);
  }
}

function openColumnView(table, column) {
  switchView('column');
  state.table = table;
  state.column = column;
  syncHash();
  refresh().catch(fail);
}

function switchView(view) {
  state.view = view;
  document.querySelectorAll('#view-tabs button').forEach((button) => {
    button.classList.toggle('active', button.dataset.view === view);
  });
}

/** 事件回调统一兜底：取数失败只写右栏提示，不让异常飘成 console 噪音 */
function guard(fn) {
  return (...args) => {
    Promise.resolve(fn(...args)).catch(fail);
  };
}

async function resetSelection() {
  state.table = null;
  state.column = null;
  state.columns = null;
  syncHash();
  await refresh();
  await markActiveRow();
}

/* ---------------- 导出与定位 ---------------- */

function download(name, href) {
  const link = document.createElement('a');
  link.download = name;
  link.href = href;
  link.click();
}

function exportPng() {
  if (!cy.nodes().length) {
    ui.warn.textContent = '当前没有可导出的图';
    return;
  }
  download(`lineage-${state.view}-${Date.now()}.png`,
    cy.png({ full: true, scale: 2, bg: '#ffffff' }));
}

function exportJson() {
  if (!state.payload) {
    ui.warn.textContent = '当前没有可导出的子图';
    return;
  }
  const blob = new Blob([JSON.stringify({
    view: state.view,
    table: state.table,
    column: state.column,
    graph: state.payload,
  }, null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  download(`lineage-${state.view}-${Date.now()}.json`, url);
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

function syncHash() {
  const params = new URLSearchParams();
  params.set('view', state.view);
  if (state.table) {
    params.set('table', state.table);
  }
  if (state.column) {
    params.set('column', state.column);
  }
  const next = `#${params}`;
  if (location.hash !== next) {
    history.replaceState(null, '', next);
  }
}

function readHash() {
  const params = new URLSearchParams(location.hash.replace(/^#/, ''));
  switchView(params.get('view') === 'column' ? 'column' : 'table');
  state.table = params.get('table') || null;
  state.column = params.get('column') || null;
}

/* ---------------- 绑定 ---------------- */

function debounce(fn, wait) {
  let timer = null;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), wait);
  };
}

document.querySelectorAll('#left-tabs button').forEach((button) => {
  button.addEventListener('click', () => {
    document.querySelectorAll('#left-tabs button').forEach((other) => {
      other.classList.toggle('active', other === button);
    });
    ['tables', 'issues', 'parse'].forEach((name) => {
      document.getElementById(`panel-${name}`)
        .classList.toggle('hidden', name !== button.dataset.tab);
    });
    if (button.dataset.tab === 'issues' && !ui.issues.childNodes.length) {
      loadIssues().catch(fail);
    }
  });
});

document.querySelectorAll('#view-tabs button').forEach((button) => {
  button.addEventListener('click', () => {
    switchView(button.dataset.view);
    syncHash();
    refresh().catch(fail);
  });
});

ui.search.addEventListener('input', debounce(guard(loadTables), 250));
ui.layerFilter.addEventListener('change', guard(loadTables));
ui.onlyUnresolved.addEventListener('change', guard(loadTables));
ui.depth.addEventListener('change', guard(refresh));
ui.direction.addEventListener('change', guard(() => {
  if (state.view === 'table') {
    drawTableGraph();
  }
}));
document.getElementById('rescan').addEventListener('click', guard(async () => {
  const dir = ui.scanDir.value.trim();
  ui.warn.textContent = `正在重扫${dir || '当前'}目录…`;
  const data = await api.scan(dir);
  state.table = null;
  state.column = null;
  state.columns = null;
  syncHash();
  await loadOverview();
  await loadTables();
  await refresh();
  ui.warn.textContent = '';
  renderEmpty(ui.detail, `重扫完成：${data.source} → ${data.fileCount} 文件 / `
    + `${data.statementCount} 条语句 / ${data.columnEdgeCount} 条字段边，用时 ${data.durationMillis}ms`);
}));
document.getElementById('export-png').addEventListener('click', exportPng);
document.getElementById('export-json').addEventListener('click', exportJson);
document.getElementById('parse-run').addEventListener('click', runParse);

/** 只认外部改 hash 的情况：自己 replaceState 不会再触发一次重画 */
window.addEventListener('hashchange', guard(async () => {
  readHash();
  await refresh();
}));

/* ---------------- 启动 ---------------- */

(async function start() {
  readHash();
  await waitScan(await loadOverview());
  await loadTables();
  if (state.column && !state.table) {
    const dot = state.column.lastIndexOf('.');
    state.table = dot < 0 ? null : state.column.slice(0, dot);
  }
  await refresh();
  await markActiveRow();
  fit(cy);
}().catch(fail));
