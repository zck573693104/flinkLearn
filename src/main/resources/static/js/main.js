/**
 * 三栏血缘 UI 的装配层：状态机 + 事件绑定。
 *
 * 这里不直接 fetch（全部走 api.js），也不画节点（走 graphTable/sqlflowView），
 * 只负责"点了一下之后该重新取哪个接口、右栏显示什么"。
 */
import { api } from './api.js';
import { create, setHot, watchResize } from './graph.js';
import { drawTable } from './graphTable.js';
import { parsedTableDetail } from './parsed.js';
import {
  columnCard,
  entityCard,
  evidenceOfEdge,
  indexSqlFlow,
  rowIdOfColumnId,
} from './sqlflowModel.js';
import { createPop } from './sqlflowPop.js';
import { paintSqlFlow } from './sqlflowView.js';
import { EDGE_STYLE, derivationLabel, ink, layerColor } from './badges.js';
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
  pop: document.getElementById('sqlflow-pop'),
  scanError: document.getElementById('scan-error'),
  scanDir: document.getElementById('scan-dir'),
  status: document.getElementById('status'),
  mark: document.querySelector('.brand .mark'),
  tabParsedTable: document.getElementById('tab-parsed-table'),
  tabParsedColumn: document.getElementById('tab-parsed-column'),
};

const state = {
  view: 'table',
  table: null,
  column: null,
  /** 最近一次下发的子图 JSON，导出与调试用 */
  payload: null,
  /** /api/overview 给的最大层号，图例按它画色块 */
  maxLayer: 0,
  /** 最近一次 /api/parse 的结果（含用户原文），临时视图的唯一数据源 */
  parse: null,
  /** 临时视图里选中的字段，切过去时用来直接填右栏 */
  parsedColumn: null,
  /** 这一次画图本身要交代的退化说明（多少列没画、多少段链路没显示） */
  modelWarning: '',
  /** 字段视图这一份响应：{index, view}，右栏与浮层的证据都从这里查，不再打第二个端点 */
  sqlflow: null,
  /** 浮层现在贴在哪个字段行上：{rowId, columnId, entityId} */
  popTarget: null,
};

const cy = create(ui.graph);

const pop = createPop(ui.pop, {
  /** "只看这一列"是纯客户端裁剪：画布上已有的边就是这次请求的全部结论 */
  chain: guard(() => {
    if (state.popTarget && state.sqlflow) {
      state.sqlflow.view.crop(state.popTarget.rowId);
      showPop(state.popTarget.rowId);
    }
  }),
  columns: guard(() => {
    if (state.popTarget && state.sqlflow) {
      listEntity(state.popTarget.entityId);
    }
  }),
  reset: guard(() => {
    if (state.sqlflow) {
      showWarning(state.sqlflow.view.reset());
    }
    pop.hide();
  }),
});

/* 浮层跟着画布走：pan/zoom 之后原来的屏幕坐标就作废了 */
cy.on('pan zoom drag', () => {
  if (pop.visible() && state.popTarget) {
    placePop(state.popTarget.rowId);
  }
});

/* 取数在 await 处会让出线程，慢的那次不能盖掉后点的：画之前比序号，过期就收手。 */
let renderSeq = 0;

/**
 * 告警条由两半拼成：fit 的"画布放不下"每次重新 framing 都要重算，
 * 图自己的"N 列未画"只在画图那一次产生。
 *
 * 两半必须分开存：以前 resize 回调直接整条覆盖，结果容器一变宽、
 * fit 说"放得下了"，就把"1540 列未画"的交代一起抹成空条——
 * 画布明明只画了 28 行，页面却一句话都不说。
 */
function showWarning(result) {
  /*
   * 裁剪是画布当前的状态，不是图本身的说明：浮层收起后（点画布空白）用户只剩一堆重排过的盒子，
   * 标题却还写着"全部字段"。所以 cropped 必须跟着进这一行，复位后自然消失。
   */
  state.modelWarning = [result.modelWarning,
    result.cropped ? '当前只画选中字段的通路（点浮层「复位」还原整片）' : ''].filter(Boolean).join('；');
  paintFraming(result.framing);
}

/** 重新 framing：图本身的说明不变，只换"放放不下"那一半 */
function paintFraming(framing) {
  ui.warn.textContent = [framing, state.modelWarning].filter(Boolean).join('；');
}

/** 与图无关的一次性话（报错、重扫进度）：整条让给它，别和上一张图的说明混在一起 */
function setWarn(text) {
  state.modelWarning = '';
  ui.warn.textContent = text || '';
}

watchResize(cy, paintFraming);

function fail(error) {
  console.error(error);
  setWarn(error.message || String(error));
  renderError(ui.detail, error);
}

function depth() {
  return ui.depth.value || '';
}

function direction() {
  return state.table ? ui.direction.value : '';
}

/* ---------------- 顶栏 ---------------- */

async function loadOverview() {
  const data = await api.overview();
  state.maxLayer = data.maxLayer;
  ui.overview.textContent = '';
  const dirty = data.parseErrorCount + data.unresolvedCount + data.starCount;
  [
    ['语料', `${data.fileCount} 文件 / ${data.statementCount} 语句`],
    ['表', `${data.tableCount}（${data.tableCountWithColumns} 张有字段）`],
    ['字段边', data.columnEdgeCount],
    ['表边', data.tableEdgeCount],
    ['层级', data.maxLayer],
    ['中间关系', data.localRelationCount],
    ['质量', `错 ${data.parseErrorCount} · 未解析 ${data.unresolvedCount} · 星号 ${data.starCount}`, dirty ? 'is-warn' : 'is-ok'],
    ['耗时', `${data.durationMillis}ms`],
  ].forEach(([label, value, cls], index) => {
    const div = document.createElement('div');
    if (cls) {
      div.className = cls;
    }
    // 入场错峰只给一次：顶栏是每次取数都重画的，逐条 delay 才有"仪器启动"的味道
    div.style.setProperty('--i', index);
    const dt = document.createElement('dt');
    dt.textContent = `${label} `;
    const dd = document.createElement('dd');
    dd.textContent = value;
    div.append(dt, dd);
    ui.overview.appendChild(div);
  });
  renderStatus(data);
  fillLayerFilter(data.maxLayer);
  showScanError(data);
  return data;
}

/** 顶栏状态位：这一屏数据是从哪儿、什么时候、以什么相位来的 */
function renderStatus(data) {
  ui.status.textContent = '';
  const src = document.createElement('b');
  src.textContent = data.source || '未配置目录';
  const sep = document.createElement('span');
  sep.className = 'sep';
  sep.textContent = '·';
  const phase = document.createElement('span');
  phase.textContent = data.scanError ? '启动扫描失败'
    : data.scanPhase === 'running' ? '扫描中…'
    : data.scanPhase === 'skipped' ? '已跳过启动扫描'
    : `就绪 · ${data.durationMillis}ms`;
  ui.status.append(src, sep, phase);
  ui.mark.classList.toggle('live', data.scanPhase === 'running' || !!data.scanError);
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
    // 层号色板：左栏列表和图共用同一份 layerColor，扫一眼就知道这张表在第几跳
    const swatch = document.createElement('i');
    swatch.className = 'dot';
    swatch.style.background = layerColor(row.layer);
    swatch.style.color = layerColor(row.layer);
    meta.append(swatch, document.createTextNode(`第 ${row.layer} 层 · ↑${row.upstream} ↓${row.downstream} · ${row.colCount} 字段`));
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
  const seq = ++renderSeq;
  ui.parseOut.textContent = '';
  const sql = ui.parseSql.value;
  if (!sql.trim()) {
    renderEmpty(ui.parseOut, '先粘一段 SQL。');
    return;
  }
  try {
    const data = await api.parse(sql);
    if (seq !== renderSeq) {
      return;
    }
    data.rawSql = sql;
    state.parse = data;
    state.parsedColumn = null;
    markParsedTabs();
    renderParse(ui.parseOut, data);
    // 解析完直接落到临时表级视图：用户粘 SQL 的下一步一定是"图在哪儿"
    switchView('parsed-table');
    await refresh();
  } catch (error) {
    renderError(ui.parseOut, error);
  }
}

function markParsedTabs() {
  ui.tabParsedTable.disabled = !state.parse;
  ui.tabParsedColumn.disabled = !state.parse;
}

/* ---------------- 中栏 ---------------- */

function maxLayerOf(view) {
  return (view.nodes || []).reduce((max, node) => Math.max(max, node.layer || 0), 0);
}

/** 标题一行放不下就省略号截断，全文同时挂到 title 上，hover 能看回来 */
function setGraphTitle(text) {
  ui.title.textContent = text;
  ui.title.title = text;
}

/**
 * 换到表级画法之前，把字段级留下的浮层与裁剪句柄一起清掉。
 *
 * 浮层是挂在 .canvas 上的 DOM，不随 cytoscape 的元素消失；留着它而 state.sqlflow.view
 * 还指向上一份图，点「只看这一列」就会拿旧行号去裁新图——新图上谁也找不到，
 * 于是整张画布被 display:none 掉（实测踩过：白屏 + 一句过期的告警）。
 */
function dropSqlFlow() {
  state.sqlflow = null;
  state.popTarget = null;
  pop.hide();
}

async function drawTableGraph(seq) {
  const view = await api.tableGraph({ root: state.table, direction: direction(), depth: depth() });
  if (seq !== renderSeq) {
    return;
  }
  state.payload = view;
  dropSqlFlow();
  // 图例和标题都在画图之前落 DOM：它们是 #graph 的兄弟节点，晚写一步就把画布压矮，
  // 而 fit 是按当时的容器尺寸算缩放的——实测过 fit 完 516x288、图例渲染后只剩 501x202。
  setGraphTitle(state.table
    ? `表级链路：${state.table}（${ui.direction.options[ui.direction.selectedIndex].text}，深度 ${ui.depth.value}）`
    : `全量表级 DAG（${(view.nodes || []).length} 表 / ${(view.edges || []).length} 边）`);
  drawLegend('table', state.maxLayer);
  const result = drawTable(cy, view, {
    onTable: guard(selectTable),
    onTableEdge: guard((data) => renderTableEdge(ui.detail, data)),
    onBlank: guard(resetSelection),
  });
  // 有节点就以"最后一次 framing"为准；一个都没画（超上限）时只剩超限告警可报。
  showWarning(result);
  setHot(cy, state.table);
}

/** 临时解析视图：数据只来自最近一次 /api/parse，全程不再打快照端点 */
async function drawParsedTable() {
  const view = state.parse.graph;
  state.payload = view;
  dropSqlFlow();
  setGraphTitle(`试解析·表级（未入快照，${(view.nodes || []).length} 表 / ${(view.edges || []).length} 边）`);
  drawLegend('table', maxLayerOf(view), '这张图来自输入框里的 SQL，重扫目录或换表即失效');
  const result = drawTable(cy, view, {
    onTable: guard(showParsedTable),
    onTableEdge: guard((data) => renderTableEdge(ui.detail, data)),
    onBlank: guard(() => renderEmpty(ui.detail, '临时解析视图：点表节点看它在这次解析里的字段清单。')),
  });
  showWarning(result);
}

async function drawParsedColumns(seq) {
  const data = await api.sqlflowParse(state.parse.rawSql, state.parsedColumn);
  if (seq !== renderSeq) {
    return;
  }
  state.payload = data;
  showSqlFlow(data, '试解析·字段（未入快照）', state.parsedColumn);
}

async function drawColumnGraph(seq) {
  if (!state.table) {
    setWarn('');
    setGraphTitle('先选一张表再看字段链路');
    dropSqlFlow();
    cy.elements().remove();
    drawLegend('column', state.maxLayer);
    return;
  }
  /*
   * 选中的那一列未必属于 state.table：点邻居盒里的一行只改 state.column（这片邻域仍以选中的
   * 那张表为中心）。把"表 X + Y 的列名"原样打过去只能得到一个在 X 里不存在的列，所以不同源时
   * 交完整标识给 focus：邻域照 X 裁，焦点保那一列。
   */
  const picked = state.column || '';
  const prefix = `${state.table}.`;
  const sameTable = picked.startsWith(prefix);
  const data = await api.sqlflowGraph({
    table: state.table,
    column: sameTable ? picked.slice(prefix.length) : '',
    focus: sameTable ? '' : picked,
    depth: depth(),
  });
  if (seq !== renderSeq) {
    return;
  }
  state.payload = data;
  showSqlFlow(data, `${state.column || `${state.table} 全部字段`} 的字段链路`, state.column);
}

/**
 * 一份响应画一张图：快照的字段视图和试解析的字段视图走同一条路径。
 *
 * focus 是列的完整标识（table.column）：服务端按它裁邻域，前端再把"只留这一列的通路"落到实处——
 * 这就是浮层第一个按钮的结论，选中了字段就先进来。
 */
function showSqlFlow(data, heading, focus) {
  const meta = data.metaInfo || {};
  const summary = data.summary || {};
  state.sqlflow = { index: indexSqlFlow(data), view: null };
  state.popTarget = null;
  pop.hide();
  /*
   * 标题报的必须是画布上真有的东西：超上限时服务端只给模型不给几何（tables 是空的），
   * 这时候照念 metaInfo.boxCount 就是"602 表盒"配一张白画布。
   */
  setGraphTitle(meta.drawn === false ? `${heading}（没画：只给数据模型，见下方说明）`
    : `${heading}（${meta.boxCount || 0} 表盒 / ${meta.rowCount || 0} 字段行 / `
      + `${summary.relationship || 0} 条关系）`);
  drawLegend('column', state.maxLayer);
  const view = paintSqlFlow(cy, data, sqlflowHandlers());
  state.sqlflow.view = view;
  showWarning(view);
  const rowId = focus ? rowIdOfColumnId(state.sqlflow.index, focus) : null;
  if (rowId) {
    view.crop(rowId);
    focusRow(rowId);
    showPop(rowId);
  }
}

/**
 * 浮层要跟的是"那一行的完整身份"，不只是图上标识：
 * 三个动作各读一个字段（裁剪读 rowId、列清单读 entityId、标题读 columnId），
 * 少填一个就在 placePop 里空指针。
 */
function focusRow(rowId) {
  const row = state.sqlflow.index.rows.get(rowId) || {};
  const column = state.sqlflow.index.columns.get(row.modelId) || {};
  state.popTarget = {
    rowId,
    columnId: column.qualifiedName || rowId,
    entityId: column.entityId,
  };
}

function sqlflowHandlers() {
  return {
    onRow: guard((rowId, data) => pickRow(rowId, data.modelId)),
    onBox: guard((boxId, data) => pickBox(data)),
    onEdge: guard((edgeId) => {
      pop.hide();
      const rows = evidenceOfEdge(state.sqlflow.index, edgeId);
      if (rows.length) {
        renderEdge(ui.detail, rows, `${rows[0].from} → ${rows[0].to}`);
      } else {
        renderEmpty(ui.detail, '这条线背后没有解析出的关系：它是中间站之间的一跳，点两端的字段行看证据。');
      }
    }),
    onBlank: guard(() => pop.hide()),
  };
}

/**
 * 点一行 = 全部在本地完成：换右栏证据、描高亮、把浮层贴上去。
 *
 * 不打请求：这次请求给的就是这一片的全部结论，再打一次只会拿到同一份。
 */
function pickRow(rowId, columnModelId) {
  const bag = state.sqlflow;
  const card = columnCard(bag.index, columnModelId);
  if (!card) {
    renderEmpty(ui.detail, '这一列没在数据模型里登记：多半是超出每盒 200 列的登记上限');
    return;
  }
  if (state.view === 'parsed-column') {
    // 试解析的选区记在 parsedColumn 上：state.column 是快照那套的坐标，串了台
    state.parsedColumn = card.id;
  } else {
    state.column = card.id;
    syncHash();
  }
  focusRow(rowId);
  setHot(cy, rowId);
  renderColumn(ui.detail, entityCard(bag.index, card.entityId), card);
  showPop(rowId);
}

/** 浮层贴回它跟着的那一行：裁剪会重排，行号一变位置就得重算 */
function showPop(rowId) {
  showWarning(state.sqlflow.view);
  placePop(rowId);
}

function placePop(rowId) {
  const node = cy.getElementById(rowId);
  if (!node.nonempty()) {
    pop.hide();
    return;
  }
  const at = node.renderedPosition();
  const target = state.popTarget;
  pop.show(at.x, at.y, `${target.columnId}（点动作条外任意处可收起）`);
}

/**
 * 点盒顶的表名 = 以那张表为中心重新展开；中间站没在库里，只列它的字段。
 *
 * 语句内关系（CTE/子查询/函数盒）不是能按名字寻址的表，打过去只能得到 400，
 * 所以它不进选表这条路。
 */
function pickBox(data) {
  pop.hide();
  if (state.view === 'parsed-column') {
    // 试解析只认字段 focus，没有"以某表为中心重新取一片"这条路；
    // 这份响应本来就是整段 SQL 的结论，就地列字段比打快照端点诚实。
    listEntity(data.modelId);
    return;
  }
  if (!data.local && data.table) {
    openColumnView(data.table, null);
    return;
  }
  listEntity(data.modelId);
}

/** "列出字段"：模型登记了什么就列什么，没画进盒子的那几行标出来 */
function listEntity(entityModelId) {
  const bag = state.sqlflow;
  const card = entityCard(bag.index, entityModelId);
  if (!card) {
    renderEmpty(ui.detail, '这个盒子里没有可列的字段');
    return;
  }
  const parsed = state.view === 'parsed-column';
  renderTable(ui.detail, card, {
    onColumn: guard((column) => openListedColumn(card, column)),
    onWholeTable: guard(() => (parsed
      ? openParsedColumn(null)
      : openColumnView(card.table, null))),
  });
}

/**
 * 清单里点一列：画得出来的就地裁剪，画不出来的才换一次中心重新取。
 *
 * 没画进盒子的列没有坐标，本地无从裁起；而中间站不能按表名寻址，只能把证据铺在右栏。
 */
function openListedColumn(card, column) {
  const bag = state.sqlflow;
  const rowId = rowIdOfColumnId(bag.index, column.id);
  if (rowId) {
    pickRow(rowId, column.modelId);
    return;
  }
  if (state.view !== 'parsed-column' && !card.local) {
    selectColumn(column.id, card.table);
    return;
  }
  renderColumn(ui.detail, card, columnCard(bag.index, column.modelId));
}

/**
 * 字段视图的右栏优先查这一份响应，查不到才退回按表寻址的老端点。
 *
 * "一次请求给全"的意思就是图上有的、右栏列的、顶栏数的是同一份结论；
 * 再打一次 /api/table/x/columns 会拿到第二个真相（那张表在整库里的全量字段）。
 *
 * @return 响应里查到了并渲染好，返回 true
 */
function sqlflowDetail(columnId, tableId) {
  const bag = state.sqlflow;
  if (!bag || !bag.view) {
    return false;
  }
  if (columnId) {
    const modelId = bag.index.columnIdByName.get(columnId);
    const card = modelId === undefined ? null : columnCard(bag.index, modelId);
    if (card) {
      renderColumn(ui.detail, entityCard(bag.index, card.entityId), card);
      return true;
    }
  }
  const entityId = bag.index.entityIdByName.get(tableId);
  if (entityId === undefined) {
    return false;
  }
  listEntity(entityId);
  return true;
}

function drawLegend(kind, maxLayer, extraNote) {
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
    for (let layer = 0; layer <= maxLayer; layer++) {
      swatch(layerColor(layer), `第 ${layer} 层`);
    }
    note('点表节点：以它为中心裁剪，并在右栏看字段清单');
  } else {
    /* 图例逐项从 EDGE_STYLE 生成：这张表漏过 UNNAMED——语料里有未命名列边，
       画布按默认色画了出来，图例却不认识它。键即语义，加一种加工方式就自动进图例。 */
    Object.keys(EDGE_STYLE).forEach((derivation) =>
      swatch(EDGE_STYLE[derivation]['line-color'],
        `${derivation} ${derivationLabel(derivation)}`));
    note('一表一盒、一行一字段，线连字段行不连盒子；盒顶色带就是该表所在的层');
    note('点字段行=只画这一列的通路（含中间跳），点盒顶表名=以那张表为中心展开');
    note('点线=按名字没对上（UNNAMED/STAR），虚线=需要人工确认（CONSTANT/UNRESOLVED）');
    note('虚线暗盒 = 语句内中间关系（CTE/子查询/UNNEST），由边的 hops 显形，一段段仍属同一条边');
  }
  if (extraNote) {
    note(extraNote);
  }
}

/* ---------------- 选区联动 ---------------- */

/** 按选区重画中栏 + 填右栏：换视图、换深度、点节点、启动都只走这一条路径 */
async function refresh() {
  const seq = ++renderSeq;
  if (state.view === 'parsed-table' || state.view === 'parsed-column') {
    // 没解析过就被点到了（手工换 hash、解析失败后残留）：退回快照视图，别画空图
    if (!state.parse) {
      switchView('table');
      await refresh();
      return;
    }
    /* 试解析的三条路径都不打快照端点，同步画完，不会有别的意图插在中间 */
    if (state.view === 'parsed-table') {
      await drawParsedTable();
      renderEmpty(ui.detail, '临时解析视图：点表节点看它在这次解析里的字段清单，点表级边看它出自哪条语句。');
      return;
    }
    await drawParsedColumns(seq);
    if (!sqlflowDetail(state.parsedColumn, null)) {
      renderEmpty(ui.detail, state.parsedColumn
        ? '这一列没进这次解析的画布：多半超出了每盒行数预算，顶栏有降级说明。'
        : '点字段行看它在这次解析里的来源，点边看加工方式与 SQL 原文。');
    }
    return;
  }
  if (state.view === 'table') {
    await drawTableGraph(seq);
  } else {
    await drawColumnGraph(seq);
  }
  if (seq !== renderSeq) {
    return;
  }
  if (state.view === 'column' && sqlflowDetail(state.column, state.table)) {
    return;
  }
  if (state.column) {
    await showColumnDetail(state.column, state.table, seq);
  } else if (state.table) {
    await showTableDetail(state.table, seq);
  } else {
    renderEmpty(ui.detail, state.view === 'column'
      ? '先在表级视图点一张表，再进字段链路'
      : '点图中的表节点看字段清单，点字段边看逐跳来源证据。');
  }
}

async function showTableDetail(tableId, seq) {
  const data = await api.columns(tableId);
  if (seq !== renderSeq) {
    return;
  }
  renderTable(ui.detail, data, {
    onColumn: (column) => openColumnView(data.table, column.id),
    onWholeTable: (table) => openColumnView(table, null),
  });
}

async function showColumnDetail(columnId, table, seq) {
  const data = await api.columns(table);
  if (seq !== renderSeq) {
    return;
  }
  const column = (data.columns || []).find((row) => row.id === columnId);
  if (column) {
    renderColumn(ui.detail, data, column);
  } else {
    renderEmpty(ui.detail, `${columnId} 没有出现在字段清单里：它可能是被折掉的中间关系字段`);
  }
}

async function selectTable(tableId) {
  leaveParsed();
  state.table = tableId;
  state.column = null;
  syncHash();
  await refresh();
  await markActiveRow();
}

async function selectColumn(columnId, table) {
  leaveParsed();
  state.table = table;
  state.column = columnId;
  syncHash();
  await refresh();
  await markActiveRow();
}

/* ---------------- 临时解析视图的右栏 ---------------- */

/** 快照里的动作（选表、点空白）一旦发动就退出临时解析视图 */
function leaveParsed() {
  if (state.view === 'parsed-table' || state.view === 'parsed-column') {
    switchView('table');
  }
}

function showParsedTable(tableId) {
  setHot(cy, tableId);
  renderTable(ui.detail, parsedTableDetail(state.parse, tableId), {
    onColumn: (column) => openParsedColumn(column.id),
    onWholeTable: () => openParsedColumn(null),
  });
}

function openParsedColumn(columnId) {
  state.parsedColumn = columnId;
  switchView('parsed-column');
  refresh().catch(fail);
}

/** 左栏高亮跟着选区走：搜索/过滤之后列表会重画，所以复用同一次取数 */
async function markActiveRow() {
  ui.tableList.querySelectorAll('li').forEach((row) => {
    row.classList.toggle('active', row.dataset.id === state.table);
  });
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
  leaveParsed();
  pop.hide();
  state.table = null;
  state.column = null;
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
    setWarn('当前没有可导出的图');
    return;
  }
  download(`lineage-${state.view}-${Date.now()}.png`,
    cy.png({ full: true, scale: 2, bg: ink.canvasBg }));
}

function exportJson() {
  if (!state.payload) {
    setWarn('当前没有可导出的子图');
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
    // 临时解析视图不写 hash：刷新之后 payload 就没了，那条链接分享出去只能得到空图
    if (!button.dataset.view.startsWith('parsed-')) {
      syncHash();
    }
    refresh().catch(fail);
  });
});

ui.search.addEventListener('input', debounce(guard(loadTables), 250));
ui.layerFilter.addEventListener('change', guard(loadTables));
ui.onlyUnresolved.addEventListener('change', guard(loadTables));
ui.depth.addEventListener('change', guard(refresh));
ui.direction.addEventListener('change', guard(() => {
  if (state.view === 'table') {
    /* 只重画中栏，不动右栏：换个方向不该把已选字段的证据面板刷掉，所以不走 refresh */
    drawTableGraph(++renderSeq);
  }
}));
document.getElementById('rescan').addEventListener('click', guard(async () => {
  const dir = ui.scanDir.value.trim();
  setWarn(`正在重扫${dir || '当前'}目录…`);
  const data = await api.scan(dir);
  state.table = null;
  state.column = null;
  pop.hide();
  // 快照换了，上一段临时解析结果也就没有对照意义了
  state.parse = null;
  state.parsedColumn = null;
  markParsedTabs();
  switchView('table');
  syncHash();
  await loadOverview();
  await loadTables();
  await refresh();
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
}().catch(fail));
