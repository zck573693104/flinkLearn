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
import { createBar } from './sqlflowBar.js';
import { mountSqlFlow } from './sqlflowView.js';
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
  bar: document.getElementById('sqlflow-bar'),
  stage: document.getElementById('sqlflow'),
  locate: document.getElementById('sqlflow-locate'),
  locateInput: document.getElementById('locate-input'),
  locateItems: document.getElementById('locate-items'),
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
  /** 字段视图这一份响应：{index, view}，右栏与动作条的证据都从这里查，不再打第二个端点 */
  sqlflow: null,
  /** 动作条现在对着哪个字段行：{rowId, columnId, entityId} */
  barTarget: null,
};

const cy = create(ui.graph);

const bar = createBar(ui.bar, {
  /** "只看这一列"是纯客户端裁剪：画布上已有的线就是这次请求的全部结论 */
  chain: guard(() => {
    if (state.barTarget && state.sqlflow) {
      showWarning(state.sqlflow.view.crop(state.barTarget.rowId));
      showBar(state.barTarget.rowId);
    }
  }),
  columns: guard(() => {
    if (state.barTarget && state.sqlflow) {
      listEntity(state.barTarget.entityId);
    }
  }),
  reset: guard(() => {
    if (state.sqlflow) {
      showWarning(state.sqlflow.view.reset());
    }
    showBar(state.barTarget && state.barTarget.rowId);
  }),
  /** 收起 = 连这次选中一起撤，不是只把条子藏掉 */
  close: guard(clearRowSelection),
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
   * 裁剪是画布当前的状态，不是图本身的说明：动作条收起后（点画布空白）用户只剩一堆重排过的盒子，
   * 标题却还写着"全部字段"。所以 cropped 必须跟着进这一行，复位后自然消失。
   */
  state.modelWarning = [result.modelWarning,
    result.cropped ? '当前只画选中字段的通路（点动作条「复位」还原整片）' : ''].filter(Boolean).join('；');
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

/*
 * 字段视图盖在画布上时，表级那个观察者得闭嘴：cytoscape 里还留着上一张表级图
 * （字段视图不再往它身上塞东西），容器一变尺寸 fit(cy) 照样报"放不下 N 个节点"——
 * 那句话讲的是用户根本没在看的图。
 */
watchResize(cy, (framing) => {
  if (!state.sqlflow) {
    paintFraming(framing);
  }
});

/*
 * 字段视图不在 cytoscape 上，上面那个观察者对它无效：画布一变宽，
 * "放得下/放不下"的结论就过期了，得单独盯着 #sqlflow 重问一次。
 * 只重算 framing 和居中留白，不改用户手动定的缩放倍率。
 */
if (window.ResizeObserver) {
  new ResizeObserver(() => {
    if (state.sqlflow && state.sqlflow.view) {
      paintFraming(state.sqlflow.view.refit().framing);
    }
  }).observe(ui.stage);
}

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
 * 换到表级画法之前，把字段级留下的动作条与裁剪句柄一起清掉。
 *
 * 动作条是钉在画布上的 DOM，画布换内容它不会自己消失；留着它而 state.sqlflow.view
 * 还指向上一份图，点「只看这一列」就会拿旧行号去裁新图。字段视图的 DOM 也在 host 里，
 * 不拆掉就会盖在表级图上（它是 inset:0 的绝对定位层）。
 */
function dropSqlFlow() {
  if (state.sqlflow && state.sqlflow.view) {
    state.sqlflow.view.dispose();
  }
  state.sqlflow = null;
  state.barTarget = null;
  bar.hide();
  ui.stage.hidden = true;
  ui.locate.hidden = true;
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
   * 交完整标识给 focus：邻域照 X 裁，焦点保那一列。手打的深链允许只写裸列名（column=vin），
   * 它讲的就是 state.table 的那一列。
   */
  const picked = state.column || '';
  const prefix = `${state.table}.`;
  const bare = picked && !picked.includes('.');
  const column = bare ? picked : (picked.startsWith(prefix) ? picked.slice(prefix.length) : '');
  const focus = bare || column ? '' : picked;
  const data = await api.sqlflowGraph({
    table: state.table,
    column: column,
    focus: focus,
    depth: depth(),
  });
  if (seq !== renderSeq) {
    return;
  }
  /*
   * 回给画布的焦点一律是完整标识：前端拿它反查图上那一行（rowIdOfColumnId 只认 table.column），
   * 直接拿裸列名去查只会得到 null——请求发得出去、图也画得出来，唯独那一行不亮、动作条也不出。
   */
  state.payload = data;
  showSqlFlow(data, picked ? `${picked} 的字段链路` : `${state.table} 全部字段的链路`,
    bare ? `${prefix}${picked}` : picked);
}

/**
 * 一份响应画一张图：快照的字段视图和试解析的字段视图走同一条路径。
 *
 * focus 是列的完整标识（table.column）：服务端按它裁邻域，前端把那一行选中——
 * 选中只是染色 + 出证据，要不要"只留这一列"归动作条第一个按钮管。
 * 上一版在这里直接 crop，用户带着一个字段进来，看到的是一片只剩三行的图，
 * 找不到自己刚才在看什么。
 */
function showSqlFlow(data, heading, focus) {
  const meta = data.metaInfo || {};
  const summary = data.summary || {};
  dropSqlFlow();
  state.sqlflow = { index: indexSqlFlow(data), view: null };
  ui.stage.hidden = false;
  ui.locate.hidden = false;
  fillLocate(state.sqlflow.index);
  /*
   * 标题报的必须是画布上真有的东西：超上限时服务端只给模型不给落位（tables 是空的），
   * 这时候照念 metaInfo.boxCount 就是"602 表盒"配一张白画布。
   *
   * 没带 focus 进来时替用户挑一个"最有故事"的列先讲起来（开场即有一条约自己的链路亮着，
   * 而不是一张全灰的图）——标题跟着换成那一列，图上亮的就是标题说的。
   */
  const story = focus ? null : storyRow(state.sqlflow.index);
  const shownHeading = story ? `${story.label} 的字段链路` : heading;
  setGraphTitle(meta.drawn === false ? `${shownHeading}（没画：只给数据模型，见下方说明）`
    : `${shownHeading}（${meta.boxCount || 0} 表盒 / ${meta.rowCount || 0} 字段行 / `
      + `${summary.relationship || 0} 条关系）`);
  drawLegend('column', state.maxLayer, null, meta.tableRelSegments);
  state.sqlflow.view = mountSqlFlow(ui.stage, data, sqlflowHandlers());
  const rowId = focus ? rowIdOfColumnId(state.sqlflow.index, focus) : null;
  if (!rowId) {
    const view = state.sqlflow.view;
    /*
     * 带着一个列进来却一行都没亮，得说是为什么：这个标识可能压根不在模型里（手打的深链），
     * 也可能在模型里却被行预算裁在了盒子外面。标题已经写了这一列，只念"N 列未画"那种
     * 通用交代，用户读不出自己找的那一列到底在不在。
     */
    if (focus) {
      view.modelWarning = [view.modelWarning, `${focus} 不在这次的画布上`
        + '（标识不存在，或被行预算裁在盒外）：只画了邻域，没有可高亮的行'].filter(Boolean).join('；');
      showWarning(view);
      return;
    }
    if (story) {
      pickRow(story.rowId, story.modelId);
      return;
    }
    showWarning(view);
    return;
  }
  pickRow(rowId, state.sqlflow.index.rows.get(rowId).modelId);
}

/**
 * 开场讲哪一个故事：在这次请求给的关系里挑一条"既有人喂它、又往下传"的目标列。
 *
 * 口径固定否则开场亮哪行就成了玄学：聚合 > 表达式 > 直通，能继续往下游讲的加半档，
 * 同分取标识最小的那个。挑不出来（全图一条关系都没有）就什么都不选，保持全图无高亮。
 */
function storyRow(index) {
  const weight = { AGGREGATE: 0, EXPRESSION: 1, IDENTITY: 2, CONSTANT: 3, POSITIONAL: 4 };
  const relationships = [...index.relationships.values()];
  let best = null;
  relationships.forEach((rel) => {
    const target = (rel || {}).target || {};
    const rowId = index.rowIdOfColumn.get(target.id);
    if (!rowId || !index.rows.has(rowId)) {
      return;
    }
    const keepsGoing = relationships.some((other) => (other.sources || [])
      .some((source) => source.id === target.id));
    const score = (weight[rel.derivation] === undefined ? 9 : weight[rel.derivation])
      - (keepsGoing ? 0.5 : 0);
    const key = String(target.id || '');
    if (!best || score < best.score || (score === best.score && key < best.key)) {
      const column = index.columns.get(target.id);
      best = {
        score,
        key,
        rowId,
        modelId: target.id,
        label: (column && column.qualifiedName) || key,
      };
    }
  });
  return best;
}

/** 定位搜索的候选：画布上真有的盒（含中间站）与画出来的字段，qualifiedName 就是取值 */
function fillLocate(index) {
  ui.locateItems.textContent = '';
  const seen = new Set();
  const add = (value, label) => {
    if (seen.has(value)) {
      return;
    }
    seen.add(value);
    const option = document.createElement('option');
    option.value = value;
    option.label = label;
    ui.locateItems.appendChild(option);
  };
  index.boxes.forEach((box) => add(box.qualifiedName, `表 · ${box.name}`));
  index.rows.forEach((row) => {
    const column = index.columns.get(row.modelId);
    if (column) {
      add(column.qualifiedName, `字段 · ${column.name}`);
    }
  });
}

/**
 * 动作条对着的是"那一行的完整身份"，不只是图上标识：
 * 三个动作各读一个字段（裁剪读 rowId、列清单读 entityId、标题读 columnId）。
 */
function focusRow(rowId) {
  const row = state.sqlflow.index.rows.get(rowId) || {};
  const column = state.sqlflow.index.columns.get(row.modelId) || {};
  state.barTarget = {
    rowId,
    columnId: column.qualifiedName || rowId,
    entityId: column.entityId,
  };
}

function sqlflowHandlers() {
  return {
    onRow: guard((rowId, column) => {
      if (column && column.kind === 'relation') {
        pickRelationRow(rowId, column);
        return;
      }
      pickRow(rowId, column.modelId);
    }),
    onBox: guard((boxId, box) => pickBox(box)),
    onEdge: guard((edgeId, edge) => {
      /*
       * 表级关系虚线背后没有字段关系，evidenceOfEdge 只会得到空：它的证据就是
       * 边上自带的"哪对表、哪条语句、SQL 原文"。
       */
      if (edge && edge.kind === 'tableRel' && edge.tableRel) {
        const rel = edge.tableRel;
        renderTableEdge(ui.detail, {
          source: rel.from,
          target: rel.to,
          jobId: rel.jobId,
          sqlText: rel.sqlText,
        });
        return;
      }
      const rows = evidenceOfEdge(state.sqlflow.index, edgeId);
      if (rows.length) {
        renderEdge(ui.detail, rows, `${rows[0].from} → ${rows[0].to}`);
      } else {
        renderEmpty(ui.detail, '这条线背后没有解析出的关系：它是中间站之间的一跳，点两端的字段行看证据。');
      }
    }),
    /** 缩放是渲染层的事，只有"放不下"那句说明要回灌到告警条 */
    onFrame: (framing) => paintFraming(framing),
    /** 点空白 = 结束这次以该列为中心的查看：染色、动作条、裁剪一起撤 */
    onBlank: guard(clearRowSelection),
  };
}

/**
 * 点 RelationRows 行：它不是某一列，右栏没有"列证据"可给——讲清这一行是什么、
 * 下一步该点什么（相连的虚线），比硬套一个字段卡诚实。
 */
function pickRelationRow(rowId, column) {
  const view = state.sqlflow.view;
  state.barTarget = {
    rowId,
    columnId: `${column.qualifiedName} · RelationRows`,
    entityId: null,
  };
  view.select(rowId);
  renderEmpty(ui.detail, `${column.qualifiedName} 的表级关系行：它不代表某一列。`
    + '连在它上面的虚线是「只有表级关系、没有字段级血缘」的表对——点那条虚线看是哪条语句。');
  showWarning(view);
  bar.show(state.barTarget.columnId, '点与它相连的虚线看表对与语句');
}

/**
 * 点一行 = 全部在本地完成：换右栏证据、把这一列的上下游染出来、动作条出现。
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
  bag.view.select(rowId);
  renderColumn(ui.detail, entityCard(bag.index, card.entityId), card);
  showBar(rowId);
}

/** 动作条钉在角落，只有文案跟着选区走：裁没裁、下一步能点什么，都在这一行讲清 */
function showBar(rowId) {
  if (!state.sqlflow || !state.sqlflow.view) {
    return;
  }
  showWarning(state.sqlflow.view);
  if (!rowId || !state.barTarget) {
    bar.hide();
    return;
  }
  bar.show(state.barTarget.columnId, state.sqlflow.view.cropped
    ? '当前只画这一列的通路，点「复位」还原整片'
    : '悬停只在原地染色，点「只看这一列」才裁掉其余');
}

/**
 * 点盒顶的表名 = 以那张表为中心重新展开；中间站没在库里，只列它的字段。
 *
 * 语句内关系（CTE/子查询/函数盒）不是能按名字寻址的表，打过去只能得到 400，
 * 所以它不进选表这条路。
 */
function pickBox(box) {
  clearRowSelection();
  if (state.view === 'parsed-column') {
    // 试解析只认字段 focus，没有"以某表为中心重新取一片"这条路；
    // 这份响应本来就是整段 SQL 的结论，就地列字段比打快照端点诚实。
    listEntity(box.modelId);
    return;
  }
  if (!box.local && box.qualifiedName) {
    openColumnView(box.qualifiedName, null);
    return;
  }
  listEntity(box.modelId);
}

/**
 * 结束这次"以某一列为中心"的查看：染色、动作条、裁剪、告警条里那句"只画这一列"四样一起收。
 *
 * 只 bar.hide() 不 deselect 会留下用户看不懂的亮色——他不知道自己为什么在看这三行。
 */
function clearRowSelection() {
  const view = state.sqlflow.view;
  state.barTarget = null;
  /* 裁剪也算这次选中的一部分：留着裁过的图又收走动作条，告警里那句
     "点动作条「复位」还原整片"就指向一个已经不存在的按钮。 */
  if (view.cropped) {
    view.reset();
  }
  view.deselect();
  bar.hide();
  showWarning(view);
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

function drawLegend(kind, maxLayer, extraNote, tableRelCount) {
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
      swatch(EDGE_STYLE[derivation].color,
        `${derivation} ${derivationLabel(derivation)}`));
    note('一表一盒、一行一字段，线连字段行不连盒子；盒顶色带就是该表所在的层');
    note('悬停=染出这一列的上下游，点字段行=选中并在右栏看证据，点动作条「只看这一列」才裁图');
    note('点线=看这一跳的加工方式与 SQL 原文，点盒顶表名=以那张表为中心展开');
    note('虚线暗盒 = 语句内中间关系（CTE/子查询/UNNEST），由边的 hops 显形，一段段仍属同一条边');
    note('左上角搜索框能定位到表或字段：选中即换以它为中心的一片');
    /*
     * 图例不许说画布上没有的东西：RelationRows 虚线只有真的存在时才讲得着，
     * 常驻这一行等于叫用户去找一条不存在的虚线。
     */
    if (tableRelCount) {
      note(`盒里标着 RelationRows 的那一行是表级挂点：这一片有 ${tableRelCount} 对表只有表级关系、`
        + '没有字段级血缘，靠它对出虚线，点虚线看是哪条语句');
    }
    note('Ctrl+滚轮或左下角 ＋／− 缩放，点「适应」回到放得下的比例');
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
  bar.hide();
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
  /*
   * 两张图是两种画法：表级在 cytoscape 的 canvas 上，字段级是 DOM + SVG。
   * 字段级的 PNG 由 sqlflowView 自己照量出来的坐标再画一遍 canvas——
   * 同一份数字画两遍，导出的才和屏幕上的是同一张图。
   */
  if (state.sqlflow && state.sqlflow.view) {
    const href = state.sqlflow.view.png();
    if (href) {
      download(`lineage-${state.view}-${Date.now()}.png`, href);
      return;
    }
    // 字段视图盖在表级图上：这时候退回 cy.png() 会导出一张用户根本没看见的表级图
    setWarn('这一片没画盒子，没有可导出的字段图');
    return;
  }
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
ui.locateInput.addEventListener('change', guard(() => {
  const picked = ui.locateInput.value.trim();
  if (!picked || !state.sqlflow || !state.sqlflow.view) {
    return;
  }
  const index = state.sqlflow.index;
  const rowId = rowIdOfColumnId(index, picked);
  if (rowId && index.rows.has(rowId)) {
    /* 字段命中：走点行同一条路（选中、染色、右栏证据、动作条），输入框清掉等下一次 */
    pickRow(rowId, index.rows.get(rowId).modelId);
    ui.locateInput.value = '';
    return;
  }
  const entityId = index.entityIdByName.get(picked);
  const box = entityId
    ? [...index.boxes.values()].find((candidate) => candidate.modelId === entityId) : null;
  if (box) {
    state.sqlflow.view.revealBox(box.id);
    ui.locateInput.value = '';
    return;
  }
  /* 没命中就不清输入：datalist 只列画布上真有的值，留着文本用户好改 */
}));
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
  bar.hide();
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
