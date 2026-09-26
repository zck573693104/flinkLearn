/**
 * 右栏详情面板：这套 UI 的交付物就是这里——"这个字段从哪来"必须一屏说清。
 *
 * 渲染全部走 DOM 节点，不用 innerHTML 拼用户/语料内容：SQL 原文是不可信文本。
 */
import { confidenceText, derivationBadge, derivationLabel } from './badges.js';

/** 字段清单一次最多铺这么多行：语料里有 783 列的表，全渲染会把右栏卡死 */
const COLUMN_CAP = 80;

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) {
    node.className = className;
  }
  if (text !== undefined && text !== null) {
    node.textContent = String(text);
  }
  return node;
}

function clear(container) {
  container.textContent = '';
  return container;
}

function kv(rows) {
  const dl = el('dl', 'kv');
  rows.forEach(([key, value]) => {
    if (value === undefined || value === null || value === '') {
      return;
    }
    dl.appendChild(el('dt', null, key));
    dl.appendChild(el('dd', null, value));
  });
  return dl;
}

function code(text) {
  return el('code', null, text);
}

function alertBox(className, text) {
  return el('div', `alert ${className}`, text);
}

function button(label, onClick) {
  const node = el('button', null, label);
  node.type = 'button';
  node.addEventListener('click', onClick);
  return node;
}

function shortName(id) {
  const dot = String(id || '').lastIndexOf('.');
  return dot < 0 ? String(id || '') : String(id).slice(dot + 1);
}

function escapeRegExp(raw) {
  return raw.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * ColumnRef.nodeId() 不是 getter，Jackson 不会下发这个字段，
 * 所以未折叠的原始边要在前端按同一口径拼一次。
 */
function refName(ref) {
  const table = ref.boundTable || ref.qualifier;
  return table ? `${table}.${ref.column}` : ref.column;
}

/**
 * 原文回显 + 来源/目标列高亮。
 *
 * 只按列名末段匹配：SQL 里写 `t.amount` 还是 `amount` 都能命中。
 * 来源列与目标列同名时无法区分两处出现，统一按目标列着色——v1 不做词法定位。
 */
function sqlBlock(sql, from, to) {
  const pre = el('pre', 'sql');
  const text = sql || '';
  if (!text) {
    pre.textContent = '（快照里没有这条语句的原文）';
    return pre;
  }
  const target = shortName(to);
  const source = shortName(from);
  const tokens = [...new Set([target, source].filter(Boolean))];
  if (!tokens.length) {
    pre.textContent = text;
    return pre;
  }
  const pattern = new RegExp(`(?:${tokens.map(escapeRegExp).join('|')})\\b`, 'gi');
  let cursor = 0;
  let hit = pattern.exec(text);
  while (hit) {
    if (hit.index > cursor) {
      pre.appendChild(document.createTextNode(text.slice(cursor, hit.index)));
    }
    const mark = el('mark', null, hit[0]);
    if (hit[0].toLowerCase() === target.toLowerCase()) {
      mark.className = 'target';
    }
    pre.appendChild(mark);
    cursor = hit.index + hit[0].length;
    hit = pattern.exec(text);
  }
  pre.appendChild(document.createTextNode(text.slice(cursor)));
  return pre;
}

/** 逐跳链路：被折掉的伪节点（CTE / 子查询）也在这里显形 */
function hopList(hops) {
  const ul = el('ul', 'hops');
  (hops || []).forEach((hop, index) => {
    const li = el('li');
    li.appendChild(code(hop));
    if (index < hops.length - 1) {
      li.appendChild(el('span', 'arrow', '→'));
    }
    ul.appendChild(li);
  });
  return ul;
}

function edgeHeader(edge) {
  const line = el('h3');
  line.appendChild(code(edge.to));
  line.appendChild(document.createTextNode(' ← '));
  line.appendChild(code(edge.from));
  line.appendChild(document.createTextNode(' '));
  line.appendChild(derivationBadge(edge.derivation));
  return line;
}

/** 一条边的完整证据块：链路 + 加工方式 + 出处 + 原文 */
function edgeBlock(edge) {
  const section = el('section');
  section.appendChild(edgeHeader(edge));
  if (edge.parseError) {
    section.appendChild(alertBox('error',
      '此边来自语法错误恢复的部分结果，不可信：解析器报错后继续恢复出的血缘，可能缺表或错表'));
  }
  if ((edge.hops || []).length > 2) {
    section.appendChild(hopList(edge.hops));
  }
  section.appendChild(kv([
    ['加工方式', `${edge.derivation || '?'} · ${derivationLabel(edge.derivation)}`],
    ['置信度', confidenceText(edge.derivation, edge.confidence)],
    ['表达式', edge.transform],
    ['引擎', edge.engine],
    ['语句', edge.jobId],
    // ordinal 是 0 起的 SELECT 下标，界面上说"第几位"要 +1
    ['SELECT 位置', Number.isInteger(edge.ordinal) ? `第 ${edge.ordinal + 1} 位` : null],
  ]));
  if (edge.sqlText) {
    section.appendChild(sqlBlock(edge.sqlText, edge.from, edge.to));
  }
  return section;
}

export function renderEmpty(container, message) {
  clear(container).appendChild(el('p', 'empty',
    message || '点图中的表节点看字段清单，点字段边看逐跳来源证据。'));
}

export function renderError(container, error) {
  clear(container).appendChild(alertBox('error',
    error instanceof Error ? error.message : String(error)));
}

/**
 * 表详情：字段清单 + 选中字段的直接来源。
 *
 * @param handlers.onColumn(column) 点字段名，图切到该字段的链路
 * @param handlers.onWholeTable(table) 一次铺开整表字段链路
 */
export function renderTable(container, data, handlers) {
  clear(container);
  const columns = data.columns || [];
  const painted = columns.filter((column) => column.painted !== false).length;
  container.appendChild(el('h2', null, data.name || data.table));
  container.appendChild(kv([
    ['标识', data.table],
    ['库/命名空间', data.db],
    ['层级', `第 ${data.layer} 层`],
    ['关系类型', data.local ? '语句内关系（CTE / 子查询 / 展开）' : '物理表'],
    ['字段数', columns.length],
    // painted 只有 /api/sqlflow/graph 给：整表列了 40 个、盒子里只画了 14 行，
    // 不交代这一层差，右栏就成了比画布更理直气壮的第二个清单
    ...(columns.some((column) => column.painted !== undefined)
      ? [['画进盒子', `${painted} / ${columns.length}`]] : []),
    ...(data.hiddenColumns ? [['超出登记上限', data.hiddenColumns]] : []),
  ]));

  const tools = el('div', 'column-picker');
  // 语句内中间站不能按名字重新寻址，这个按钮点了只会拿到 400，那就别摆出来
  if (!data.local) {
    tools.appendChild(button('铺开整表字段链路', () => handlers.onWholeTable(data.table)));
    container.appendChild(tools);
  }

  if (!columns.length) {
    container.appendChild(alertBox('info', '这张表没有字段级血缘：多半是 SELECT * 或建表 DDL 未提供列清单'));
    return;
  }

  const list = el('ul', 'list');
  columns.slice(0, COLUMN_CAP).forEach((column) => {
    const li = el('li');
    li.appendChild(el('span', 'name', column.column));
    if (column.unresolvedSource) {
      // 零来源有两种：真起点，还是来源没绑上被折进账本——只有后者要标
      li.appendChild(derivationBadge('UNRESOLVED'));
    }
    const meta = el('span', 'meta');
    meta.textContent = `↑${column.sourceCount} 来源 · ↓${column.consumerCount} 去向`;
    if (column.painted === false) {
      meta.appendChild(el('span', 'dim', '· 未画进盒子'));
    }
    li.appendChild(meta);
    li.addEventListener('click', () => handlers.onColumn(column));
    list.appendChild(li);
  });
  container.appendChild(list);
  if (columns.length > COLUMN_CAP) {
    container.appendChild(el('p', 'note', `只列出前 ${COLUMN_CAP} 个字段，共 ${columns.length} 个`));
  }
}

/** 单个字段的来源清单：一个目标列多条来源时全部铺开，不做折叠 */
export function renderColumn(container, table, column) {
  clear(container);
  container.appendChild(el('h2', null, `${column.column}（${table.name || table.table}）`));
  container.appendChild(kv([
    ['字段标识', column.id],
    ['直接来源数', column.sourceCount],
    ['下游去向数', column.consumerCount],
  ]));

  const block = el('section');
  block.appendChild(el('h3', null, '来源'));
  if ((column.sources || []).length) {
    column.sources.forEach((edge) => block.appendChild(edgeBlock(edge)));
  } else if (column.unresolvedSource) {
    block.appendChild(alertBox('error',
      '来源列没能绑定到任何输入表（UNRESOLVED）：这条边进不了图，所以图上没有入边。'
      + '它是解析缺口，不是链路起点，结论要人工核对。'));
  } else {
    block.appendChild(el('p', 'note', '没有解析到来源：这是链路起点（源表字段）'));
  }
  container.appendChild(block);

  if ((column.consumers || []).length) {
    const out = el('section');
    out.appendChild(el('h3', null, '下游字段'));
    const ul = el('ul', 'hops');
    column.consumers.forEach((id) => {
      const li = el('li');
      li.appendChild(code(id));
      ul.appendChild(li);
    });
    out.appendChild(ul);
    container.appendChild(out);
  }
}

/** 字段边证据：同一对端点可能有多条边（不同加工方式），逐条铺开 */
export function renderEdge(container, edges, summary) {
  clear(container);
  const rows = edges || [];
  const head = el('h2', null, '字段边');
  container.appendChild(head);
  container.appendChild(el('p', 'note',
    `${summary || `${(rows[0] || {}).from} → ${(rows[0] || {}).to}`}：命中 ${rows.length} 条边`));
  if (rows.length > 1) {
    container.appendChild(alertBox('info', '同一对端点有多条边：不同语句或不同加工方式各算一条，不是重复数据'));
  }
  rows.forEach((edge) => container.appendChild(edgeBlock(edge)));
}

/** 表级边：只带端点与语句标识，够回答"这条表间边来自哪份文件" */
export function renderTableEdge(container, data) {
  clear(container);
  container.appendChild(el('h2', null, '表级边'));
  container.appendChild(kv([
    ['来源表', data.source],
    ['目标表', data.target],
    ['语句', data.jobId],
  ]));
  if (!data.jobId) {
    container.appendChild(el('p', 'note', '这条边没有 jobId：由文本摘要兜底命名，见 ColumnGraphBuilder.localName()'));
  }
  /*
   * 表级关系虚线（RelationRows）把语句原文带在边上：有 sqlText 字段就高亮端点表名。
   * 用 hasOwnProperty 判断而不是 truthy——表级 DAG 的老边没这个字段，别给它们凭空加
   * 一块"（快照里没有这条语句的原文）"。
   */
  if (Object.prototype.hasOwnProperty.call(data, 'sqlText')) {
    container.appendChild(sqlBlock(data.sqlText, data.source, data.target));
  }
}

/** 试解析结果：落库前的临时视图，用来人工核对新 SQL 的血缘 */
export function renderParse(container, data) {
  clear(container);
  container.appendChild(el('p', 'note', `切分出 ${data.statementCount} 条语句，`
    + `折叠后 ${data.columnEdges.length} 条字段边，${data.graph.nodes.length} 张表`));
  (data.statements || []).forEach((statement, index) => {
    const section = el('section');
    const title = el('h3');
    title.appendChild(el('span', null, `#${index + 1} ${statement.processType || '-'} → `));
    title.appendChild(code(statement.targetTable || '(无输出表)'));
    section.appendChild(title);
    if (statement.parseError) {
      section.appendChild(alertBox('error', 'parseError：语法有错，以下是错误恢复出的部分结果'));
    }
    section.appendChild(kv([
      ['输入表', (statement.sourceTables || []).join(', ') || '（无）'],
      ['插入模式', statement.insertMode],
      ['CTE/窗口/时态', [statement.hasCte && 'CTE',
        statement.hasWindowFunc && 'WINDOW', statement.hasTemporalJoin && 'TEMPORAL']
        .filter(Boolean).join(' ') || '（无）'],
      ['字段边数', (statement.columnEdges || []).length],
    ]));
    (statement.columnEdges || []).forEach((edge) => {
      const line = el('p', 'note');
      line.appendChild(code(`${edge.targetTable}.${edge.targetColumn}`));
      line.appendChild(document.createTextNode(' ← '));
      line.appendChild(code((edge.sources || []).map(refName).join(', ')));
      line.appendChild(document.createTextNode(' '));
      line.appendChild(derivationBadge(edge.derivation));
      section.appendChild(line);
    });
    section.appendChild(sqlBlock(statement.originalSql, null, null));
    container.appendChild(section);
  });
}
