/**
 * 字段级链路图：一张表一个盒子，盒子里一行一个字段，线连行不连盒。
 *
 * 为什么不是"一列一个飘着的方块"：语料实测 783 列的表展开就是 783 个方块，
 * 层与层之间糊成一片噪点，谁流向谁根本看不出来（用户原话：没办法看出血缘关系来）。
 * 表名收进盒子顶部的标题行、列名变成盒子里的一行行槽位之后，一眼能读到的是
 * "哪张表的哪个字段流向哪张表的哪个字段"——这正是字段级血缘要答的问题。
 *
 * 三件事一起撑起可读性：
 * 1. focus：选中某一列时只留它自己的通路（服务端按邻域裁剪，给的是相关表的全部列，
 *    不在前端再收一道的话，"点一个字段"和"展开整表"看起来没区别）。
 * 2. hops 显形：中间关系（CTE/子查询/UNNEST 折出来的伪表）本来只藏在边的 hops 里，
 *    画出来之后"kafka.eventbodylist 直连 user_app_data.app_name"这种断链才接得上。
 * 3. 行数闸门：整表展开时每盒只画有边的前若干列，其余折成"还有 N 列未画"。
 *
 * @param handlers.onColumn(columnId, table) 点列行：换成这一列的单链路
 * @param handlers.onBox(table)              点表名（或盒子空白）：展开那张表
 * @param handlers.onEdge(edgeData)          点线看逐跳证据
 * @param opts.focus                         当前选中的字段标识，用于单链路裁剪
 */

import { EDGE_STYLE } from './badges.js';
import { capNotice, fit, nodePaint, replace, runLayout } from './graph.js';

/** 一个盒子最多画几行列：整表展开时的可读性闸门 */
const ROWS_CAP = 14;
/** 一整张图最多画多少行：盒子多到摊薄时每盒至少留 1 行，宁可字小也不要空白画布 */
const ROW_BUDGET = 600;

export function drawColumns(cy, view, handlers, opts) {
  const model = buildModel(view, (opts || {}).focus);
  const cap = capNotice(model.boxCount, '表盒');
  if (!cap.ok) {
    cy.elements().remove();
    return { framing: '', modelWarning: cap.warning, nodeCount: model.boxCount };
  }

  replace(cy, model.elements);
  cy.nodes().forEach(nodePaint);
  Object.entries(EDGE_STYLE).forEach(([derivation, style]) => {
    cy.edges(`.${derivation}`).forEach((edge) => edge.style(style));
  });
  cy.edges().forEach((edge) => {
    edge.style('label', '');
    edge.on('mouseover', () => edge.style('label', edge.data('derivation')));
    edge.on('mouseout', () => edge.style('label', ''));
  });
  runLayout(cy);
  const fitted = fit(cy);
  bind(cy, handlers);
  return {
    framing: fitted.warning,
    modelWarning: model.warning,
    nodeCount: cy.nodes().size(),
  };
}

/**
 * 只留 focus 这一列的通路：沿边正向走到它的下游、反向走到它的源头。
 *
 * 焦点不在这张图里（比如刚换过表、hash 里留着旧字段）时返回 null：
 * 这时候按"整表展开"画，而不是画一张空图。
 */
function chainOf(view, focus) {
  const edges = view.edges || [];
  if (!focus || !edges.some((edge) => edge.from === focus || edge.to === focus)) {
    return null;
  }
  const next = new Map();
  const prev = new Map();
  const add = (map, key, value) => {
    if (!map.has(key)) {
      map.set(key, new Set());
    }
    map.get(key).add(value);
  };
  edges.forEach((edge) => {
    add(next, edge.from, edge.to);
    add(prev, edge.to, edge.from);
  });
  const keep = new Set([focus]);
  [next, prev].forEach((map) => {
    const queue = [focus];
    while (queue.length) {
      (map.get(queue.pop()) || new Set()).forEach((id) => {
        if (!keep.has(id)) {
          keep.add(id);
          queue.push(id);
        }
      });
    }
  });
  return keep;
}

/**
 * 边里的中间跳摊成真实的一段段。
 *
 * 服务端下发的 hops 是 [源, 中间..., 目标] 的完整路径，端点对不上就不猜
 * （UNRESOLVED 边本来就带不出可信路径），保持原样直连。
 * 层号按路径长度在两端之间插值，伪表因此落在上下游之间而不是叠在某一端身上。
 */
function flatten(edges, layerOf, columns, known) {
  const segments = [];
  const hops = new Set();
  edges.forEach((edge) => {
    const path = (Array.isArray(edge.hops) ? edge.hops : []).filter(Boolean);
    if (path.length < 3 || path[0] !== edge.from || path[path.length - 1] !== edge.to) {
      segments.push({ edge, from: edge.from, to: edge.to, index: 0, of: 1 });
      return;
    }
    const from = layerOf.get(edge.from) || 0;
    const to = layerOf.get(edge.to) || 0;
    const steps = path.length - 1;
    path.slice(1, -1).forEach((id, position) => {
      if (known.has(id)) {
        return;
      }
      known.add(id);
      hops.add(id);
      const dot = id.lastIndexOf('.');
      const table = dot < 0 ? id : id.slice(0, dot);
      columns.push({
        id,
        column: dot < 0 ? id : id.slice(dot + 1),
        table,
        name: relationName(table),
        local: true,
        hop: true,
        layer: from + ((to - from) * (position + 1)) / steps,
      });
    });
    path.slice(0, -1).forEach((fromId, index) => {
      segments.push({
        edge,
        from: fromId,
        to: path[index + 1],
        index: index + 1,
        of: steps,
      });
    });
  });
  return { segments, hopCount: hops.size };
}

/** 伪表标识形如 [语句#序号]#unnest1，标题行只画得起最后那个别名 */
function relationName(table) {
  const hash = table.lastIndexOf(']#');
  return hash < 0 ? table : table.slice(hash + 2);
}

function buildModel(view, focus) {
  const all = view.nodes || [];
  const chain = chainOf(view, focus);
  const layerOf = new Map();
  all.forEach((node) => layerOf.set(node.id, Number.isFinite(node.layer) ? node.layer : 0));

  const columns = all
    .filter((node) => !chain || chain.has(node.id))
    .map((node) => ({
      id: node.id,
      column: node.column || node.id,
      table: node.table || '',
      name: node.name || node.table || '',
      local: !!node.local,
      hop: false,
      layer: Number.isFinite(node.layer) ? node.layer : 0,
    }));
  const { segments, hopCount } = flatten(
    (view.edges || []).filter((edge) => !chain || (chain.has(edge.from) && chain.has(edge.to))),
    layerOf,
    columns,
    new Set(all.map((node) => node.id)),
  );

  const degree = new Map();
  segments.forEach((segment) => {
    degree.set(segment.from, (degree.get(segment.from) || 0) + 1);
    degree.set(segment.to, (degree.get(segment.to) || 0) + 1);
  });

  const boxes = new Map();
  columns.forEach((column) => {
    const id = `box:${column.table}`;
    if (!boxes.has(id)) {
      boxes.set(id, {
        id, table: column.table, label: column.name, local: column.local,
        layer: column.layer, rows: [],
      });
    }
    const box = boxes.get(id);
    box.layer = Math.min(box.layer, column.layer);
    box.local = box.local || column.local;
    box.rows.push(column);
  });

  const elements = [];
  const drawn = new Set();
  let droppedRows = 0;
  /* 表少的时候每盒 14 行，表多的时候按整图预算摊薄：
     语料实测 2 盒 * 14 行还读得出因果，300 盒 * 14 行就是把浏览器当渲染靶子。 */
  const limit = Math.max(1, Math.min(ROWS_CAP, Math.floor(ROW_BUDGET / Math.max(1, boxes.size))));
  boxes.forEach((box) => {
    /* 谁留在盒子里：焦点列永远第一，其次是有边的列（孤岛列画进来只是多一行没线的槽位，
       而截断一条链路会让因果断在半空），同档再按列名排，保证同一张图两次渲染长得一样。 */
    const ranked = box.rows
      .slice()
      .sort((a, b) => (b.id === focus ? 1 : 0) - (a.id === focus ? 1 : 0)
        || (degree.get(b.id) || 0) - (degree.get(a.id) || 0)
        || a.column.localeCompare(b.column));
    const kept = ranked.slice(0, limit);
    const overflow = ranked.length - kept.length;
    droppedRows += overflow;
    kept.forEach((row) => drawn.add(row.id));
    elements.push({
      group: 'nodes',
      data: {
        id: box.id, label: '', kind: box.local ? 'hopBox' : 'tableBox',
        table: box.table, layer: box.layer, local: box.local, rows: kept.length + (overflow ? 1 : 0),
      },
    });
    elements.push({
      group: 'nodes',
      data: {
        id: `${box.id}#h`, label: box.label, kind: 'headerRow', box: box.id, order: 0,
        table: box.table, layer: box.layer, local: box.local,
      },
    });
    kept.forEach((row, index) => elements.push({
      group: 'nodes',
      data: {
        id: row.id, label: row.column, kind: 'columnRow', box: box.id, order: index + 1,
        table: row.table, column: row.column, layer: row.layer, local: row.local, hop: row.hop,
      },
    }));
    if (overflow) {
      elements.push({
        group: 'nodes',
        data: {
          id: `${box.id}#more`, label: `还有 ${overflow} 列未画`, kind: 'columnRow',
          box: box.id, order: kept.length + 1, table: box.table,
          layer: box.layer, local: box.local, capped: true,
        },
      });
    }
  });

  const keptSegments = segments.filter((segment) => drawn.has(segment.from) && drawn.has(segment.to));
  const droppedEdges = segments.length - keptSegments.length;
  const notices = [];
  if (hopCount) {
    notices.push(`${hopCount} 个中间跳（CTE/子查询/UNNEST 折出来的伪表）已按边的 hops 显形`);
  }
  if (droppedRows) {
    notices.push(`${droppedRows} 列未画（${boxes.size} 张表，每表最多 ${limit} 列）：`
      + '点字段行走单列链路，或点表名以该表为中心裁剪');
  }
  if (droppedEdges) {
    notices.push(`${droppedEdges} 段链路因两端列未画而暂不显示`);
  }
  return {
    boxCount: boxes.size,
    hopCount,
    warning: notices.join('；'),
    elements: [
      ...elements,
      ...keptSegments.map((segment) => ({
        group: 'edges',
        data: {
          id: segment.of > 1 ? `${segment.edge.id}>${segment.index}` : segment.edge.id,
          source: segment.from,
          target: segment.to,
          derivation: segment.edge.derivation,
          confidence: segment.edge.confidence,
          jobId: segment.edge.jobId,
          hops: segment.edge.hops,
          transform: segment.edge.transform,
          realFrom: segment.edge.from,
          realTo: segment.edge.to,
          step: segment.of > 1 ? `${segment.index}/${segment.of}` : '',
        },
        classes: segment.edge.derivation || '',
      })),
    ],
  };
}

function bind(cy, handlers) {
  cy.off('tap');
  cy.on('tap', 'edge', (event) => handlers.onEdge(event.target.data()));
  /* 中间跳的行不是真字段：拿它的标识去打 /api/column 只能得到 400，所以只让它当视觉锚点。 */
  cy.on('tap', 'node[kind="columnRow"]', (event) => {
    const data = event.target.data();
    if (!data.capped && !data.hop) {
      handlers.onColumn(data.id, data.table);
    }
  });
  cy.on('tap', 'node[kind="headerRow"], node[kind="tableBox"], node[kind="hopBox"]', (event) => {
    const data = event.target.data();
    if (!data.local) {
      handlers.onBox(data.table);
    }
  });
}
