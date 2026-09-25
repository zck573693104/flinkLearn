/**
 * 字段级血缘画布：坐标全部来自服务端，这里只做"左上角 → cytoscape 中心点"的换算。
 *
 * 为什么布局搬到后端：这套图的尺寸口径（盒宽 162、行距 16、标题带 21.97）一旦只有前端懂，
 * "这张图长什么样"就没法在单测里断言，也没法被第二个消费方（导出、截图）复用。
 * 前端再算一遍就是一份会走样的副本。
 *
 * 画布上的三种节点与旧版同名（tableBox/hopBox、headerRow、columnRow），
 * 所以配色继续走 graph.js 的 nodePaint——两种图共用一份口径，图例和色块不会各说各话。
 *
 * 表名不写在盒子上：盒子的第一行距离盒顶是一条固定的标题带，带子单独画一个实色节点，
 * 色块自己就是分隔线，也不用猜 text-margin-y。
 */

import { EDGE_STYLE } from './badges.js';
import { fit, nodePaint } from './graph.js';

/**
 * 从响应里量回布局口径，而不是在前端抄一份常量。
 *
 * 行距 = 相邻两行的 y 之差（盒里只有一行时用它自己的高度，服务端这两者相等）；
 * 标题带 = 第一行 y 与盒顶之差；盒底留白 = 盒高减去全部行高。
 */
function measureGeometry(tables) {
  const withRows = tables.filter((box) => (box.columns || []).length);
  if (!withRows.length) {
    return null;
  }
  const box = withRows[0];
  const rows = box.columns;
  const rowH = rows.length > 1 ? rows[1].y - rows[0].y : rows[0].height;
  const rowTop = rows[0].y - box.y;
  return {
    rowH,
    rowTop,
    boxPad: box.height - rows.length * rowH,
    gap: gapOf(tables, box.height - rows.length * rowH),
  };
}

/** 同列相邻两盒之间的缝：取所有能量到的差里最小的那个，别让某条特别宽的列代表全局 */
function gapOf(tables, boxPad) {
  const ranks = new Map();
  tables.forEach((node) => {
    const column = ranks.get(node.x) || [];
    column.push(node);
    ranks.set(node.x, column);
  });
  let gap = Infinity;
  ranks.forEach((column) => {
    const sorted = column.slice().sort((a, b) => a.y - b.y);
    for (let i = 1; i < sorted.length; i++) {
      gap = Math.min(gap, sorted[i].y - (sorted[i - 1].y + sorted[i - 1].height));
    }
  });
  return Number.isFinite(gap) && gap > 0 ? gap : Math.max(12, boxPad);
}

function nodeOf(geo, box, row) {
  if (!row) {
    return {
      group: 'nodes',
      data: {
        id: box.id, label: '', kind: box.local ? 'hopBox' : 'tableBox',
        local: !!box.local, layer: box.layer || 0, modelId: box.modelId,
        table: box.qualifiedName, type: box.type,
      },
      style: style(box.width, box.height, box.label),
      position: { x: box.x + box.width / 2, y: box.y + box.height / 2 },
    };
  }
  if (row === true) {
    return {
      group: 'nodes',
      data: {
        id: `${box.id}#h`, label: box.label.content, kind: 'headerRow', box: box.id,
        local: !!box.local, layer: box.layer || 0, modelId: box.modelId,
        table: box.qualifiedName, type: box.type,
      },
      style: Object.assign(style(box.width - 6, geo.rowTop - 5, box.label),
        { 'text-max-width': `${box.width - 16}px` }),
      position: { x: box.x + box.width / 2, y: box.y + geo.rowTop / 2 },
    };
  }
  return {
    group: 'nodes',
    data: {
      id: row.id, label: row.label.content, kind: 'columnRow', box: box.id,
      local: !!box.local, layer: box.layer || 0, column: row.qualifiedName,
      modelId: row.modelId, entityId: box.modelId, table: box.qualifiedName,
    },
    /* 行高让出 2px 缝：服务端按 16px 排距，紧贴的色块会糊成一片表格线 */
    style: Object.assign(style(row.width - 2, row.height - 2, row.label),
      { 'text-max-width': `${row.width - 10}px` }),
    position: { x: row.x + row.width / 2, y: row.y + row.height / 2 },
  };
}

/** 字号与字体也听服务端的：标签度量本来就和坐标一起算出来的 */
function style(width, height, label) {
  return {
    width,
    height,
    'font-size': `${label.fontSize}px`,
    'font-family': label.fontFamily,
  };
}

function edgeOf(edge, rel) {
  return {
    group: 'edges',
    data: {
      id: edge.id, source: edge.sourceId, target: edge.targetId,
      derivation: rel ? (rel.derivation || '') : '',
      confidence: rel ? rel.confidence : null,
      relationship: rel ? rel.id : null,
    },
    style: Object.assign({}, EDGE_STYLE[rel && rel.derivation] || {},
      edge.synthetic ? { 'line-style': 'dashed' } : {}),
  };
}

export function paintSqlFlow(cy, data, handlers) {
  const elements = ((data.graph || {}).elements) || {};
  const tables = elements.tables || [];
  const meta = data.metaInfo || {};
  cy.elements().remove();
  if (!meta.drawn || !tables.length) {
    return idleView(meta.warning);
  }
  const geo = measureGeometry(tables);
  const relationshipOf = new Map();
  ((((data.sqlflow || {}).relationships) || [])).forEach((rel) => relationshipOf.set(rel.id, rel));
  const edgeRelationships = ((data.graph || {}).relationshipIdMap) || {};

  const json = [];
  tables.forEach((box) => {
    json.push(nodeOf(geo, box), nodeOf(geo, box, true));
    (box.columns || []).forEach((row) => json.push(nodeOf(geo, box, row)));
  });
  (elements.edges || []).forEach((edge) => {
    const ids = edgeRelationships[edge.id] || [];
    json.push(edgeOf(edge, ids.length ? relationshipOf.get(ids[0]) : null));
  });
  cy.add(json);
  cy.nodes().forEach(nodePaint);
  cy.edges().forEach((edge) => {
    edge.on('mouseover', () => edge.style('label', edge.data('derivation')));
    edge.on('mouseout', () => edge.style('label', ''));
  });
  bind(cy, handlers);
  return interactiveView(cy, geo, tables, meta.warning);
}

/** 超上限或空图：不画，但后端那句交代必须原样送到用户眼前 */
function idleView(warning) {
  return {
    framing: '',
    modelWarning: warning || '这份血缘没有可画的盒子：请选中具体表或字段，或调小深度',
    cropped: false,
    boxCount: 0,
    crop: () => ({ framing: '', modelWarning: warning || '', cropped: false, boxCount: 0 }),
    reset: () => ({ framing: '', modelWarning: warning || '', cropped: false, boxCount: 0 }),
  };
}

function bind(cy, handlers) {
  cy.off('tap');
  cy.on('tap', 'edge', (event) => handlers.onEdge(event.target.id(), event.target.data()));
  cy.on('tap', 'node[kind="columnRow"]', (event) => handlers.onRow(event.target.id(), event.target.data()));
  cy.on('tap', 'node[kind="headerRow"], node[kind="tableBox"], node[kind="hopBox"]',
    (event) => handlers.onBox(event.target.id(), event.target.data()));
  cy.on('tap', (event) => {
    if (event.target === cy) {
      handlers.onBlank();
    }
  });
}

/**
 * 沿边走一圈：正向到下游、反向到源头。
 *
 * 端点可能是行也可能是盒（服务端预算没把那一行画进盒子时，线就连盒子），
 * 所以这里只按标识收集，不假设拿到的都是行。
 */
function chainOf(cy, rowId) {
  const next = new Map();
  const prev = new Map();
  const add = (map, key, value) => {
    if (!map.has(key)) {
      map.set(key, []);
    }
    map.get(key).push(value);
  };
  cy.edges().forEach((edge) => {
    add(next, edge.data('source'), edge.data('target'));
    add(prev, edge.data('target'), edge.data('source'));
  });
  const keep = new Set([rowId]);
  [next, prev].forEach((map) => {
    const queue = [rowId];
    while (queue.length) {
      (map.get(queue.pop()) || []).forEach((id) => {
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
 * 裁剪后的重排：留下的行重新从盒顶往下摞，盒高跟着行数变，同列往上挤。
 *
 * 只有纵向动，横向（哪一列）一律不动——列号是服务端算的层号，是这套图的语义本身，
 * 前端没有理由在用户点一下之后把"第 2 层"挪到"第 1 列"的位置上。
 */
function restack(cy, geo, base, visible) {
  const heights = new Map();
  base.forEach((box, id) => {
    if (!visible.boxes.has(id)) {
      return;
    }
    const rows = box.rows.filter((row) => visible.rows.has(row.id));
    heights.set(id, geo.boxPad + rows.length * geo.rowH);
  });

  const ranks = new Map();
  heights.forEach((height, id) => {
    const x = base.get(id).x;
    const column = ranks.get(x) || [];
    column.push({ id, height, y: base.get(id).y });
    ranks.set(x, column);
  });
  ranks.forEach((column) => {
    column.sort((a, b) => a.y - b.y);
    const top = Math.min(...column.map((node) => node.y));
    let cursor = top;
    column.forEach((node) => {
      place(cy, geo, base.get(node.id), node.height, cursor, visible);
      cursor += node.height + geo.gap;
    });
  });
}

/** 一个盒落位：盒、标题带、留下的行按新高度重排，行序保持服务端给的那一份 */
function place(cy, geo, box, height, top, visible) {
  const center = { x: box.x + box.width / 2 };
  cy.getElementById(box.id).position({ y: top + height / 2, x: center.x })
    .style('height', height);
  cy.getElementById(`${box.id}#h`).position({ y: top + geo.rowTop / 2, x: center.x });
  let index = 0;
  box.rows.forEach((row) => {
    if (!visible.rows.has(row.id)) {
      return;
    }
    cy.getElementById(row.id).position({
      x: row.x + row.width / 2,
      y: top + geo.rowTop + index * geo.rowH + row.height / 2,
    });
    index += 1;
  });
}

function interactiveView(cy, geo, tables, modelWarning) {
  const base = new Map();
  tables.forEach((box) => {
    base.set(box.id, {
      // id 必须跟着一起存：place() 用 getElementById(box.id) 找盒，缺了就是一串空集合写入
      id: box.id,
      x: box.x,
      y: box.y,
      width: box.width,
      height: box.height,
      rows: (box.columns || []).map((row) => ({
        id: row.id, x: row.x, y: row.y, width: row.width, height: row.height,
      })),
    });
  });
  const rowOwner = new Map();
  base.forEach((box, id) => box.rows.forEach((row) => rowOwner.set(row.id, id)));

  const showAll = () => {
    cy.elements().style('display', 'element');
    base.forEach((box, id) => {
      cy.getElementById(id).position({ x: box.x + box.width / 2, y: box.y + box.height / 2 })
        .style('height', box.height);
      cy.getElementById(`${id}#h`).position({ x: box.x + box.width / 2, y: box.y + geo.rowTop / 2 });
      box.rows.forEach((row, index) => cy.getElementById(row.id).position({
        x: row.x + row.width / 2,
        y: box.y + geo.rowTop + index * geo.rowH + row.height / 2,
      }));
    });
  };

  const view = {
    framing: fit(cy).warning,
    modelWarning,
    cropped: false,
    boxCount: base.size,

    /** 只留这一列的通路：不重发请求，画布上已有的边就是这次请求的结论 */
    crop(rowId) {
      /*
       * 这一行不在当前画布上（换过图、句柄没跟着清）就什么都不做：往下第一步是把全部元素
       * display:none，再按旧标识往回找——旧标识在新图上谁也找不到，剩下的就是一张白画布。
       */
      if (!cy.getElementById(rowId).nonempty()) {
        return view;
      }
      const reached = chainOf(cy, rowId);
      const rows = new Set();
      const boxes = new Set();
      reached.forEach((id) => {
        const owner = rowOwner.get(id);
        if (owner) {
          rows.add(id);
          boxes.add(owner);
        } else if (base.has(id)) {
          boxes.add(id);
        }
      });
      cy.elements().style('display', 'none');
      boxes.forEach((id) => cy.getElementById(id).style('display', 'element'));
      boxes.forEach((id) => cy.getElementById(`${id}#h`).style('display', 'element'));
      rows.forEach((id) => cy.getElementById(id).style('display', 'element'));
      cy.edges().forEach((edge) => {
        const both = (rows.has(edge.data('source')) || boxes.has(edge.data('source')))
          && (rows.has(edge.data('target')) || boxes.has(edge.data('target')));
        if (both) {
          edge.style('display', 'element');
        }
      });
      restack(cy, geo, base, { rows, boxes });
      view.cropped = true;
      view.framing = fit(cy).warning;
      return view;
    },

    reset() {
      showAll();
      view.cropped = false;
      view.framing = fit(cy).warning;
      return view;
    },
  };
  return view;
}
