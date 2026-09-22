/**
 * 把 /api/parse 的临时结果转成画图与右栏要的形状。
 *
 * 这段 SQL 没进快照，所以这里一律本地合成，不能再打快照里的按表寻址端点
 * （表字段清单、字段边详情）——那些端点只认快照里的表，打过去必然 400。
 */

export function tableOf(id) {
  const dot = String(id || '').lastIndexOf('.');
  return dot < 0 ? '' : String(id).slice(0, dot);
}

function columnOf(id) {
  const dot = String(id || '').lastIndexOf('.');
  return dot < 0 ? String(id) : String(id).slice(dot + 1);
}

function tableNode(payload, table) {
  return (payload.graph.nodes || []).find((node) => node.id === table)
    || { id: table, name: table, layer: 0 };
}

/** 字段级视图：端点集从边里推，层号沿用表级图里那张表的层 */
export function parsedColumnsView(payload) {
  const edges = payload.columnEdges || [];
  const ids = new Set();
  edges.forEach((edge) => {
    ids.add(edge.from);
    ids.add(edge.to);
  });
  return {
    nodes: [...ids].map((id) => ({
      id,
      column: columnOf(id),
      name: columnOf(id),
      table: tableOf(id),
      layer: tableNode(payload, tableOf(id)).layer,
      local: false,
    })),
    // columnEdges 的字段名与 /api/graph/column 下发的边同构，直接透传
    edges: edges.map((edge) => ({ ...edge })),
  };
}

/** 表详情：字段清单按这张表参与的边合成，供右栏点进字段链路 */
export function parsedTableDetail(payload, table) {
  const node = tableNode(payload, table);
  const rows = new Map();
  const row = (id) => {
    if (!rows.has(id)) {
      rows.set(id, {
        id, column: columnOf(id), sourceCount: 0, consumerCount: 0, unresolvedSource: false,
      });
    }
    return rows.get(id);
  };
  (payload.columnEdges || []).forEach((edge) => {
    if (tableOf(edge.to) === table) {
      row(edge.to).sourceCount += 1;
    }
    if (tableOf(edge.from) === table) {
      row(edge.from).consumerCount += 1;
    }
  });
  return {
    table,
    name: node.name,
    db: node.db,
    layer: node.layer,
    local: false,
    columns: [...rows.values()],
  };
}

/** 字段详情：来源边带上输入框里的原文，右栏才能高亮到列名 */
export function parsedColumnDetail(payload, columnId) {
  const edges = payload.columnEdges || [];
  const sources = edges
    .filter((edge) => edge.to === columnId)
    .map((edge) => ({ ...edge, sqlText: payload.rawSql }));
  const consumers = edges.filter((edge) => edge.from === columnId);
  return {
    id: columnId,
    column: columnOf(columnId),
    sourceCount: sources.length,
    consumerCount: consumers.length,
    sources,
    consumers: consumers.map((edge) => edge.to),
    unresolvedSource: false,
  };
}
