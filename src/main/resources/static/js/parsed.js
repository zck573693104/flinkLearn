/**
 * 把 /api/parse 的临时结果转成右栏要的形状。
 *
 * 这段 SQL 没进快照，所以这里一律本地合成，不能再打快照里的按表寻址端点
 * （表字段清单、字段边详情）——那些端点只认快照里的表，打过去必然 400。
 *
 * 字段级的图和证据不走这里：贴进输入框的 SQL 也能问 /api/sqlflow/graph（POST），
 * 后端把同一套装配逻辑跑在临时解析结果上，前端的画法和右栏口径两边完全一致。
 */

function tableOf(id) {
  const dot = String(id || '').lastIndexOf('.');
  return dot < 0 ? '' : String(id).slice(0, dot);
}

function columnOf(id) {
  const dot = String(id || '').lastIndexOf('.');
  return dot < 0 ? String(id) : id.slice(dot + 1);
}

function tableNode(payload, table) {
  return (payload.graph.nodes || []).find((node) => node.id === table)
    || { id: table, name: table, layer: 0 };
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
