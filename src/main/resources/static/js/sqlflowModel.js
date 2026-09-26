/**
 * /api/sqlflow/graph 响应的索引层：把"图上的一个色块"翻译回"数据模型里的一个对象"。
 *
 * 服务端一次给全（数据模型 + 布局 + 证据），所以前端不再自己拼第二份真相：
 * 这里只做查表和形状适配，所有派生结论（谁是谁的来源、置信度多少、哪条语句产出的）
 * 都由后端算好——前端再算一遍就会和单测里的断言分叉。
 */

/** dbobjs 是 server → database → schema → tables/views/others 的嵌套，摊平成一张表 */
function eachEntity(data, visit) {
  const servers = ((((data.sqlflow || {}).dbobjs) || {}).servers) || [];
  servers.forEach((server) => (server.databases || []).forEach((database) =>
    (database.schemas || []).forEach((schema) => {
      ['tables', 'views', 'others'].forEach((group) => {
        (schema[group] || []).forEach((entity) => visit(entity, schema, database));
      });
    })));
}

export function indexSqlFlow(data) {
  const payload = data || {};
  const sqlflow = payload.sqlflow || {};
  const elements = ((payload.graph || {}).elements) || {};
  const index = {
    data: payload,
    /** 模型标识 → 实体（物理表 / 视图 / 语句内中间站） */
    entities: new Map(),
    /** 实体标识（table）→ 模型标识：左栏选中的表要能对回图上的那个盒 */
    entityIdByName: new Map(),
    /** 模型标识 → 列，含被预算挡在画布外面的那些 */
    columns: new Map(),
    /** 列的完整标识（table.column）→ 模型标识 */
    columnIdByName: new Map(),
    relationships: new Map(),
    processes: new Map(),
    /** 图上标识 → 盒 / 行，值里带着服务端算好的落位（层号、同层序号） */
    boxes: new Map(),
    rows: new Map(),
    /** 列模型标识 → 图上标识：查不到就是没画进盒子，这是 uiVisible 的另一种写法 */
    rowIdOfColumn: new Map(),
    /** 图上边标识 → 这条边背后的 relationship 标识列表 */
    relationshipsOfEdge: new Map(),
  };

  eachEntity(payload, (entity) => {
    index.entities.set(entity.id, entity);
    index.entityIdByName.set(entity.qualifiedName, entity.id);
    (entity.columns || []).forEach((column) => {
      index.columns.set(column.id, Object.assign({ entityId: entity.id }, column));
      index.columnIdByName.set(column.qualifiedName, column.id);
    });
  });
  (sqlflow.processes || []).forEach((process) => index.processes.set(process.id, process));
  (sqlflow.relationships || []).forEach((rel) => index.relationships.set(rel.id, rel));

  (elements.tables || []).forEach((box) => {
    index.boxes.set(box.id, box);
    (box.columns || []).forEach((row) => {
      index.rows.set(row.id, Object.assign({ boxId: box.id }, row));
      /*
       * 表级关系行没有模型标识：往"列模型标识 → 图上标识"里塞一个 null 键，
       * 下一次带着空 target.id 回来的 relationship 就会查中它，把开场选中的那一行
       * 领到一个根本不是列的行上。这一行只归 rows 管。
       */
      if (row.modelId !== null && row.modelId !== undefined) {
        index.rowIdOfColumn.set(row.modelId, row.id);
      }
    });
  });
  index.relationshipsOfEdge = new Map(Object.entries((payload.graph || {}).relationshipIdMap || {}));
  return index;
}

/**
 * 一条 relationship 摊成 renderEdge 认的形状：多来源就是多条边。
 *
 * 表达式只挂在"进函数盒的那一段"上（后端口径），所以取不到时退回 relationship 自己的 transform，
 * 别让右栏的证据块出现"有链路、没表达式"的空位。
 */
export function evidenceOf(index, relationships) {
  const rows = [];
  (relationships || []).forEach((rel) => {
    const process = index.processes.get(rel.processId) || {};
    const sqlText = ((process.transforms || [])[0] || {}).code || '';
    (rel.sources || []).forEach((source) => {
      rows.push({
        from: source.qualifiedName,
        to: (rel.target || {}).qualifiedName,
        derivation: rel.derivation,
        confidence: rel.confidence,
        transform: ((source.transforms || [])[0] || {}).code || rel.transform,
        engine: rel.engine,
        jobId: process.qualifiedName,
        ordinal: rel.ordinal,
        hops: rel.hops,
        sqlText,
        parseError: !!process.parseError,
        effectType: rel.effectType,
      });
    });
  });
  return rows;
}

export function evidenceOfEdge(index, edgeId) {
  const ids = index.relationshipsOfEdge.get(edgeId) || [];
  return evidenceOf(index, ids.map((id) => index.relationships.get(id)).filter(Boolean));
}

/**
 * 一个列的来源与去向：端点带的是模型标识，全程按标识比，不去拼字符串。
 *
 * 只数这次请求里的关系——换中心表、换深度，来源数就会变，这不是 bug 而是口径：
 * 画得出来的因果才有边，画不出来的在 metaInfo.droppedSegments 里交代过。
 */
export function columnCard(index, columnModelId) {
  const column = index.columns.get(columnModelId);
  if (!column) {
    return null;
  }
  const rels = [...index.relationships.values()];
  const incoming = rels.filter((rel) => (rel.target || {}).id === columnModelId);
  const outgoing = rels.filter((rel) => (rel.sources || [])
    .some((source) => source.id === columnModelId));
  return {
    column: column.name,
    id: column.qualifiedName,
    modelId: columnModelId,
    entityId: column.entityId,
    uiVisible: !!column.uiVisible,
    sourceCount: incoming.length,
    consumerCount: outgoing.length,
    sources: evidenceOf(index, incoming),
    consumers: [...new Set(outgoing.map((rel) => (rel.target || {}).qualifiedName)
      .filter(Boolean))],
    unresolvedSource: incoming.some((rel) => rel.derivation === 'UNRESOLVED'),
  };
}

/**
 * 盒（实体）的列清单：右栏的"List Columns"要列得出没画进盒子的那些列。
 *
 * 这里的 {@code columns} 保持 detailPanel.renderTable 的字段口径（column/id/sourceCount/…），
 * 一套右栏渲染器读两种来源，不再为 sqlflow 复制一份 DOM。
 */
export function entityCard(index, entityModelId) {
  const entity = index.entities.get(entityModelId);
  if (!entity) {
    return null;
  }
  const box = [...index.boxes.values()].find((row) => row.modelId === entityModelId);
  return {
    table: entity.qualifiedName,
    name: entity.name,
    db: String(entity.qualifiedName || '').split('.')[0],
    layer: box ? box.layer : 0,
    local: !!entity.local,
    type: entity.type,
    /** 模型登记的列数与连登记都没进去的列数：画布说不上话，只有后端知道 */
    totalColumns: entity.columnCount,
    hiddenColumns: entity.hiddenColumns,
    columns: (entity.columns || []).map((column) => {
      const card = columnCard(index, column.id);
      return {
        column: column.name,
        id: column.qualifiedName,
        modelId: column.id,
        sourceCount: card.sourceCount,
        consumerCount: card.consumerCount,
        unresolvedSource: card.unresolvedSource,
        painted: !!column.uiVisible,
      };
    }),
  };
}

/** 列的完整标识 → 图上行标识：hash 里存的、左栏点来的都是完整标识 */
export function rowIdOfColumnId(index, qualifiedName) {
  const modelId = index.columnIdByName.get(qualifiedName);
  return modelId === undefined ? null : (index.rowIdOfColumn.get(modelId) || null);
}
