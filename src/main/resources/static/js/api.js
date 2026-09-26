const BASE = '/api';

function query(params) {
  const pairs = Object.entries(params)
    .filter(([, value]) => value !== undefined && value !== null && value !== '')
    .map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(value)}`);
  return pairs.length ? `?${pairs.join('&')}` : '';
}

/* 顶栏 2px 进度条：并发请求要计数，先回来的那个不能把条关掉 */
let inflight = 0;

function busy(delta) {
  inflight = Math.max(0, inflight + delta);
  document.body.classList.toggle('busy', inflight > 0);
}

async function request(path, options) {
  busy(1);
  let res;
  try {
    res = await fetch(BASE + path, options);
    let body;
    try {
      body = await res.json();
    } catch (error) {
      throw new Error(`${path} 返回非 JSON（HTTP ${res.status}）`);
    }
    if (!body.success) {
      throw new Error(body.message || `HTTP ${res.status}`);
    }
    return body.data;
  } finally {
    busy(-1);
  }
}

function json(body) {
  return {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  };
}

export const api = {
  overview: () => request('/overview'),
  tables: (params) => request(`/tables${query(params)}`),
  tableGraph: (params) => request(`/graph/table${query(params)}`),
  columns: (table) => request(`/table/${encodeURIComponent(table)}/columns`),
  issues: (type) => request(`/issues${query({ type })}`),
  parse: (sql) => request('/parse', json({ sql })),
  /** 字段级血缘：一次请求拿回数据模型 + 证据 + 网格落位（层号/同层序号），像素不在这份响应里 */
  sqlflowGraph: (params) => request(`/sqlflow/graph${query(params)}`),
  sqlflowParse: (sql, focus) => request('/sqlflow/graph', json({ sqltext: sql, focus })),
  scan: (dir) => request('/scan', json(dir ? { dir } : {})),
};
