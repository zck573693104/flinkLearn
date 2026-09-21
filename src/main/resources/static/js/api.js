const BASE = '/api';

function query(params) {
  const pairs = Object.entries(params)
    .filter(([, value]) => value !== undefined && value !== null && value !== '')
    .map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(value)}`);
  return pairs.length ? `?${pairs.join('&')}` : '';
}

async function request(path, options) {
  const res = await fetch(BASE + path, options);
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
  columnGraph: (params) => request(`/graph/column${query(params)}`),
  edge: (from, to) => request(`/edge/column${query({ from, to })}`),
  issues: (type) => request(`/issues${query({ type })}`),
  parse: (sql) => request('/parse', json({ sql })),
  scan: (dir) => request('/scan', json(dir ? { dir } : {})),
};
