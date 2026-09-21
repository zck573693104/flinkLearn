/** 加工方式的中文提示：v1 不展开星号、只标置信度，不在前端重新推断语义。 */
const DERIVATION_HINT = {
  IDENTITY: '直通',
  EXPRESSION: '表达式',
  AGGREGATE: '聚合',
  CONSTANT: '常量',
  POSITIONAL: '按位置对齐',
  UNNAMED: '未命名列',
  STAR: '星号未展开',
  UNRESOLVED: '来源未绑定',
};

const LAYER_COLORS = ['#4c8bf5', '#2ca089', '#d4a017', '#b5651d', '#8e44ad', '#5b6472'];

export function derivationLabel(derivation) {
  return DERIVATION_HINT[derivation] || derivation || '未知';
}

export function badge(text, cls) {
  const span = document.createElement('span');
  span.className = `badge ${cls || ''}`.trim();
  span.textContent = text;
  return span;
}

export function derivationBadge(derivation) {
  const span = badge(`${derivation || '?'} · ${derivationLabel(derivation)}`, derivation);
  if (derivation === 'UNRESOLVED') {
    span.title = '来源绑定失败：这条边的两端不保证真实存在，请配合 /api/issues 人工确认';
  }
  if (derivation === 'STAR') {
    span.title = 'v1 按约定不展开 SELECT *，字段级链路到此为止';
  }
  return span;
}

export function confidenceText(derivation, confidence) {
  const percent = Math.round((confidence || 0) * 100);
  return `${percent}%（${derivation || '?'}）`;
}

export function layerColor(layer) {
  const index = Number.isFinite(layer) ? Math.max(0, Math.round(layer)) : 0;
  return LAYER_COLORS[index % LAYER_COLORS.length];
}

export function confidenceClass(confidence) {
  if (confidence >= 0.9) return 'high';
  if (confidence >= 0.7) return 'mid';
  return 'low';
}
