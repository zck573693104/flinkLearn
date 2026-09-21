import { layerColor } from './badges.js';
import { capNotice, fit, replace, runLayout } from './graph.js';

/** 加工方式决定线的画法：低置信度的边必须在视觉上就和直通边区分开 */
const EDGE_STYLE = {
  UNRESOLVED: { 'line-color': '#e07a70', 'target-arrow-color': '#e07a70', 'line-style': 'dashed', width: 2 },
  STAR: { 'line-color': '#e0a870', 'target-arrow-color': '#e0a870', 'line-style': 'dotted', width: 2 },
  AGGREGATE: { 'line-color': '#c08a2e', 'target-arrow-color': '#c08a2e', width: 2.2 },
  EXPRESSION: { 'line-color': '#5b7fd4', 'target-arrow-color': '#5b7fd4' },
  CONSTANT: { 'line-color': '#a89bc4', 'target-arrow-color': '#a89bc4', 'line-style': 'dashed' },
  POSITIONAL: { 'line-color': '#6fa8ae', 'target-arrow-color': '#6fa8ae' },
};

/**
 * 字段级链路图：节点是 {@code table.column}，这就是"清晰展示字段来源"的那一屏。
 *
 * @param handlers.onColumn(columnId, table) 点列节点
 * @param handlers.onEdge(edgeData)          点边看证据
 */
export function drawColumns(cy, view, handlers) {
  const nodes = view.nodes || [];
  const edges = view.edges || [];
  const cap = capNotice(nodes.length);
  if (!cap.ok) {
    cy.elements().remove();
    return { warning: cap.warning, nodeCount: nodes.length, edgeCount: edges.length };
  }

  replace(cy, [
    ...nodes.map((node) => ({
      group: 'nodes',
      data: {
        id: node.id,
        label: node.column,
        column: node.column,
        table: node.table,
        name: node.name,
        local: !!node.local,
        layer: node.layer,
        kind: 'column',
      },
      classes: node.local ? 'local' : '',
    })),
    ...edges.map((edge) => ({
      group: 'edges',
      data: {
        id: edge.id,
        source: edge.from,
        target: edge.to,
        derivation: edge.derivation,
        confidence: edge.confidence,
        jobId: edge.jobId,
        hops: edge.hops,
        transform: edge.transform,
      },
      classes: edge.derivation || '',
    })),
  ]);

  cy.nodes().forEach((node) => {
    if (node.data('local')) {
      node.style('background-color', '#eceff4');
      node.style('color', '#4b5563');
      node.style('border-style', 'dashed');
      node.style('border-color', '#9aa3b2');
      return;
    }
    node.style('background-color', layerColor(node.data('layer')));
    node.style('color', '#fff');
  });
  Object.entries(EDGE_STYLE).forEach(([derivation, style]) => {
    cy.edges(`.${derivation}`).forEach((edge) => edge.style(style));
  });
  cy.edges().forEach((edge) => {
    edge.style('label', '');
    edge.on('mouseover', () => edge.style('label', edge.data('derivation')));
    edge.on('mouseout', () => edge.style('label', ''));
  });
  runLayout(cy, 'TB');
  fit(cy);
  bind(cy, handlers);
  return { warning: '', nodeCount: cy.nodes().size(), edgeCount: cy.edges().size() };
}

function bind(cy, handlers) {
  cy.off('tap');
  cy.on('tap', 'edge', (event) => handlers.onEdge(event.target.data()));
  cy.on('tap', 'node', (event) => {
    const data = event.target.data();
    handlers.onColumn(data.id, data.table);
  });
}

/** 让当前定位的列在图上显眼：字段视图经常是十几层链路里找一个点 */
export function markColumn(cy, columnId) {
  cy.nodes().removeClass('hot');
  if (!columnId) {
    return;
  }
  const node = cy.getElementById(columnId);
  if (node.nonempty()) {
    node.addClass('hot');
  }
}
