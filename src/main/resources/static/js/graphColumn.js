import { EDGE_STYLE } from './badges.js';
import { capNotice, fit, nodePaint, replace, runLayout } from './graph.js';

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

  cy.nodes().forEach(nodePaint);
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
