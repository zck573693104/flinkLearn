import { capNotice, fit, nodePaint, replace, runLayout } from './graph.js';

/**
 * 表级分层 DAG。
 *
 * @param handlers.onTable(tableId) 点表节点：左栏联动 + 换字段视图
 */
export function drawTable(cy, view, handlers) {
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
        label: node.name,
        table: node.id,
        db: node.db,
        layer: node.layer,
        colCount: node.colCount,
        kind: 'table',
      },
    })),
    ...edges.map((edge) => ({
      group: 'edges',
      data: { id: edge.id, source: edge.from, target: edge.to, jobId: edge.jobId },
    })),
  ]);
  cy.nodes().forEach((node) => {
    nodePaint(node);
    node.style('text-max-width', '170px');
  });
  runLayout(cy, 'LR');
  fit(cy);
  bind(cy, handlers);
  return { warning: '', nodeCount: cy.nodes().size(), edgeCount: cy.edges().size() };
}

function bind(cy, handlers) {
  cy.off('tap');
  cy.on('tap', 'node', (event) => handlers.onTable(event.target.data('id')));
  cy.on('tap', 'edge', (event) => handlers.onTableEdge(event.target.data()));
  cy.on('tap', (event) => {
    if (event.target === cy) {
      handlers.onBlank();
    }
  });
}
