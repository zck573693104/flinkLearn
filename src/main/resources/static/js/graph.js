/** 两张图共用的挂载、布局与超限判断。 */

export const NODE_CAP = 300;

/** dagre 由 vendor 脚本自动注册；万一没注册成功就退回内置 breadthfirst，图仍然可看 */
let layoutName = 'dagre';

export function create(el) {
  return window.cytoscape({
    container: el,
    minZoom: 0.15,
    maxZoom: 2.5,
    wheelSensitivity: 0.25,
    style: [
      {
        selector: 'node',
        style: {
          shape: 'round-rectangle',
          'font-size': '10px',
          color: '#22272f',
          'text-valign': 'center',
          'text-halign': 'center',
          'text-wrap': 'ellipsis',
          'text-max-width': '150px',
          'background-opacity': 1,
          'border-color': '#8a92a0',
          'border-width': 1,
        },
      },
      {
        selector: 'edge',
        style: {
          width: 1.6,
          'line-color': '#9aa3b2',
          'target-arrow-color': '#9aa3b2',
          'target-arrow-shape': 'triangle',
          'curve-style': 'bezier',
          'font-size': '9px',
          color: '#6b7280',
          label: '',
        },
      },
      { selector: ':selected', style: { 'border-width': 3, 'border-color': '#2f6fdd' } },
      { selector: '.hot', style: { 'border-width': 3, 'border-color': '#d4a017' } },
      { selector: '.faded', style: { opacity: 0.22 } },
    ],
  });
}

function spec(depthDir) {
  if (layoutName === 'dagre') {
    return {
      name: 'dagre',
      rankdir: depthDir,
      nodesep: depthDir === 'LR' ? 14 : 24,
      ranksep: depthDir === 'LR' ? 90 : 70,
      ranker: 'network-simplex',
      avoidOverlap: true,
      animate: false,
      padding: 24,
    };
  }
  return {
    name: 'breadthfirst',
    directed: true,
    fit: true,
    padding: 24,
    spacingFactor: 1.2,
    animate: false,
  };
}

/** 布局异常只降级不抛错：布局挂了不该让整张图消失 */
export function runLayout(cy, direction) {
  try {
    const layout = cy.layout(spec(direction));
    layout.run();
  } catch (error) {
    if (layoutName === 'dagre') {
      layoutName = 'breadthfirst';
      runLayout(cy, direction);
      return;
    }
    throw error;
  }
}

export function replace(cy, elements) {
  cy.elements().remove();
  cy.add(elements);
  cy.nodes().forEach((node) => {
    const label = node.data('label') || node.data('id');
    node.style('width', Math.min(190, 12 + label.length * 6.4));
    node.style('height', node.data('kind') === 'column' ? 24 : 34);
  });
}

export function fit(cy) {
  if (cy.nodes().length) {
    cy.fit(undefined, 24);
  }
}

/**
 * 节点数超上限时不去渲染上千个方块——语料里有单表 783 列的情况，
 * 全画出来浏览器直接卡住，而且用户也看不清。
 */
export function capNotice(nodeCount) {
  return nodeCount <= NODE_CAP
    ? { ok: true, warning: '' }
    : {
      ok: false,
      warning: `节点 ${nodeCount} 个，超过 ${NODE_CAP} 上限：请先选中具体表/字段或调小深度`,
    };
}
