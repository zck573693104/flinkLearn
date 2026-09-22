/** 两张图共用的挂载、节点画法、布局与超限判断。 */

import { ink, layerColor } from './badges.js';

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
          'font-size': '11px',
          'font-family': 'Cascadia Mono, Consolas, monospace',
          'font-weight': 500,
          color: ink.nodeText,
          'text-valign': 'center',
          'text-halign': 'center',
          'text-wrap': 'ellipsis',
          'text-max-width': '150px',
          'background-opacity': 1,
          'border-color': '#33475d',
          'border-width': 1,
        },
      },
      {
        selector: 'edge',
        style: {
          width: 1.5,
          'line-color': ink.edgeDefault,
          'target-arrow-color': ink.edgeDefault,
          'target-arrow-shape': 'triangle',
          'curve-style': 'bezier',
          'font-size': '9.5px',
          'font-family': 'Cascadia Mono, Consolas, monospace',
          color: ink.localText,
          'text-background-color': '#05070c',
          'text-background-opacity': 0.85,
          'text-background-padding': '2px',
          label: '',
        },
      },
      /* 选中态不在这里：Cytoscape 的直接样式压过样式表，节点底色/描边是 nodePaint() 逐个写的，
         所以 .hot 写成样式表规则根本不会生效——高亮也必须走同一条直接样式路径，见 setHot()。 */
      { selector: '.faded', style: { opacity: 0.16 } },
    ],
  });
}

/**
 * 节点的基础外观：表级图和字段级图共用一份，避免两种图各写一遍颜色口径。
 *
 * 物理节点按层号取色、语句内中间关系压成暗底虚线框，都走直接样式；
 * 这里把 overlay 显式关掉，是给 setHot() 留出的可视余量。
 */
export function nodePaint(node) {
  const local = !!node.data('local');
  node.style({
    'background-color': local ? ink.localFill : layerColor(node.data('layer')),
    color: local ? ink.localText : ink.nodeText,
    'border-style': local ? 'dashed' : 'solid',
    'border-color': local ? ink.localLine : ink.nodeText,
    'border-opacity': local ? 1 : 0.55,
    'border-width': 1,
    'overlay-opacity': 0,
  });
}

/**
 * 只让当前定位的那个节点描酸绿边：class 在直接样式面前说话不算数，
 * 所以取消选中也要把基础外观重新写回去，而不是 removeClass 了事。
 */
export function setHot(cy, id) {
  cy.nodes().forEach((node) => {
    if (node.hasClass('hot')) {
      node.removeClass('hot');
      nodePaint(node);
    }
  });
  if (!id) {
    return false;
  }
  const node = cy.getElementById(id);
  if (!node.nonempty()) {
    return false;
  }
  node.addClass('hot');
  node.style({
    'border-width': 3,
    'border-color': ink.hot,
    'border-opacity': 1,
  });
  return true;
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
