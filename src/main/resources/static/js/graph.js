/** 表级 DAG 的挂载、节点画法、分层排布与超限判断。字段链路在 sqlflowView.js，它不走这里。 */

import { ink, layerColor } from './badges.js';

export const NODE_CAP = 300;

/**
 * 标签的字体与"还能读"的下限。
 *
 * 画布文字跟着 zoom 缩放，所以小容器里必须先保住字号，而不是保住整图：
 * 整图看得见但字读不出，等于没有图。
 */
const NODE_FONT_PX = 11;
const MIN_LABEL_PX = 10;

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
          label: 'data(label)',
          'font-size': `${NODE_FONT_PX}px`,
          'font-family': 'Cascadia Mono, Consolas, monospace',
          'font-weight': 500,
          color: ink.nodeText,
          'text-valign': 'center',
          'text-halign': 'center',
          'text-wrap': 'ellipsis',
          'text-max-width': '150px',
          'background-opacity': 1,
          'border-color': ink.nodeLine,
          'border-width': 1,
          'z-index': 2,
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
          'text-background-color': ink.canvasBg,
          'text-background-opacity': 0.85,
          'text-background-padding': '2px',
          label: '',
          'z-index': 1,
        },
      },
      /* 选中态不在这里：Cytoscape 的直接样式压过样式表，节点底色/描边是 nodePaint() 逐个写的，
         所以 .hot 写成样式表规则根本不会生效——高亮也必须走同一条直接样式路径，见 setHot()。 */
    ],
  });
}

/**
 * 节点的基础外观。
 *
 * 物理节点按层号取色、语句内的中间关系压成暗底虚线框。层号取色只有"实体"节点参与，
 * 图例上第几层是什么颜色，这里就是什么颜色。
 * 这里把 overlay 显式关掉，是给 setHot() 留出可视余量。
 */
export function nodePaint(node) {
  const local = !!node.data('local');
  const layer = layerColor(node.data('layer'));
  node.style({
    'background-color': local ? ink.localFill : layer,
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

/** 列间距（沿层方向）与同层内间距，LR/TB 各用一套 */
const GAP = {
  LR: { rank: 110, node: 16 },
  TB: { rank: 80, node: 26 },
};
const FIT_PADDING = 24;

function num(value) {
  return parseFloat(value) || 0;
}

/** 按 layer 分列：层号缺失一律按第 0 层 */
function groupByLayer(nodes) {
  const columns = new Map();
  nodes.forEach((node) => {
    const layer = Number.isFinite(node.data('layer')) ? node.data('layer') : 0;
    if (!columns.has(layer)) {
      columns.set(layer, []);
    }
    columns.get(layer).push(node);
  });
  return [...columns.keys()].sort((a, b) => a - b).map((layer) => columns.get(layer));
}

/**
 * 同层内的行序：按"已排定的前一层邻居"的平均行号排（重心法一趟）。
 * 前一层没排过的节点（链路起点、孤岛、同层入边）留在原位，不打乱已有顺序。
 */
function orderWithinColumns(columns) {
  const row = new Map();
  columns.forEach((group, index) => {
    const ranked = group
      .map((node, position) => {
        if (index === 0) {
          return { node, key: position };
        }
        const sources = node.incomers('node').map((prev) => prev.id());
        const before = sources.filter((id) => row.has(id)).map((id) => row.get(id));
        return {
          node,
          key: before.length ? before.reduce((a, b) => a + b, 0) / before.length : position,
        };
      })
      .sort((a, b) => a.key - b.key);
    ranked.forEach((entry, position) => row.set(entry.node.id(), position));
    group.splice(0, group.length, ...ranked.map((entry) => entry.node));
  });
}

function sizeAlong(node, direction) {
  return num(node.style(direction === 'TB' ? 'height' : 'width'));
}

function sizeCross(node, direction) {
  return num(node.style(direction === 'TB' ? 'width' : 'height'));
}

/** 只量尺寸不落位，用于两个朝向里挑一个放得大的 */
function measure(columns, direction) {
  let along = 0;
  let cross = 0;
  columns.forEach((group) => {
    along += Math.max(...group.map((node) => sizeAlong(node, direction))) + GAP[direction].rank;
    cross = Math.max(cross, group.reduce(
      (sum, node) => sum + sizeCross(node, direction) + GAP[direction].node,
      GAP[direction].node,
    ));
  });
  return direction === 'TB' ? { w: cross, h: along } : { w: along, h: cross };
}

/** 落位：列 = 层，列内按行号堆叠，短列在跨轴上居中对齐 */
function place(columns, direction) {
  const box = measure(columns, direction);
  const crossTotal = direction === 'TB' ? box.w : box.h;
  let alongCursor = 0;
  columns.forEach((group) => {
    const depth = Math.max(...group.map((node) => sizeAlong(node, direction)));
    const span = group.reduce(
      (sum, node) => sum + sizeCross(node, direction) + GAP[direction].node,
      GAP[direction].node,
    ) - GAP[direction].node;
    let crossCursor = (crossTotal - span) / 2;
    group.forEach((node) => {
      const cross = sizeCross(node, direction);
      if (direction === 'TB') {
        node.position('x', crossCursor + cross / 2);
        node.position('y', alongCursor + depth / 2);
      } else {
        node.position('x', alongCursor + depth / 2);
        node.position('y', crossCursor + cross / 2);
      }
      crossCursor += cross + GAP[direction].node;
    });
    alongCursor += depth + GAP[direction].rank;
  });
}

/**
 * 分层排布：列号直接用服务端算好的 layer，不再交给 dagre 重排。
 *
 * 语料实测：6 条互不相干的表边被 dagre 按连通分量摊成 8 列 1653×200，
 * fit 到中间栏只剩 0.39 缩放，字号 4px、线宽 0.6px——数据全对但看上去"没有图"；
 * 而且下游节点会排到上游左边，跟"第几层"图例直接矛盾。层号本来就是这套图的语义。
 * 朝向按容器实际尺寸挑：谁的 fit 缩放大就用谁，避免长条图被压成一条线。
 */
export function runLayout(cy) {
  const columns = groupByLayer(cy.nodes());
  if (!columns.length) {
    return;
  }
  orderWithinColumns(columns);
  const container = { w: cy.width(), h: cy.height() };
  const zoomOf = (direction) => {
    const box = measure(columns, direction);
    return Math.min(
      container.w / (box.w + FIT_PADDING * 2),
      container.h / (box.h + FIT_PADDING * 2),
    );
  };
  const horizontal = zoomOf('LR');
  const vertical = zoomOf('TB');
  place(columns, Number.isFinite(vertical) && vertical > horizontal ? 'TB' : 'LR');
}

export function replace(cy, elements) {
  cy.elements().remove();
  cy.add(elements);
  cy.nodes().forEach((node) => {
    // 宽度必须按"真正画出来的那个串"算：标签样式表取的就是 data(label)，
    // 按短名算宽度却画全名，字就溢出色块压到邻居身上。
    const label = node.data('label') || node.data('id');
    node.style('width', Math.min(190, 12 + label.length * 6.4));
    node.style('height', 34);
  });
}

/**
 * 装得下就整图铺满，装不下就先把字保住。
 *
 * 语料实测：中间栏 516x192 时整图 fit 完只剩 0.372 倍，11px 的标签被压成 4px——
 * 节点点得中、右栏也出得来表信息，但图上读不出字，用户看到的就是"没有字"。
 * 于是低于可读下限时改为放大到下限并左上对齐：链路起点先入眼，
 * 超出容器的部分交给拖动画布（minZoom 仍允许用户自己缩回去看整体形状）。
 */
export function fit(cy) {
  const nodes = cy.nodes();
  if (!nodes.length) {
    return { zoom: cy.zoom(), cramped: false, warning: '' };
  }
  /* cytoscape 把容器尺寸缓存在自己手里，DOM 变了它不认（实测画布从 500x300 改成 700x400，
     cy.width() 照旧报 500x300，只有显式 resize() 才刷新）。fit 读的就是这个缓存，
     所以每次 framing 前自己去刷一遍——否则缩放和"装不下"的结论都是对着旧盒子算出来的。 */
  cy.resize();
  cy.fit(nodes, FIT_PADDING);
  /* 表级节点一律带标签、也不覆写字号，所以可读下限直接拿 NODE_FONT_PX 换算就行。 */
  const floor = Math.max(cy.minZoom(), MIN_LABEL_PX / NODE_FONT_PX);
  if (cy.zoom() >= floor) {
    return { zoom: cy.zoom(), cramped: false, warning: '' };
  }
  cy.zoom(floor);
  const box = nodes.boundingBox();
  cy.pan({ x: FIT_PADDING - box.x1 * floor, y: FIT_PADDING - box.y1 * floor });
  const canvas = `${Math.round(cy.width())}x${Math.round(cy.height())}`;
  return {
    zoom: floor,
    cramped: true,
    warning: `画布 ${canvas} 放不下 ${nodes.length} 个节点：已放大到 ${floor.toFixed(2)} 倍保住标签，`
      + '整图请拖动画布或滚轮缩小',
  };
}

/**
 * 容器尺寸变了就重新 framing，并把可读性结论回灌给调用方。
 *
 * 实测过：首屏 fit 时画布 516x288，兄弟节点落位后只剩 516x272——缩放是对着旧盒子算的，
 * "能不能放下整图"这个判断也就跟着偏。节点为空时不动（超限告警还挂在那儿，不能被抹掉）。
 * 重新 fit 之前由 fit() 自己刷 cytoscape 的容器尺寸缓存，这里不额外插手。
 */
export function watchResize(cy, onFrame) {
  if (!window.ResizeObserver) {
    return () => {};
  }
  const observer = new ResizeObserver(() => {
    if (!cy.nodes().length) {
      return;
    }
    onFrame(fit(cy).warning);
  });
  observer.observe(cy.container());
  return () => observer.disconnect();
}

/**
 * 超上限时不去渲染上千个方块——语料里有单表 787 列的情况，
 * 全画出来浏览器直接卡住，而且用户也看不清。
 *
 * 数的是什么由调用方说：表级图数节点，字段级图数表盒（列已经收进盒子里了）。
 * 报错误的单位必须和判断用的单位一致，否则"N 个"对不上用户看得见的东西。
 */
export function capNotice(count, unit) {
  return count <= NODE_CAP
    ? { ok: true, warning: '' }
    : {
      ok: false,
      warning: `${unit || '节点'} ${count} 个，超过 ${NODE_CAP} 上限：请先选中具体表/字段或调小深度`,
    };
}
