/** 两张图共用的挂载、节点画法、分层排布与超限判断。 */

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

/**
 * 字段视图的"表盒 + 列行"尺寸。
 *
 * 盒子不是 cytoscape 的 compound 父节点：它的宽高由行数算出来直接写进样式，
 * 列行是同层级的普通节点、由 stackRows() 摆进盒子里。自算布局本来就掌握全部坐标，
 * 用 compound 反而要把坐标换成"相对父节点"的口径，还要跟父框自动外扩较劲。
 *
 * 表名也不写在盒子上：写在盒子上的字要么压住第一行列行，要么靠 text-margin-y 猜位置。
 * 改成盒子顶部一条实色"标题行"节点，色块自己就是分隔线。
 */
export const BOX_W = 178;
/** 行距（pitch）：比行色块高，多出来的缝让相邻两行不至于糊成一片 */
export const ROW_H = 20;
export const ROW_BOX_H = 17;
export const HEADER_H = 24;
export const BOX_PAD = 8;
export const ROW_W = BOX_W - 14;
export const HEADER_W = BOX_W - 6;

/** 画盒子/行的那套 kind，其余（表级图）走通用外观 */
const BOX_KINDS = '[kind="tableBox"], [kind="hopBox"]';

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
      /* 盒子垫底、线走在中间、行浮在上面：边的端点被 cytoscape 裁到节点边界，
         所以线只会在盒体上走明路，不会被色块盖掉半截。 */
      {
        selector: BOX_KINDS,
        style: {
          label: '',
          'background-opacity': 1,
          'overlay-opacity': 0,
          'z-index': 0,
        },
      },
      {
        selector: 'node[kind="headerRow"]',
        style: {
          shape: 'rectangle',
          'font-weight': 700,
          'text-wrap': 'ellipsis',
          'text-max-width': `${HEADER_W - 10}px`,
          'background-opacity': 1,
          'overlay-opacity': 0,
          'z-index': 3,
        },
      },
      {
        selector: 'node[kind="columnRow"]',
        style: {
          shape: 'rectangle',
          'text-wrap': 'ellipsis',
          'text-max-width': `${ROW_W - 8}px`,
          'background-opacity': 1,
          'overlay-opacity': 0,
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
 * 节点的基础外观：表级图和字段级图共用一份，避免两种图各写一遍颜色口径。
 *
 * 物理节点按层号取色、语句内中间关系压成暗底虚线框，都走直接样式；
 * 盒/行/标题行各有各的口径，但层号取色这件事只有"实体"节点参与——
 * 盒体永远是最暗的容器，层号写在标题行上，这样图例的第几层仍然读得出来。
 * 这里把 overlay 显式关掉，是给 setHot() 留出的可视余量。
 */
export function nodePaint(node) {
  const kind = node.data('kind');
  const local = !!node.data('local');
  const layer = layerColor(node.data('layer'));
  if (kind === 'tableBox' || kind === 'hopBox') {
    node.style({
      'background-color': ink.boxFill,
      'border-style': kind === 'hopBox' || local ? 'dashed' : 'solid',
      'border-color': kind === 'hopBox' || local ? ink.localLine : layer,
      'border-opacity': 1,
      'border-width': 1.5,
      'overlay-opacity': 0,
    });
    return;
  }
  if (kind === 'headerRow') {
    node.style({
      'background-color': local ? ink.localFill : layer,
      color: local ? ink.localText : ink.nodeText,
      'border-style': local ? 'dashed' : 'solid',
      'border-color': local ? ink.localLine : layer,
      'border-opacity': 1,
      'border-width': 1,
      'overlay-opacity': 0,
    });
    return;
  }
  if (kind === 'columnRow') {
    const note = !!node.data('capped');
    node.style({
      'background-color': note ? ink.boxFill : ink.rowFill,
      color: note ? ink.localText : ink.rowText,
      'border-width': note ? 0 : 1,
      'border-style': 'solid',
      'border-color': ink.rowLine,
      'border-opacity': 1,
      'overlay-opacity': 0,
    });
    return;
  }
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

/** 按 layer 分列：层号缺失一律按第 0 层，非整数层（中间跳占位）自成一列 */
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
 *
 * peers 是盒子视图给的"上游盒"表：字段视图的边连的是行，盒与盒之间没有边，
 * 直接走 incomers() 会把行当成邻居、把盒当成孤岛，重心全丢。
 */
function orderWithinColumns(columns, peers) {
  const row = new Map();
  columns.forEach((group, index) => {
    const ranked = group
      .map((node, position) => {
        if (index === 0) {
          return { node, key: position };
        }
        const sources = peers
          ? [...(peers.get(node.id()) || [])]
          : node.incomers('node').map((prev) => prev.id());
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

/** 行级边折算成盒级邻接：重心法要的是"我这盒的上游有哪些盒" */
function boxPeers(cy) {
  const owner = new Map();
  cy.nodes('[kind="columnRow"], [kind="headerRow"]').forEach((node) => {
    owner.set(node.id(), node.data('box'));
  });
  const upstream = new Map();
  cy.edges().forEach((edge) => {
    const from = owner.get(edge.data('source'));
    const to = owner.get(edge.data('target'));
    if (!from || !to || from === to) {
      return;
    }
    if (!upstream.has(to)) {
      upstream.set(to, new Set());
    }
    upstream.get(to).add(from);
  });
  return upstream;
}

/** 盒子落位后把标题行和列行填进去：行坐标由盒子的左上角推，不指望布局顺手摆 */
function stackRows(cy) {
  const byBox = new Map();
  const push = (node) => {
    const id = node.data('box');
    if (!byBox.has(id)) {
      byBox.set(id, []);
    }
    byBox.get(id).push(node);
  };
  cy.nodes('[kind="headerRow"]').forEach(push);
  cy.nodes('[kind="columnRow"]').forEach(push);
  cy.nodes(BOX_KINDS).forEach((box) => {
    const members = byBox.get(box.id()) || [];
    const top = box.position('y') - num(box.style('height')) / 2;
    members
      .sort((a, b) => num(a.data('order')) - num(b.data('order')))
      .forEach((node) => {
        const band = node.data('kind') === 'headerRow'
          ? top + HEADER_H / 2
          : top + HEADER_H + (num(node.data('order')) - 1) * ROW_H + ROW_BOX_H / 2;
        node.position('x', box.position('x'));
        node.position('y', band);
      });
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
 *
 * 字段视图排的是盒子（一表一盒），行列由 stackRows() 填进各自盒子里；
 * 没有盒子时（表级图）排的就是节点本身。
 */
export function runLayout(cy) {
  const boxes = cy.nodes(BOX_KINDS);
  const columns = groupByLayer(boxes.length ? boxes : cy.nodes());
  if (!columns.length) {
    return;
  }
  orderWithinColumns(columns, boxes.length ? boxPeers(cy) : null);
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
  if (boxes.length) {
    stackRows(cy);
  }
}

export function replace(cy, elements) {
  cy.elements().remove();
  cy.add(elements);
  cy.nodes().forEach((node) => {
    const kind = node.data('kind');
    if (kind === 'tableBox' || kind === 'hopBox') {
      node.style({
        width: BOX_W,
        height: HEADER_H + (node.data('rows') || 0) * ROW_H + BOX_PAD,
      });
      return;
    }
    if (kind === 'headerRow') {
      node.style({ width: HEADER_W, height: HEADER_H - 4 });
      return;
    }
    if (kind === 'columnRow') {
      node.style({ width: ROW_W, height: ROW_BOX_H });
      return;
    }
    // 宽度必须按"真正画出来的那个串"算：标签样式表取的就是 data(label)，
    // 按短名算宽度却画全名（字段视图两边差的正是表前缀），字就溢出色块压到邻居身上。
    const label = node.data('label') || node.data('id');
    node.style('width', Math.min(190, 12 + label.length * 6.4));
    node.style('height', kind === 'column' ? 24 : 34);
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
  cy.fit(undefined, FIT_PADDING);
  /* 可读下限按"最小的那个有字的节点"算：字段视图里盒子不写字，拿 nodes[0]（通常是盒子）
     来定字号等于用空气做基准。 */
  let fontPx = Infinity;
  nodes.forEach((node) => {
    if (node.data('label')) {
      fontPx = Math.min(fontPx, parseFloat(node.style('font-size')) || NODE_FONT_PX);
    }
  });
  if (!Number.isFinite(fontPx)) {
    fontPx = NODE_FONT_PX;
  }
  const floor = Math.max(cy.minZoom(), MIN_LABEL_PX / fontPx);
  if (cy.zoom() >= floor) {
    return { zoom: cy.zoom(), cramped: false, warning: '' };
  }
  cy.zoom(floor);
  const box = cy.elements().boundingBox();
  cy.pan({ x: FIT_PADDING - box.x1 * floor, y: FIT_PADDING - box.y1 * floor });
  const canvas = `${Math.round(cy.width())}x${Math.round(cy.height())}`;
  // 报"多少个"要报用户眼里的那个数：字段视图的盒体和空盒子不算信息量
  const labeled = nodes.filter((node) => node.data('label')).length || nodes.length;
  return {
    zoom: floor,
    cramped: true,
    warning: `画布 ${canvas} 放不下 ${labeled} 个节点：已放大到 ${floor.toFixed(2)} 倍保住标签，`
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
