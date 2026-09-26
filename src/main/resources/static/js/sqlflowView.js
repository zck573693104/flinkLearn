/**
 * 字段级血缘的渲染层：DOM 摆盒子，一层 SVG 走线，不碰 cytoscape。
 *
 * 为什么不画在 canvas 上：这张图的全部信息量就是"一表一盒、一行一字段"，字段名必须
 * 一个字符一个字符读得出来。canvas 上的字要跟着缩放走，缩放又要 fit 容器，于是永远
 * 在"放不下"和"字太小"之间二选一——上一版为这件事写了 350 行几何兜底，最后还是被
 * 反馈成"图上看不到字"。交给浏览器排版之后，盒高等于内容高，字号恒定，
 * 缩放是把整张纸放大，而不是把字压小。
 *
 * 服务端只给网格（layer = 第几层，slot = 同层第几个），像素在这里量：
 * 先落 left 读 offsetHeight，再按列累加落 top，全部量完才画线。
 * 读必须成批读、写必须成批写——读写交替会每一格都强制重排一次。
 *
 * 交互是两步：悬停只在原地染色（不动布局），要不要"只留这一列"由用户点工具条。
 * 上一版是点一下字段就自动裁剪，用户找不到自己刚才在看的那片图。
 */

import { edgeInk, ink, layerColor, SYNTHETIC_EDGE } from './badges.js';

/** 层与层之间沿 X 的路：线要在这一段里拐弯。盒宽不在这儿——它只有 CSS 知道，量出来的 */
const COL_GAP = 92;
/** 同层盒与盒的缝 */
const ROW_GAP = 18;
/** 整张图四周留白，也是 fit 留的边距 */
const PAD = 26;
/** 12px × 0.8 = 9.6px，再小就不是"能读"而是"有个形状" */
const READABLE_Z = 0.8;
const ZOOM_MIN = 0.3;
const ZOOM_MAX = 2.5;
const ZOOM_STEPS = [0.3, 0.4, 0.5, 0.62, 0.75, 0.9, 1, 1.15, 1.35, 1.6, 1.9, 2.2, 2.5];

const SVG_NS = 'http://www.w3.org/2000/svg';

function el(tag, cls, text) {
  const node = document.createElement(tag);
  if (cls) {
    node.className = cls;
  }
  if (text !== undefined && text !== null) {
    node.textContent = text;
  }
  return node;
}

function svg(tag, attrs) {
  const node = document.createElementNS(SVG_NS, tag);
  Object.keys(attrs || {}).forEach((key) => node.setAttribute(key, attrs[key]));
  return node;
}

/** 箭头按线色各配一个 marker：SVG 的 marker 不继承 stroke，颜色是 #rrggbb 时不能直接当 id */
function markerKey(color) {
  return `slf-ah-${String(color).replace(/[^0-9a-zA-Z]/g, '')}`;
}

/**
 * 一次请求摊成渲染要的三张表。
 *
 * 加工方式（derivation）决定线色，它在 sqlflow.relationships 里，图上标识只挂在
 * graph.relationshipIdMap 上——这里一次合好，渲染循环里不再穿三层。
 */
function readPayload(data) {
  const payload = data || {};
  const elements = ((payload.graph || {}).elements) || {};
  const relationships = new Map();
  (((payload.sqlflow || {}).relationships) || []).forEach((rel) => {
    relationships.set(rel.id, rel);
  });
  const idMap = (payload.graph || {}).relationshipIdMap || {};
  return {
    tables: elements.tables || [],
    edges: (elements.edges || []).map((edge) => {
      const ids = idMap[edge.id] || [];
      const rel = ids.length ? relationships.get(ids[0]) : null;
      return Object.assign({}, edge, { derivation: rel ? rel.derivation : null });
    }),
    meta: payload.metaInfo || {},
  };
}

export function mountSqlFlow(host, data, handlers) {
  const model = readPayload(data);
  host.textContent = '';
  if (!model.meta.drawn || !model.tables.length) {
    return idleView(model.meta.warning
      || '这份血缘没有可画的盒子：请选中具体表或字段，或调小深度');
  }

  const columnOf = new Map();
  model.tables.forEach((box) => (box.columns || []).forEach((column) => {
    columnOf.set(column.id, column);
  }));
  const tableOf = new Map(model.tables.map((box) => [box.id, box]));

  const scroll = el('div', 'slf-scroll');
  const sizer = el('div', 'slf-sizer');
  const stage = el('div', 'slf-stage');
  const sheet = svg('svg', { class: 'slf-wires' });
  const defs = svg('defs');
  const flock = svg('g');
  sheet.append(defs, flock);
  stage.appendChild(sheet);
  sizer.appendChild(stage);
  scroll.appendChild(sizer);
  host.append(scroll);

  /** 已经建过 arrow 的线色：一个颜色一个 marker，重复建会在 defs 里堆一堆同名节点 */
  const markers = new Set();

  function arrowMarker(color) {
    const key = markerKey(color);
    if (!markers.has(key)) {
      const marker = svg('marker', {
        id: key, viewBox: '0 0 8 8', refX: 7, refY: 4, markerWidth: 8, markerHeight: 8,
        markerUnits: 'userSpaceOnUse', orient: 'auto',
      });
      marker.appendChild(svg('path', { d: 'M0 0 L8 4 L0 8 Z', fill: color }));
      defs.appendChild(marker);
      markers.add(key);
    }
    return key;
  }

  /* 盒与行都按 payload 标识登记：线的一端可能是行，也可能是没画进那一行的盒子 */
  const nodes = new Map();
  const boxes = [];

  model.tables.forEach((box) => {
    const node = el('div', `slf-box${box.local ? ' local' : ''}`);
    node.dataset.node = box.id;
    const head = el('div', 'slf-head');
    head.append(el('span', 'slf-name', box.name || box.qualifiedName),
      el('span', 'slf-layer', `L${box.layer || 0}`));
    head.title = `${box.qualifiedName || ''}${box.type ? ` · ${box.type}` : ''}`
      + '（点表名：以这张表为中心重开一片）';
    // 层号取色只有 JS 知道（色板在 CSS 变量里，badges.js 读同一份），中间站不参与
    if (!box.local) {
      head.style.background = layerColor(box.layer);
    }
    node.appendChild(head);
    const placed = { id: box.id, kind: 'box', layer: box.layer || 0, slot: box.slot || 0,
      el: node, head: head, local: !!box.local, x: 0, y: 0, w: 0, h: 0, headCy: 12 };
    boxes.push(placed);
    nodes.set(box.id, placed);
    (box.columns || []).forEach((column) => {
      const relation = column.kind === 'relation';
      const row = el('div', relation ? 'slf-row rel' : 'slf-row');
      row.dataset.node = column.id;
      row.append(el('span', 'slf-col', column.name));
      row.title = relation
        ? '表级关系行：相连的虚线是「只有表级关系、没有字段级血缘」的表对，点虚线看语句'
        : `${column.qualifiedName}（点一下看这一列的上下游）`;
      node.appendChild(row);
      nodes.set(column.id, { id: column.id, kind: 'row', boxId: box.id, el: row,
        rel: relation, ox: 0, oy: 0, ow: 0, oh: 16 });
    });
    stage.appendChild(node);
  });

  const links = model.edges.map((edge) => {
    const from = nodes.get(edge.sourceId);
    const to = nodes.get(edge.targetId);
    if (!from || !to) {
      return null;
    }
    /*
     * 表级关系边（RelationRows 行之间的虚线）背后没有 derivation：它是"这对表发生过
     * 关系"的最低表述，一律默认灰 + 长虚线，不和任何加工方式的真因果撞色。
     */
    const style = edge.kind === 'tableRel'
      ? Object.assign({}, edgeInk(null), { dash: '6 4' })
      : edgeInk(edge.derivation);
    const group = svg('g', { class: 'slf-edge' });
    const line = svg('path', {
      fill: 'none',
      stroke: style.color,
      'stroke-width': edge.synthetic ? SYNTHETIC_EDGE.width : style.width,
      'stroke-dasharray': edge.synthetic ? SYNTHETIC_EDGE.dash : (style.dash || 'none'),
      'marker-end': `url(#${arrowMarker(style.color)})`,
    });
    /* 1.4px 的线点不中：同一条路径再描一遍宽的、只用来接点击 */
    const hit = svg('path', { class: 'slf-hit', fill: 'none', 'data-edge': edge.id });
    group.append(line, hit);
    flock.appendChild(group);
    return { edge, group, line, hit, from, to };
  }).filter(Boolean);

  const view = {
    framing: '',
    modelWarning: model.meta.warning || '',
    cropped: false,
  };

  const zoom = createZoom(scroll, sizer, stage, (text) => {
    view.framing = text;
    handlers.onFrame(text);
  });
  // 缩放控件挂在 host 上而不是 scroll 上：挂在滚动容器里，一滚它就跟着跑掉了
  host.appendChild(zoom.node);

  /* ---------------- 量布、落位、画线 ---------------- */

  function shown(node) {
    return node.el.style.display !== 'none';
  }

  function owner(node) {
    return node.kind === 'row' ? nodes.get(node.boxId) : node;
  }

  /** 量 → 落位 → 画线：裁剪会改盒高，所以这三步必须一起重来 */
  function layout() {
    const live = boxes.filter(shown);
    live.forEach((box) => {
      box.h = box.el.offsetHeight;
      box.w = box.el.offsetWidth;
      box.headCy = box.head.offsetTop + box.head.offsetHeight / 2;
    });
    nodes.forEach((node) => {
      if (node.kind === 'row' && shown(node) && shown(owner(node))) {
        node.ox = node.el.offsetLeft;
        node.oy = node.el.offsetTop;
        node.ow = node.el.offsetWidth;
        node.oh = node.el.offsetHeight;
      }
    });

    const columns = new Map();
    live.forEach((box) => {
      const column = columns.get(box.layer) || [];
      column.push(box);
      columns.set(box.layer, column);
    });
    /* 列的间距对着量出来的盒宽算：宽度写在 CSS 里，这里再抄一遍数字，
       哪天 CSS 改了宽就是两份走样的真相——盒子会叠在一起。 */
    const colW = live.reduce((widest, box) => Math.max(widest, box.w), 0);
    let span = 0;
    columns.forEach((column) => {
      column.sort((a, b) => a.slot - b.slot);
      span = Math.max(span, depthOf(column));
    });
    columns.forEach((column) => {
      let cursor = PAD + (span - depthOf(column)) / 2;
      column.forEach((box) => {
        box.x = PAD + box.layer * (colW + COL_GAP);
        box.y = cursor;
        box.el.style.left = `${box.x}px`;
        box.el.style.top = `${box.y}px`;
        cursor += box.h + ROW_GAP;
      });
    });

    const layers = columns.size ? Math.max(...columns.keys()) + 1 : 0;
    const w = PAD * 2 + Math.max(0, layers - 1) * (colW + COL_GAP) + colW;
    stage.style.width = `${w}px`;
    stage.style.height = `${PAD * 2 + span}px`;
    sheet.setAttribute('width', w);
    sheet.setAttribute('height', PAD * 2 + span);
    sheet.setAttribute('viewBox', `0 0 ${w} ${PAD * 2 + span}`);
    zoom.size(w, PAD * 2 + span);
    drawLinks();
  }

  function depthOf(column) {
    return column.reduce((sum, box) => sum + box.h + ROW_GAP, ROW_GAP) - ROW_GAP;
  }

  function sourcePoint(node) {
    const box = owner(node);
    return node.kind === 'row'
      ? { x: box.x + node.ox + node.ow, y: box.y + node.oy + node.oh / 2 }
      : { x: box.x + box.w, y: box.y + box.headCy };
  }

  function targetPoint(node) {
    const box = owner(node);
    return node.kind === 'row'
      ? { x: box.x + node.ox, y: box.y + node.oy + node.oh / 2 }
      : { x: box.x, y: box.y + box.headCy };
  }

  function drawLinks() {
    links.forEach((link) => {
      const on = shown(link.from) && shown(link.to)
        && shown(owner(link.from)) && shown(owner(link.to));
      link.group.style.display = on ? '' : 'none';
      const d = pathOf(link, on);
      link.line.setAttribute('d', d);
      link.hit.setAttribute('d', d);
    });
  }

  function pathOf(link, on) {
    if (!on) {
      return '';
    }
    const a = sourcePoint(link.from);
    const b = targetPoint(link.to);
    const bend = Math.max(28, Math.abs(b.x - a.x) * 0.45);
    return `M${a.x} ${a.y}C${a.x + bend} ${a.y} ${b.x - bend} ${b.y} ${b.x} ${b.y}`;
  }

  /* ---------------- 染色与裁剪 ---------------- */

  /**
   * 这一列的上游闭包 + 下游闭包。
   *
   * 不能用无向可达：共用同一个来源的两个兄弟列会被连成一片，那不是血缘，
   * 用户照着那片染色读出来的因果是假的。
   */
  function coneOf(rowId) {
    const forward = new Map();
    const backward = new Map();
    const push = (map, key, value) => {
      if (!map.has(key)) {
        map.set(key, []);
      }
      map.get(key).push(value);
    };
    links.forEach((link) => {
      push(forward, link.edge.sourceId, link.edge.targetId);
      push(backward, link.edge.targetId, link.edge.sourceId);
    });
    const keep = new Set([rowId]);
    [forward, backward].forEach((map) => {
      const queue = [rowId];
      while (queue.length) {
        const here = queue.shift();
        (map.get(here) || []).forEach((next) => {
          if (!keep.has(next)) {
            keep.add(next);
            queue.push(next);
          }
        });
      }
    });
    return keep;
  }

  /** mode 只分 preview / on：悬停那层是细边，选中那层才把整行点亮 */
  function tint(keep, mode) {
    stage.classList.toggle('tracing', keep.size > 1);
    nodes.forEach((node, id) => {
      if (node.kind === 'row') {
        node.el.classList.toggle('on', mode === 'on' && keep.has(id));
        node.el.classList.toggle('soft', mode === 'preview' && keep.has(id));
      }
    });
    // 整盒压暗靠盒上的类，不用 :has()：锥外的盒子内一行都不亮，那才是"这片不相干"
    boxes.forEach((box) => {
      box.el.classList.toggle('on', keep.has(box.id)
        || (tableOf.get(box.id).columns || []).some((column) => keep.has(column.id)));
    });
    links.forEach((link) => {
      const hit = keep.has(link.edge.sourceId) && keep.has(link.edge.targetId);
      link.group.classList.toggle('on', mode === 'on' && hit);
      link.group.classList.toggle('soft', mode === 'preview' && hit);
    });
  }

  function clearTint() {
    stage.classList.remove('tracing');
    nodes.forEach((node) => node.el.classList.remove('on', 'soft'));
    boxes.forEach((box) => box.el.classList.remove('on'));
    links.forEach((link) => link.group.classList.remove('on', 'soft'));
  }

  let selected = null;

  view.preview = function preview(rowId) {
    if (selected || !nodes.get(rowId)) {
      return view;
    }
    tint(coneOf(rowId), 'preview');
    return view;
  };

  /** 移开鼠标回到"选中态的染色"；没选中就整片擦干净 */
  view.unpreview = function unpreview() {
    if (selected) {
      tint(coneOf(selected), 'on');
    } else {
      clearTint();
    }
    return view;
  };

  /** 选中只染色 + 滚进视野：裁剪是另一个动作，由工具条那个按钮发起 */
  view.select = function select(rowId) {
    const node = nodes.get(rowId);
    if (!node) {
      return view;
    }
    nodes.forEach((other) => other.el.classList.remove('sel'));
    selected = rowId;
    node.el.classList.add('sel');
    tint(coneOf(rowId), 'on');
    zoom.reveal(node.el);
    return view;
  };

  view.deselect = function deselect() {
    selected = null;
    nodes.forEach((node) => node.el.classList.remove('sel'));
    clearTint();
    return view;
  };

  /** 只留这一列的通路：锥外的盒与行藏掉，剩下的重排——不重发请求 */
  view.crop = function crop(rowId) {
    if (!nodes.get(rowId)) {
      return view;
    }
    const keep = coneOf(rowId);
    nodes.forEach((node, id) => {
      if (node.kind === 'row') {
        node.el.style.display = keep.has(id) ? '' : 'none';
      }
    });
    boxes.forEach((box) => {
      const owns = (tableOf.get(box.id).columns || [])
        .some((column) => keep.has(column.id));
      box.el.style.display = owns || keep.has(box.id) ? '' : 'none';
    });
    view.cropped = true;
    layout();
    zoom.fit();
    tint(keep, 'on');
    return view;
  };

  view.reset = function reset() {
    nodes.forEach((node) => {
      node.el.style.display = '';
    });
    view.cropped = false;
    layout();
    zoom.fit();
    if (selected) {
      tint(coneOf(selected), 'on');
    } else {
      clearTint();
    }
    return view;
  };

  /** 容器尺寸变了：fit 的结论要重算，但用户手动定的倍率不许被覆盖 */
  view.refit = function refit() {
    zoom.refit();
    return view;
  };

  /**
   * 定位搜索命中一个盒：滚进视野 + 闪一下边框。
   *
   * 不顺手选中（select 只认字段行）：盒的手势在图上另有约定——点表名是以它为中心重开，
   * 搜索不应该偷偷替用户做这个决定。
   */
  view.revealBox = function revealBox(boxId) {
    const box = nodes.get(boxId);
    if (!box || box.kind !== 'box') {
      return view;
    }
    zoom.reveal(box.el);
    box.el.classList.remove('flash');
    /* 读一次强制回流，让同一盒子连续两次命中也能重启动画 */
    void box.el.offsetWidth;
    box.el.classList.add('flash');
    return view;
  };

  view.png = function png() {
    return snapshot(stage, boxes, links, sourcePoint, targetPoint, shown);
  };

  view.dispose = function dispose() {
    host.textContent = '';
  };

  /* ---------------- 事件 ---------------- */

  stage.addEventListener('click', (event) => {
    const row = event.target.closest('.slf-row');
    if (row) {
      handlers.onRow(row.dataset.node, columnOf.get(row.dataset.node));
      return;
    }
    const head = event.target.closest('.slf-head');
    if (head) {
      handlers.onBox(head.parentNode.dataset.node, tableOf.get(head.parentNode.dataset.node));
      return;
    }
    const link = event.target.closest('.slf-hit');
    if (link) {
      const id = link.getAttribute('data-edge');
      const hit = links.find((candidate) => candidate.edge.id === id);
      handlers.onEdge(id, hit ? hit.edge : {});
      return;
    }
    handlers.onBlank();
  });

  scroll.addEventListener('pointerover', (event) => {
    const row = event.target.closest('.slf-row');
    if (row) {
      view.preview(row.dataset.node);
    }
  });
  scroll.addEventListener('pointerout', (event) => {
    const row = event.target.closest('.slf-row');
    const to = event.relatedTarget && event.relatedTarget.closest
      ? event.relatedTarget.closest('.slf-row') : null;
    if (row && to !== row) {
      view.unpreview();
    }
  });

  layout();
  zoom.fit();
  view.framing = zoom.framing();
  return view;
}

/* ---------------- 缩放与平移 ---------------- */

/**
 * 缩放走 CSS transform：这是"把纸放大"，不是"把字缩小"。
 *
 * fit 的上限是 1 倍——放大到超过原始尺寸只会让格子发虚；下限卡在可读字号，
 * 再小就宁可让它超出容器、交给滚动条，也不画一张读不出字的图。
 */
function createZoom(scroll, sizer, stage, onFrame) {
  const node = el('div', 'slf-zoom');
  const readout = el('span', 'slf-zoom-val', '100%');
  const state = { z: 1, auto: true, width: 1, height: 1 };
  let drag = null;

  function button(text, title, fn) {
    const dom = el('button', null, text);
    dom.type = 'button';
    dom.title = title;
    dom.addEventListener('click', () => {
      fn();
      dom.blur();
    });
    return dom;
  }
  node.append(
    button('−', '缩小一档', () => step(-1)),
    button('＋', '放大一档', () => step(1)),
    button('适应', '缩放到刚好放得下（不低于可读字号）', () => {
      state.auto = true;
      fit();
    }),
    readout);

  function apply() {
    stage.style.transform = `scale(${state.z})`;
    const w = state.width * state.z;
    const h = state.height * state.z;
    sizer.style.width = `${w}px`;
    sizer.style.height = `${h}px`;
    /* 整图比画布小的时候别让它瘫在左上角：居中。只在放得下时居中——真超出容器还留白，
       留白就变成滚不到的黑边。 */
    sizer.style.marginLeft = w < scroll.clientWidth
      ? `${Math.round((scroll.clientWidth - w) / 2)}px` : '0';
    sizer.style.marginTop = h < scroll.clientHeight
      ? `${Math.round((scroll.clientHeight - h) / 2)}px` : '0';
    readout.textContent = `${Math.round(state.z * 100)}%`;
  }

  function step(dir) {
    const next = dir > 0
      ? ZOOM_STEPS.find((candidate) => candidate > state.z + 0.001)
      : ZOOM_STEPS.slice().reverse().find((candidate) => candidate < state.z - 0.001);
    state.auto = false;
    setZ(next === undefined ? (dir > 0 ? ZOOM_MAX : ZOOM_MIN) : next);
  }

  function setZ(z) {
    state.z = Math.max(ZOOM_MIN, Math.min(ZOOM_MAX, z));
    apply();
    onFrame(framing());
  }

  function fit() {
    const wide = (scroll.clientWidth - PAD) / state.width;
    const high = (scroll.clientHeight - PAD) / state.height;
    setZ(Math.min(1, Math.max(READABLE_Z, Math.min(wide, high))));
  }

  /** 装得下就不报；为了保住字而超出容器时，把滚动和拖拽这条路讲出来 */
  function framing() {
    const wide = state.width * state.z > scroll.clientWidth + 1;
    const high = state.height * state.z > scroll.clientHeight + 1;
    if (!wide && !high) {
      return '';
    }
    return `整图 ${Math.round(state.width * state.z)}×${Math.round(state.height * state.z)}`
      + ' 比画布大：滚动或按住空白处拖动看全，点「适应」回到放得下的比例';
  }

  /** 选中一行后把它滚进视野：三百个盒子的时候不滚就等于没选中 */
  function reveal(target) {
    const box = target.getBoundingClientRect();
    const area = scroll.getBoundingClientRect();
    if (box.top < area.top || box.bottom > area.bottom) {
      scroll.scrollTop += box.top - area.top - (area.height - box.height) / 2;
    }
    if (box.left < area.left || box.right > area.right) {
      scroll.scrollLeft += box.left - area.left - (area.width - box.width) / 2;
    }
  }

  scroll.addEventListener('wheel', (event) => {
    if (!event.ctrlKey && !event.metaKey) {
      return;
    }
    event.preventDefault();
    state.auto = false;
    setZ(state.z * (event.deltaY < 0 ? 1.12 : 0.89));
  }, { passive: false });

  scroll.addEventListener('pointerdown', (event) => {
    if (event.button !== 0 || event.target.closest('.slf-box, .slf-zoom, .slf-hit')) {
      return;
    }
    drag = { x: event.clientX, y: event.clientY, left: scroll.scrollLeft, top: scroll.scrollTop };
    scroll.setPointerCapture(event.pointerId);
    scroll.classList.add('grabbing');
  });
  scroll.addEventListener('pointermove', (event) => {
    if (!drag) {
      return;
    }
    scroll.scrollLeft = drag.left - (event.clientX - drag.x);
    scroll.scrollTop = drag.top - (event.clientY - drag.y);
  });
  function release() {
    drag = null;
    scroll.classList.remove('grabbing');
  }
  scroll.addEventListener('pointerup', release);
  scroll.addEventListener('pointercancel', release);

  return {
    node,
    /** 量完布再报尺寸：第一次落位还在 auto 状态，正好按新的整图大小 fit 一次 */
    size: (width, height) => {
      state.width = Math.max(1, width);
      state.height = Math.max(1, height);
      apply();
      if (state.auto) {
        fit();
      }
    },
    fit: () => {
      if (state.auto) {
        fit();
      } else {
        setZ(state.z);
      }
    },
    refit: () => {
      if (state.auto) {
        fit();
      } else {
        /* 手动定过倍率也得重落居中距：容器一窄，987px 的左居中就把图推到要空滚半天才够得着的地方。
           apply() 不动 state.z，用户的缩放仍然作数，只是居中跟着新容器走。 */
        apply();
        onFrame(framing());
      }
    },
    framing,
    reveal,
  };
}

/* ---------------- 只给模型、不落笔 ---------------- */

function idleView(warning) {
  const view = {
    framing: '',
    modelWarning: warning,
    cropped: false,
    preview: () => view,
    unpreview: () => view,
    select: () => view,
    deselect: () => view,
    crop: () => view,
    reset: () => view,
    refit: () => view,
    revealBox: () => view,
    png: () => null,
    dispose: () => {},
  };
  return view;
}

/* ---------------- 导出 PNG ---------------- */

/**
 * 屏上是 DOM，导出就得对着 canvas 再画一遍。
 *
 * 坐标一律沿用 layout() 量出来的那份（盒的 x/y/w/h、行和表名在自己盒里的 offset），
 * 颜色来自 CSS 变量（badges.js 的 ink / layerColor）——同一份数字画两遍，
 * 导出图和屏幕上才会是同一张图。藏起来的盒与行不出图：用户要的 PNG 是他看见的那片。
 */
function snapshot(stage, boxes, links, sourcePoint, targetPoint, shown) {
  const width = parseFloat(stage.style.width);
  const height = parseFloat(stage.style.height);
  if (!(width > 0 && height > 0)) {
    return null;
  }
  const scale = 2;
  const canvas = document.createElement('canvas');
  canvas.width = Math.round(width * scale);
  canvas.height = Math.round(height * scale);
  const ctx = canvas.getContext('2d');
  ctx.scale(scale, scale);
  ctx.fillStyle = ink.canvasBg;
  ctx.fillRect(0, 0, width, height);
  /* 12px 与 .slf-row/.slf-name 的字号是一对：改 CSS 不改这里，导出图会截错省略号 */
  ctx.font = '12px "Cascadia Mono", Consolas, monospace';
  ctx.textBaseline = 'middle';

  /* 屏幕上没画出来的线不出图：drawLinks 刚按可见性把 group 藏了，这里以它为准 */
  links.filter((link) => link.group.style.display !== 'none').forEach((link) => {
    const a = sourcePoint(link.from);
    const b = targetPoint(link.to);
    const bend = Math.max(28, Math.abs(b.x - a.x) * 0.45);
    ctx.globalAlpha = link.group.classList.contains('on') ? 1 : 0.75;
    ctx.strokeStyle = link.line.getAttribute('stroke');
    ctx.lineWidth = parseFloat(link.line.getAttribute('stroke-width')) || 1.4;
    const dash = link.line.getAttribute('stroke-dasharray');
    ctx.setLineDash(!dash || dash === 'none' ? [] : dash.split(' ').map(Number));
    ctx.beginPath();
    ctx.moveTo(a.x, a.y);
    ctx.bezierCurveTo(a.x + bend, a.y, b.x - bend, b.y, b.x, b.y);
    ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = ctx.strokeStyle;
    ctx.beginPath();
    ctx.moveTo(b.x + 1, b.y);
    ctx.lineTo(b.x - 7, b.y - 4);
    ctx.lineTo(b.x - 7, b.y + 4);
    ctx.closePath();
    ctx.fill();
  });

  ctx.globalAlpha = 1;
  boxes.filter(shown).forEach((box) => paintBox(ctx, box));
  return canvas.toDataURL('image/png');
}

/**
 * 一个盒：容器、标题带、留下的行。
 *
 * 所有文字的量都从盒的原点起算——.slf-box 是唯一的定位父级（head/row 都不设 position），
 * 所以 offsetTop/Left 报的就是"离盒顶多远"，和屏幕上浏览器摆的位置是同一个数。
 */
function paintBox(ctx, box) {
  const accent = layerColor(box.layer);
  ctx.fillStyle = ink.boxFill;
  ctx.strokeStyle = box.local ? ink.localLine : accent;
  ctx.lineWidth = 1;
  ctx.setLineDash(box.local ? [4, 3] : []);
  roundRect(ctx, box.x, box.y, box.w, box.h);
  ctx.fill();
  ctx.stroke();
  ctx.setLineDash([]);

  const head = box.head;
  ctx.fillStyle = box.local ? ink.localFill : accent;
  ctx.fillRect(box.x + 1, box.y + head.offsetTop, box.w - 2, head.offsetHeight);
  const name = head.querySelector('.slf-name');
  ctx.fillStyle = box.local ? ink.localText : ink.nodeText;
  clipText(ctx, name.textContent, box.x + name.offsetLeft,
    box.y + name.offsetTop + name.offsetHeight / 2, name.offsetWidth);

  Array.prototype.forEach.call(box.el.querySelectorAll('.slf-row'), (row) => {
    if (row.style.display === 'none') {
      return;
    }
    const on = row.classList.contains('on');
    ctx.fillStyle = on ? ink.hot : ink.rowFill;
    ctx.fillRect(box.x + row.offsetLeft, box.y + row.offsetTop,
      row.offsetWidth, row.offsetHeight);
    ctx.strokeStyle = ink.rowLine;
    ctx.strokeRect(box.x + row.offsetLeft + 0.5, box.y + row.offsetTop + 0.5,
      row.offsetWidth - 1, row.offsetHeight - 1);
    ctx.fillStyle = on ? ink.canvasBg : ink.rowText;
    const label = row.querySelector('.slf-col');
    clipText(ctx, label.textContent, box.x + label.offsetLeft,
      box.y + label.offsetTop + label.offsetHeight / 2, label.offsetWidth);
  });
}

function roundRect(ctx, x, y, w, h) {
  const r = 3;
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.arcTo(x + w, y, x + w, y + h, r);
  ctx.arcTo(x + w, y + h, x, y + h, r);
  ctx.arcTo(x, y + h, x, y, r);
  ctx.arcTo(x, y, x + w, y, r);
  ctx.closePath();
}

/** CSS 那边是 text-overflow:ellipsis，这边得自己截：导出图和屏幕上的字要断在同一个地方 */
function clipText(ctx, text, x, y, maxW) {
  const raw = text || '';
  if (ctx.measureText(raw).width <= maxW) {
    ctx.fillText(raw, x, y);
    return;
  }
  let cut = raw;
  while (cut.length > 1 && ctx.measureText(`${cut}…`).width > maxW) {
    cut = cut.slice(0, -1);
  }
  ctx.fillText(`${cut}…`, x, y);
}
