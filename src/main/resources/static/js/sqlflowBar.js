/**
 * 常驻动作条：钉在画布右上角，可以按住把手拖走。
 *
 * 为什么不再跟着那一行贴：上一版把动作条贴在刚点的行旁边，于是它的坐标依赖
 * "那一行现在在哪儿"——裁剪会重排、缩放会改屏幕位置、换视图会留下过期句柄，
 * §8.12 里五处缺陷有三处就是这么来的。钉死在角落里，位置就和图的内容无关了。
 *
 * 为什么是右上角：层 0 永远从左上角起步，钉在那儿就是压住用户正在看的起点。
 * 拖拽是必要的：图很宽的时候最深那一层也会路过右上角。
 */
export function createBar(el, actions) {
  const title = el.querySelector('.bar-title');
  el.querySelectorAll('[data-bar]').forEach((button) => {
    // 「×」也归调用方：只把条子藏起来，画布上那一列还亮着，用户就不明白自己在看什么
    button.addEventListener('click', () => actions[button.dataset.bar]());
  });

  const grip = el.querySelector('.bar-grip');
  grip.addEventListener('pointerdown', (event) => {
    const frame = el.parentElement.getBoundingClientRect();
    const box = el.getBoundingClientRect();
    const start = { x: event.clientX, y: event.clientY, left: box.left, top: box.top };
    grip.setPointerCapture(event.pointerId);
    el.classList.add('dragging');
    const move = (event2) => {
      const left = Math.min(Math.max(0, start.left - frame.left + event2.clientX - start.x),
        frame.width - box.width);
      const top = Math.min(Math.max(0, start.top - frame.top + event2.clientY - start.y),
        frame.height - box.height);
      el.style.left = `${left}px`;
      el.style.top = `${top}px`;
      el.style.right = 'auto';
      el.style.bottom = 'auto';
    };
    const up = () => {
      el.classList.remove('dragging');
      grip.removeEventListener('pointermove', move);
      grip.removeEventListener('pointerup', up);
      grip.removeEventListener('pointercancel', up);
    };
    grip.addEventListener('pointermove', move);
    grip.addEventListener('pointerup', up);
    // 指针被系统收走（切窗口、触摸被手势打断）时没有 pointerup：不清就把一对监听留在身上
    grip.addEventListener('pointercancel', up);
  });

  function hide() {
    el.hidden = true;
  }

  /** 标题只放列标识：整句提示挂在 tooltip 上，一行放不下就会把条子撑成一块砖 */
  function show(text, hint) {
    title.textContent = text;
    el.title = hint || '';
    el.hidden = false;
  }

  return { show, hide };
}
