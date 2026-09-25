/**
 * 跟随式浮层：贴在用户刚点的那一行旁边，三个动作 + 关闭。
 *
 * 为什么把动作放在节点旁边而不是右栏顶上：字段级血缘的操作对象就是"这一行"，
 * 按钮离得远，用户得在 300 列的图上找回自己点的那一格。对方的做法是同一个道理。
 *
 * 位置由调用方给（node.renderedPosition() 换算的画布内坐标），这里只管贴上去、别溢出画布。
 * 浮层挂在 .canvas 上、#graph 是 inset:0，所以画布内坐标和这里的 left/top 是同一套原点。
 */
export function createPop(el, actions) {
  const title = el.querySelector('.pop-title');
  el.querySelectorAll('[data-pop]').forEach((button) => {
    button.addEventListener('click', () => {
      if (button.dataset.pop === 'close') {
        hide();
        return;
      }
      actions[button.dataset.pop]();
    });
  });

  function hide() {
    el.hidden = true;
  }

  /** 贴着节点右侧；快到画布右边缘时翻到左边，纵向夹在画布内 */
  function show(x, y, text) {
    title.textContent = text;
    el.hidden = false;
    const canvas = el.parentElement.getBoundingClientRect();
    const box = el.getBoundingClientRect();
    const left = x + 14 + box.width > canvas.width ? x - box.width - 14 : x + 14;
    el.style.left = `${Math.max(4, Math.min(left, canvas.width - box.width - 4))}px`;
    el.style.top = `${Math.max(4, Math.min(y - box.height / 2,
      canvas.height - box.height - 4))}px`;
  }

  return { show, hide, visible: () => !el.hidden };
}
