/**
 * Adds a draggable resize handle between .split-main and .split-detail
 * within a .split-layout container.
 */
export function initSplitResizer(splitLayout) {
  if (!splitLayout) return null;

  const handle = document.createElement('div');
  handle.className = 'split-handle';
  const main = splitLayout.querySelector('.split-main');
  const detail = splitLayout.querySelector('.split-detail');
  if (!main || !detail) return null;

  main.after(handle);

  let startX, startDetailWidth;

  function onMouseDown(e) {
    e.preventDefault();
    startX = e.clientX;
    startDetailWidth = detail.getBoundingClientRect().width;
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
    handle.classList.add('active');
    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
  }

  function onMouseMove(e) {
    const dx = startX - e.clientX; // dragging left = bigger detail
    const newWidth = Math.max(200, Math.min(startDetailWidth + dx, window.innerWidth * 0.6));
    detail.style.width = newWidth + 'px';
  }

  function onMouseUp() {
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
    handle.classList.remove('active');
    document.removeEventListener('mousemove', onMouseMove);
    document.removeEventListener('mouseup', onMouseUp);
  }

  handle.addEventListener('mousedown', onMouseDown);

  return {
    destroy() {
      handle.removeEventListener('mousedown', onMouseDown);
      handle.remove();
    }
  };
}
