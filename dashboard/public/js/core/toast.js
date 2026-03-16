const container = document.createElement('div');
container.style.cssText = 'position:fixed;bottom:16px;right:16px;z-index:9999;display:flex;flex-direction:column;gap:8px;';
document.body.appendChild(container);

function show(msg, type = 'info', duration = 3000) {
  const el = document.createElement('div');
  const colors = { success: '#27ae60', error: '#c0392b', warning: '#f39c12', info: '#3498db' };
  el.style.cssText = `
    padding: 10px 16px; border-radius: 6px; color: #fff; font-size: 13px;
    background: ${colors[type] || colors.info}; box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    opacity: 0; transform: translateY(10px); transition: all 250ms ease; max-width: 360px;
  `;
  el.textContent = msg;
  container.appendChild(el);
  requestAnimationFrame(() => { el.style.opacity = '1'; el.style.transform = 'translateY(0)'; });
  setTimeout(() => {
    el.style.opacity = '0';
    el.style.transform = 'translateY(10px)';
    setTimeout(() => el.remove(), 300);
  }, duration);
}

export const toast = {
  success: (m) => show(m, 'success'),
  error: (m) => show(m, 'error', 5000),
  warning: (m) => show(m, 'warning'),
  info: (m) => show(m, 'info'),
};

export function showToast(msg, type = 'info') { show(msg, type); }
