export function formatNumber(n, decimals = 0) {
  if (n == null || isNaN(n)) return '—';
  return Number(n).toLocaleString('en-US', { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
}

export function formatCompact(n) {
  if (n == null || isNaN(n)) return '—';
  const abs = Math.abs(n);
  if (abs >= 1e9) return (n / 1e9).toFixed(1) + 'B';
  if (abs >= 1e6) return (n / 1e6).toFixed(1) + 'M';
  if (abs >= 1e3) return (n / 1e3).toFixed(1) + 'K';
  return n.toString();
}

export function formatDate(d) {
  if (!d) return '—';
  if (d.length === 6) {
    return d.substring(0, 4) + '-' + d.substring(4, 6);
  }
  return d;
}

export function formatDepth(ft) {
  if (ft == null) return '—';
  return formatNumber(ft) + ' ft';
}

export function formatBarrels(bbl) {
  if (bbl == null || isNaN(bbl)) return '—';
  if (bbl >= 1e6) return (bbl / 1e6).toFixed(2) + ' MM';
  if (bbl >= 1e3) return (bbl / 1e3).toFixed(1) + 'K';
  return formatNumber(bbl);
}

export function debounce(fn, ms = 300) {
  let timer;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), ms);
  };
}

export function throttle(fn, ms = 300) {
  let last = 0;
  return (...args) => {
    const now = Date.now();
    if (now - last >= ms) { last = now; fn(...args); }
  };
}

export function escapeHtml(str) {
  if (!str) return '';
  return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

export function wellStatusBadge(status) {
  const map = {
    'C': ['Completed', 'badge-success'],
    'D': ['Drilling', 'badge-info'],
    'E': ['Expired', 'badge-muted'],
    'O': ['Active', 'badge-success'],
    'R': ['P&A', 'badge-danger'],
  };
  const [label, cls] = map[status] || [status || '—', 'badge-muted'];
  return `<span class="badge ${cls}">${label}</span>`;
}

export function leaseStatusBadge(status) {
  if (!status) return '<span class="badge badge-muted">—</span>';
  const s = status.trim();
  const map = {
    'PROD': 'badge-success', 'PRIMRY': 'badge-info', 'UNIT': 'badge-info',
    'SOP': 'badge-warning', 'SOO': 'badge-warning', 'DSO': 'badge-warning',
    'RELINQ': 'badge-muted', 'EXPIR': 'badge-muted', 'TERMIN': 'badge-danger',
  };
  return `<span class="badge ${map[s] || 'badge-muted'}">${s}</span>`;
}

export function flagDot(val) {
  const on = val === 'Y';
  return `<span class="dot ${on ? 'on' : 'off'}"></span>`;
}
