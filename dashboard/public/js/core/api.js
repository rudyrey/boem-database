import { CONFIG } from './config.js';

export async function apiGet(path, params = {}) {
  const url = new URL(CONFIG.API_BASE + path, window.location.origin);
  for (const [k, v] of Object.entries(params)) {
    if (v != null && v !== '') url.searchParams.set(k, v);
  }
  const res = await fetch(url);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.error || `API error ${res.status}`);
  }
  return res.json();
}

export async function apiPost(path, body) {
  const res = await fetch(CONFIG.API_BASE + path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({}));
    throw new Error(data.error || `API error ${res.status}`);
  }
  return res.json();
}

// Singleton stats cache
let _stats = null;
export async function getStats() {
  if (!_stats) _stats = await apiGet('/stats');
  return _stats;
}
