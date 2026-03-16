import { apiGet } from '../core/api.js';
import { debounce, escapeHtml } from '../core/utils.js';

export class SearchBox {
  constructor(container) {
    this.container = container;
    this._render();
  }

  _render() {
    this.container.innerHTML = `
      <div style="position:relative">
        <input type="text" id="search-input" placeholder="Search wells, leases, platforms, companies..."
          style="width:100%;height:36px;padding:0 12px;border:1px solid rgba(255,255,255,0.2);
          border-radius:6px;background:rgba(255,255,255,0.1);color:#fff;font-size:13px;
          outline:none;transition:all 150ms ease;">
        <div id="search-dropdown" style="display:none;position:absolute;top:40px;left:0;right:0;
          background:#fff;border-radius:6px;box-shadow:0 10px 24px rgba(0,0,0,0.15);
          max-height:400px;overflow-y:auto;z-index:9999;"></div>
      </div>
    `;

    this.input = this.container.querySelector('#search-input');
    this.dropdown = this.container.querySelector('#search-dropdown');

    this.input.addEventListener('input', debounce(() => this._search(), 300));
    this.input.addEventListener('focus', () => { if (this.input.value.length >= 2) this._search(); });

    document.addEventListener('click', (e) => {
      if (!this.container.contains(e.target)) this.dropdown.style.display = 'none';
    });

    this.input.style.cssText += 'transition:all 150ms;';
    this.input.addEventListener('focus', () => {
      this.input.style.background = 'rgba(255,255,255,0.2)';
      this.input.style.borderColor = 'rgba(255,255,255,0.4)';
    });
    this.input.addEventListener('blur', () => {
      this.input.style.background = 'rgba(255,255,255,0.1)';
      this.input.style.borderColor = 'rgba(255,255,255,0.2)';
    });
  }

  async _search() {
    const q = this.input.value.trim();
    if (q.length < 2) { this.dropdown.style.display = 'none'; return; }

    try {
      const results = await apiGet('/search', { q, limit: 15 });
      this._showResults(results);
    } catch (e) {
      console.error('Search error:', e);
    }
  }

  _showResults(results) {
    let html = '';

    const sections = [
      { key: 'wells', label: 'Wells', items: results.wells, fmt: (w) => ({
        title: w.well_name || w.api_well_number,
        sub: w.operator_name || '',
        href: `#/wells/${encodeURIComponent(w.api_well_number)}`,
      })},
      { key: 'leases', label: 'Leases', items: results.leases, fmt: (l) => ({
        title: `Lease ${l.lease_number}`,
        sub: `${l.area_code} ${l.block_number} — ${l.lease_status || ''}`,
        href: `#/leases/${encodeURIComponent(l.lease_number)}`,
      })},
      { key: 'platforms', label: 'Platforms', items: results.platforms, fmt: (p) => ({
        title: p.structure_name || `Complex ${p.complex_id}`,
        sub: `${p.area_code} ${p.block_number} — ${p.operator_name || ''}`,
        href: `#/platforms/${encodeURIComponent(p.complex_id)}`,
      })},
      { key: 'companies', label: 'Companies', items: results.companies, fmt: (c) => ({
        title: c.company_name,
        sub: [c.city, c.state_code].filter(Boolean).join(', '),
        href: `#/companies/${encodeURIComponent(c.company_num)}`,
      })},
    ];

    for (const sec of sections) {
      if (!sec.items || sec.items.length === 0) continue;
      html += `<div style="padding:4px 12px;font-size:11px;color:#7f8c8d;text-transform:uppercase;
        letter-spacing:0.5px;font-weight:600;border-bottom:1px solid #ecf0f1;
        background:#f5f6fa;">${sec.label}</div>`;
      for (const item of sec.items) {
        const d = sec.fmt(item);
        html += `<a href="${d.href}" class="search-result-item" style="display:block;padding:8px 12px;
          text-decoration:none;color:#2c3e50;border-bottom:1px solid #f5f6fa;transition:background 100ms;">
          <div style="font-size:13px;font-weight:500;">${escapeHtml(d.title)}</div>
          <div style="font-size:11px;color:#7f8c8d;">${escapeHtml(d.sub)}</div>
        </a>`;
      }
    }

    if (!html) {
      html = '<div style="padding:16px;text-align:center;color:#bdc3c7;font-size:13px;">No results</div>';
    }

    this.dropdown.innerHTML = html;
    this.dropdown.style.display = 'block';

    this.dropdown.querySelectorAll('a').forEach(a => {
      a.addEventListener('mouseenter', () => a.style.background = '#f5f6fa');
      a.addEventListener('mouseleave', () => a.style.background = '');
      a.addEventListener('click', () => {
        this.dropdown.style.display = 'none';
        this.input.value = '';
      });
    });
  }
}
