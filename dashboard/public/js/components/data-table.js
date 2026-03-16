import { formatNumber } from '../core/utils.js';

/**
 * Server-paginated, sortable data table.
 *
 * Usage:
 *   const table = new DataTable({
 *     container: el,
 *     columns: [{ key, label, width?, format?, className? }],
 *     fetchFn: async (page, limit, sort, order, filters) => ({ data, pagination }),
 *     onRowClick: (row) => {},
 *   });
 *   table.load();                // Initial load
 *   table.setFilters(filters);   // Triggers reload
 */
export class DataTable {
  constructor({ container, columns, fetchFn, onRowClick, pageSize = 50 }) {
    this.container = container;
    this.columns = columns;
    this.fetchFn = fetchFn;
    this.onRowClick = onRowClick;
    this.pageSize = pageSize;

    this.page = 1;
    this.sort = null;
    this.order = 'asc';
    this.filters = {};
    this.data = [];
    this.pagination = { page: 1, limit: pageSize, total: 0, totalPages: 0 };
    this.selectedId = null;
    this.loading = false;

    this._render();
  }

  _render() {
    this.container.innerHTML = `
      <div class="data-table-wrapper">
        <table class="data-table">
          <thead><tr></tr></thead>
          <tbody></tbody>
        </table>
      </div>
      <div class="pagination-bar">
        <span class="page-info"></span>
        <div class="page-controls"></div>
      </div>
    `;
    this.theadRow = this.container.querySelector('thead tr');
    this.tbody = this.container.querySelector('tbody');
    this.pageInfo = this.container.querySelector('.page-info');
    this.pageControls = this.container.querySelector('.page-controls');

    this._renderHeader();
  }

  _renderHeader() {
    this.theadRow.innerHTML = this.columns.map(col => {
      const isActive = this.sort === col.key;
      const arrow = isActive ? (this.order === 'asc' ? '▲' : '▼') : '↕';
      const w = col.width ? `width:${col.width}` : '';
      return `<th data-key="${col.key}" class="${isActive ? 'sort-active' : ''}" style="${w}">
        ${col.label} <span class="sort-icon">${arrow}</span>
      </th>`;
    }).join('');

    this.theadRow.querySelectorAll('th').forEach(th => {
      th.addEventListener('click', () => {
        const key = th.dataset.key;
        if (this.sort === key) {
          this.order = this.order === 'asc' ? 'desc' : 'asc';
        } else {
          this.sort = key;
          this.order = 'asc';
        }
        this.page = 1;
        this._renderHeader();
        this.load();
      });
    });
  }

  _renderBody() {
    if (this.loading) {
      this.tbody.innerHTML = `<tr><td colspan="${this.columns.length}" class="loading-overlay">
        <div class="spinner"></div> Loading...
      </td></tr>`;
      return;
    }

    if (this.data.length === 0) {
      this.tbody.innerHTML = `<tr><td colspan="${this.columns.length}" class="empty-state">No results found</td></tr>`;
      return;
    }

    this.tbody.innerHTML = this.data.map((row, idx) => {
      const cls = row._id === this.selectedId ? 'selected' : '';
      return `<tr class="${cls}" data-idx="${idx}">
        ${this.columns.map(col => {
          let val = row[col.key];
          if (col.format) val = col.format(val, row);
          else if (val == null) val = '—';
          const cellClass = col.className || '';
          return `<td class="${cellClass}">${val}</td>`;
        }).join('')}
      </tr>`;
    }).join('');

    this.tbody.querySelectorAll('tr').forEach(tr => {
      tr.addEventListener('click', () => {
        const idx = parseInt(tr.dataset.idx);
        const row = this.data[idx];
        if (row && this.onRowClick) {
          this.selectedId = row._id;
          this._renderBody();
          this.onRowClick(row);
        }
      });
    });
  }

  _renderPagination() {
    const { page, total, totalPages, limit } = this.pagination;
    const start = total === 0 ? 0 : (page - 1) * limit + 1;
    const end = Math.min(page * limit, total);
    this.pageInfo.textContent = `${formatNumber(start)}–${formatNumber(end)} of ${formatNumber(total)}`;

    let btns = '';
    btns += `<button data-page="${page - 1}" ${page <= 1 ? 'disabled' : ''}>‹</button>`;

    const range = this._pageRange(page, totalPages);
    for (const p of range) {
      if (p === '...') {
        btns += `<button disabled>…</button>`;
      } else {
        btns += `<button data-page="${p}" class="${p === page ? 'active' : ''}">${p}</button>`;
      }
    }

    btns += `<button data-page="${page + 1}" ${page >= totalPages ? 'disabled' : ''}>›</button>`;
    this.pageControls.innerHTML = btns;

    this.pageControls.querySelectorAll('button[data-page]').forEach(btn => {
      btn.addEventListener('click', () => {
        const p = parseInt(btn.dataset.page);
        if (p >= 1 && p <= totalPages && p !== this.page) {
          this.page = p;
          this.load();
        }
      });
    });
  }

  _pageRange(current, total) {
    if (total <= 7) return Array.from({ length: total }, (_, i) => i + 1);
    const pages = [];
    pages.push(1);
    if (current > 3) pages.push('...');
    for (let i = Math.max(2, current - 1); i <= Math.min(total - 1, current + 1); i++) pages.push(i);
    if (current < total - 2) pages.push('...');
    if (total > 1) pages.push(total);
    return pages;
  }

  async load() {
    this.loading = true;
    this._renderBody();
    try {
      const result = await this.fetchFn(this.page, this.pageSize, this.sort, this.order, this.filters);
      this.data = result.data.map((row, i) => ({ ...row, _id: i }));
      this.pagination = result.pagination;
    } catch (err) {
      console.error('DataTable load error:', err);
      this.data = [];
      this.pagination = { page: 1, limit: this.pageSize, total: 0, totalPages: 0 };
    }
    this.loading = false;
    this._renderBody();
    this._renderPagination();
  }

  setFilters(filters) {
    this.filters = { ...filters };
    this.page = 1;
    this.load();
  }

  destroy() {
    this.container.innerHTML = '';
  }
}
