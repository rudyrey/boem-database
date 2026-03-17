import { apiGet, getStats } from '../core/api.js';
import { formatNumber, escapeHtml } from '../core/utils.js';
import { DataTable } from '../components/data-table.js';
import { initSplitResizer } from '../components/split-resizer.js';
import { ChartPanel, productionChartConfig } from '../components/chart-panel.js';

export async function initCompaniesView(container, params) {
  container.innerHTML = `
    <div class="view-header"><h2>Companies</h2></div>
    <div class="split-layout">
      <div class="split-main">
        <div class="filter-bar">
          <input type="text" id="company-search" placeholder="Search companies..." class="filter-input" style="flex:1;min-width:250px">
        </div>
        <div id="companies-table"></div>
      </div>
      <div class="split-detail">
        <div id="company-detail" class="detail-panel">
          <div class="empty-state">Select a company to view details</div>
        </div>
      </div>
    </div>
  `;

  const table = new DataTable({
    container: document.getElementById('companies-table'),
    columns: [
      { key: 'company_num', label: 'Company #', width: '100px' },
      { key: 'company_name', label: 'Company Name' },
      { key: 'bus_asc_name', label: 'Business Associate', width: '200px' },
    ],
    fetchFn: (page, limit, sort, order, filters) => {
      return apiGet('/companies', { page, limit, sort, order, ...filters });
    },
    onRowClick: (row) => loadCompanyDetail(row.company_num),
    pageSize: 50,
  });

  // Search input
  const searchInput = document.getElementById('company-search');
  let searchTimeout;
  searchInput.addEventListener('input', () => {
    clearTimeout(searchTimeout);
    searchTimeout = setTimeout(() => {
      table.setFilters({ search: searchInput.value.trim() || undefined });
    }, 400);
  });

  const resizer = initSplitResizer(container.querySelector('.split-layout'));

  table.load();

  // Direct navigation to a company
  if (params?.id) {
    loadCompanyDetail(params.id);
  }

  let detailChart = null;

  async function loadCompanyDetail(companyNum) {
    const panel = document.getElementById('company-detail');
    panel.innerHTML = '<div class="loading"><div class="spinner"></div></div>';

    try {
      const c = await apiGet(`/companies/${companyNum}`);

      panel.innerHTML = `
        <div class="detail-header">
          <h3>${escapeHtml(c.company_name || 'N/A')}</h3>
          <span class="badge badge-info">#${c.company_num}</span>
        </div>
        <div class="detail-body">
          <div class="kv-section">
            <div class="kv-row"><span class="kv-label">Company Number</span><span class="kv-value">${c.company_num}</span></div>
            <div class="kv-row"><span class="kv-label">Company Name</span><span class="kv-value">${escapeHtml(c.company_name || 'N/A')}</span></div>
            <div class="kv-row"><span class="kv-label">Business Associate</span><span class="kv-value">${escapeHtml(c.bus_asc_name || 'N/A')}</span></div>
          </div>

          <div class="kv-section">
            <div class="kv-section-title">Related Entities</div>
            <div class="kv-row"><span class="kv-label">Wells Operated</span><span class="kv-value">${formatNumber(c.wellCount || 0)}</span></div>
            <div class="kv-row"><span class="kv-label">Platforms Operated</span><span class="kv-value">${formatNumber(c.platformCount || 0)}</span></div>
            <div class="kv-row"><span class="kv-label">Leases (Owner)</span><span class="kv-value">${formatNumber(c.leaseCount || 0)}</span></div>
          </div>
        </div>

        <div class="detail-tabs">
          <button class="tab-btn active" data-tab="wells">Wells (${c.wellCount || 0})</button>
          <button class="tab-btn" data-tab="leases">Leases (${c.leaseCount || 0})</button>
          <button class="tab-btn" data-tab="platforms">Platforms (${c.platformCount || 0})</button>
        </div>
        <div id="company-tab-content" style="padding: var(--space-sm) var(--space-md) var(--space-md)"></div>
      `;

      // Tab switching
      panel.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => {
          panel.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
          btn.classList.add('active');
          loadTab(companyNum, btn.dataset.tab);
        });
      });

      // Load default tab
      loadTab(companyNum, 'wells');
    } catch (e) {
      panel.innerHTML = `<div class="empty-state">Error loading company: ${e.message}</div>`;
    }
  }

  async function loadTab(companyNum, tab, page = 1) {
    const content = document.getElementById('company-tab-content');
    if (!content) return;
    content.innerHTML = '<div class="loading"><div class="spinner"></div></div>';

    const pageSize = 25;

    try {
      const res = await apiGet(`/companies/${companyNum}/${tab}`, { limit: pageSize, page });
      const rows = res.data;
      const total = res.pagination ? res.pagination.total : rows.length;
      const totalPages = res.pagination ? res.pagination.totalPages : 1;

      if (!rows || rows.length === 0) {
        content.innerHTML = '<div class="empty-state">No records found</div>';
        return;
      }

      let tableHTML = '';
      if (tab === 'wells') {
        tableHTML = `
          <table class="detail-subtable">
            <thead><tr><th>API Number</th><th>Well Name</th><th>Status</th><th>Type</th></tr></thead>
            <tbody>
              ${rows.map(w => `
                <tr class="clickable-row" data-href="#/wells/${w.api_well_number}">
                  <td><code>${w.api_well_number}</code></td>
                  <td>${escapeHtml(w.well_name || '')}</td>
                  <td>${w.status_code || ''}</td>
                  <td>${w.type_code || ''}</td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        `;
      } else if (tab === 'leases') {
        tableHTML = `
          <table class="detail-subtable">
            <thead><tr><th>Lease #</th><th>Area/Block</th><th>Status</th><th>% Owned</th></tr></thead>
            <tbody>
              ${rows.map(l => `
                <tr class="clickable-row" data-href="#/leases/${l.lease_number}">
                  <td><code>${l.lease_number}</code></td>
                  <td>${escapeHtml((l.area_code || '') + ' ' + (l.block_number || ''))}</td>
                  <td>${escapeHtml(l.lease_status || '')}</td>
                  <td>${l.assignment_pct ? l.assignment_pct.toFixed(2) + '%' : ''}</td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        `;
      } else if (tab === 'platforms') {
        tableHTML = `
          <table class="detail-subtable">
            <thead><tr><th>Complex ID</th><th>Structure</th><th>Area</th><th>Block</th></tr></thead>
            <tbody>
              ${rows.map(p => `
                <tr class="clickable-row" data-href="#/platforms/${p.complex_id}">
                  <td><code>${p.complex_id}</code></td>
                  <td>${escapeHtml(p.structure_name || '')}</td>
                  <td>${escapeHtml(p.area_code || '')}</td>
                  <td>${p.block_number || ''}</td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        `;
      }

      const paginationHTML = total > pageSize ? `
        <div class="detail-tab-pagination">
          <span>Page ${page} of ${totalPages} (${formatNumber(total)} total)</span>
          <div class="page-btns">
            <button id="tab-prev" ${page <= 1 ? 'disabled' : ''}>Prev</button>
            <button id="tab-next" ${page >= totalPages ? 'disabled' : ''}>Next</button>
          </div>
        </div>
      ` : '';

      content.innerHTML = tableHTML + paginationHTML;

      // Make rows clickable
      content.querySelectorAll('.clickable-row').forEach(row => {
        row.addEventListener('click', () => {
          window.location.hash = row.dataset.href;
        });
      });

      // Pagination buttons
      const prevBtn = content.querySelector('#tab-prev');
      const nextBtn = content.querySelector('#tab-next');
      if (prevBtn) prevBtn.addEventListener('click', () => loadTab(companyNum, tab, page - 1));
      if (nextBtn) nextBtn.addEventListener('click', () => loadTab(companyNum, tab, page + 1));
    } catch (e) {
      content.innerHTML = `<div class="empty-state">Error: ${e.message}</div>`;
    }
  }

  return () => {
    if (resizer) resizer.destroy();
    if (detailChart) detailChart.destroy();
  };
}
