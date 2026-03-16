import { apiGet, getStats } from '../core/api.js';
import { formatNumber, formatDate, leaseStatusBadge, wellStatusBadge, formatDepth, escapeHtml } from '../core/utils.js';
import { DataTable } from '../components/data-table.js';
import { ChartPanel, productionChartConfig } from '../components/chart-panel.js';
import { debounce } from '../core/utils.js';

export async function initLeasesView(container, params = {}) {
  const stats = await getStats();

  container.innerHTML = `
    <div class="view-header">
      <h2>Lease Explorer <span class="subtitle">${formatNumber(stats.counts.leases)} leases</span></h2>
    </div>
    <div class="filter-bar" id="lease-filters"></div>
    <div class="split-layout">
      <div class="split-main" id="lease-table"></div>
      <div class="split-detail" id="lease-detail">
        <div class="detail-panel"><div class="detail-panel-empty">Select a lease to view details</div></div>
      </div>
    </div>
  `;

  const filterEl = document.getElementById('lease-filters');
  const opts = stats.filterOptions;
  filterEl.innerHTML = `
    <input type="text" id="lf-search" placeholder="Search leases...">
    <select id="lf-status"><option value="">All Status</option>
      ${opts.leaseStatus.map(s => `<option value="${s}">${s}</option>`).join('')}</select>
    <select id="lf-area"><option value="">All Areas</option>
      ${opts.areaCodes.map(a => `<option value="${a}">${a}</option>`).join('')}</select>
    <button class="btn-clear" id="lf-clear">Clear</button>
  `;

  const columns = [
    { key: 'lease_number', label: 'Lease #', width: '90px', className: 'cell-mono' },
    { key: 'area_code', label: 'Area', width: '50px' },
    { key: 'block_number', label: 'Block', width: '70px' },
    { key: 'lease_status', label: 'Status', width: '90px', format: v => leaseStatusBadge(v) },
    { key: 'designated_operator', label: 'Operator' },
    { key: 'royalty_rate', label: 'Royalty', width: '70px', className: 'cell-number', format: v => v ? `${v}%` : '—' },
    { key: 'bid_amount', label: 'Bid', width: '100px', className: 'cell-number', format: v => v ? `$${formatNumber(v)}` : '—' },
    { key: 'max_water_depth', label: 'Water Depth', width: '90px', className: 'cell-number', format: v => formatDepth(v) },
    { key: 'first_production_date', label: '1st Prod', width: '90px', format: v => formatDate(v) },
  ];

  const table = new DataTable({
    container: document.getElementById('lease-table'),
    columns,
    fetchFn: (page, limit, sort, order, filters) => apiGet('/leases', { page, limit, sort, order, ...filters }),
    onRowClick: row => showLeaseDetail(row.lease_number),
  });

  const applyFilters = debounce(() => {
    table.setFilters({
      search: document.getElementById('lf-search').value || undefined,
      lease_status: document.getElementById('lf-status').value || undefined,
      area_code: document.getElementById('lf-area').value || undefined,
    });
  }, 300);

  filterEl.querySelectorAll('input, select').forEach(el => el.addEventListener('input', applyFilters));
  document.getElementById('lf-clear').addEventListener('click', () => {
    filterEl.querySelectorAll('input, select').forEach(el => el.value = '');
    applyFilters();
  });

  let detailChart = null;

  async function showLeaseDetail(id) {
    const detailEl = document.getElementById('lease-detail');
    detailEl.innerHTML = '<div class="detail-panel"><div class="loading-overlay"><div class="spinner"></div>Loading...</div></div>';

    const [lease, owners, prod, wells, platforms] = await Promise.all([
      apiGet(`/leases/${id}`),
      apiGet(`/leases/${id}/owners`),
      apiGet(`/leases/${id}/production`),
      apiGet(`/leases/${id}/wells`),
      apiGet(`/leases/${id}/platforms`),
    ]);

    detailEl.innerHTML = `
      <div class="detail-panel">
        <div class="detail-header">
          <div><h3>Lease ${escapeHtml(lease.lease_number)}</h3>
            <div class="detail-subtitle">${escapeHtml(lease.designated_operator || '')} · ${lease.area_code} ${lease.block_number}</div></div>
          <button class="detail-close" id="lease-close">×</button>
        </div>
        <div class="detail-body">
          <div class="kv-section">
            <div class="kv-section-title">Lease Details</div>
            ${kv('Status', leaseStatusBadge(lease.lease_status))}
            ${kv('Effective', formatDate(lease.effective_date))}
            ${kv('Expiration', formatDate(lease.expiration_date))}
            ${kv('Primary Term', lease.primary_term ? `${lease.primary_term} years` : '—')}
            ${kv('Royalty Rate', lease.royalty_rate ? `${lease.royalty_rate}%` : '—')}
            ${kv('Bid Amount', lease.bid_amount ? `$${formatNumber(lease.bid_amount)}` : '—')}
            ${kv('Water Depth', `${formatDepth(lease.min_water_depth)} – ${formatDepth(lease.max_water_depth)}`)}
            ${kv('Platforms', lease.num_platforms)}
            ${kv('1st Production', formatDate(lease.first_production_date))}
          </div>
          ${owners.data.length > 0 ? `
            <div class="kv-section">
              <div class="kv-section-title">Owners (${owners.data.length})</div>
              <table class="detail-subtable">
                <thead><tr><th>Company</th><th>Share</th><th>Status</th></tr></thead>
                <tbody>
                  ${owners.data.map(o => `<tr>
                    <td><a class="link-value" href="#/companies/${o.company_num}">${escapeHtml(o.company_name || o.company_num)}</a></td>
                    <td>${o.assignment_pct ? o.assignment_pct.toFixed(2) + '%' : '—'}</td>
                    <td>${o.assignment_status || '—'}</td>
                  </tr>`).join('')}
                </tbody>
              </table>
            </div>
          ` : ''}
          ${wells.data.length > 0 ? `
            <div class="kv-section">
              <div class="kv-section-title">Wells (${wells.data.length})</div>
              <table class="detail-subtable">
                <thead><tr><th>API Number</th><th>Name</th><th>Status</th><th>Type</th></tr></thead>
                <tbody>${wells.data.slice(0, 20).map(w => `<tr>
                  <td><a class="link-value" href="#/wells/${encodeURIComponent(w.api_well_number)}">${w.api_well_number}</a></td>
                  <td>${escapeHtml(w.well_name || '—')}</td>
                  <td>${wellStatusBadge(w.status_code)}</td>
                  <td>${w.type_code || '—'}</td>
                </tr>`).join('')}</tbody>
              </table>
              ${wells.data.length > 20 ? `<p style="padding:6px 0;font-size:11px;color:var(--color-text-muted)">Showing 20 of ${wells.data.length}</p>` : ''}
            </div>
          ` : ''}
          ${platforms.data.length > 0 ? `
            <div class="kv-section">
              <div class="kv-section-title">Platforms (${platforms.data.length})</div>
              <table class="detail-subtable">
                <thead><tr><th>Complex</th><th>Name</th><th>Type</th><th>Water</th></tr></thead>
                <tbody>${platforms.data.map(p => `<tr>
                  <td><a class="link-value" href="#/platforms/${p.complex_id}">${p.complex_id}</a></td>
                  <td>${escapeHtml(p.structure_name || '—')}</td>
                  <td>${p.structure_type || '—'}</td>
                  <td>${formatDepth(p.water_depth)}</td>
                </tr>`).join('')}</tbody>
              </table>
            </div>
          ` : ''}
          ${prod.data.length > 0 ? `
            <div class="kv-section">
              <div class="kv-section-title">Production History</div>
              <div class="detail-chart" id="lease-prod-chart"></div>
            </div>
          ` : ''}
        </div>
      </div>
    `;

    document.getElementById('lease-close').addEventListener('click', () => {
      detailEl.innerHTML = '<div class="detail-panel"><div class="detail-panel-empty">Select a lease to view details</div></div>';
    });

    if (prod.data.length > 0) {
      detailChart = new ChartPanel(document.getElementById('lease-prod-chart'));
      detailChart.update(productionChartConfig(prod.data));
    }
  }

  function kv(key, val) {
    return `<div class="kv-row"><span class="kv-key">${key}</span><span class="kv-value">${val ?? '—'}</span></div>`;
  }

  if (params.id) showLeaseDetail(params.id);
  table.load();

  return () => { table.destroy(); if (detailChart) detailChart.destroy(); };
}
