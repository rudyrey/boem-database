import { apiGet, getStats } from '../core/api.js';
import { formatNumber, formatDate, formatDepth, wellStatusBadge, escapeHtml } from '../core/utils.js';
import { DataTable } from '../components/data-table.js';
import { ChartPanel, productionChartConfig } from '../components/chart-panel.js';
import { MiniMap } from '../components/mini-map.js';
import { MultiSelect } from '../components/multi-select.js';
import { debounce } from '../core/utils.js';

export async function initWellsView(container, params = {}) {
  const stats = await getStats();
  const opts = stats.filterOptions;
  const statusLabels = opts.wellStatusLabels || {};
  const typeLabels = opts.wellTypeLabels || {};

  container.innerHTML = `
    <div class="view-header">
      <h2>Wells Explorer <span class="subtitle">${formatNumber(stats.counts.wells)} wells</span></h2>
      <button class="btn-export" id="wf-export" title="Export current view to CSV">⤓ Export CSV</button>
    </div>
    <div class="filter-bar" id="well-filters">
      <input type="text" id="wf-search" placeholder="Search wells...">
      <div id="wf-area-ms"></div>
      <div id="wf-status-ms"></div>
      <div id="wf-type-ms"></div>
      <div class="filter-group">
        <span class="filter-group-label">Spud</span>
        <input type="date" id="wf-spud-from" title="Spud date from">
        <input type="date" id="wf-spud-to" title="Spud date to">
      </div>
      <div class="filter-group">
        <span class="filter-group-label">MD (ft)</span>
        <input type="number" id="wf-min-depth" placeholder="Min" style="width:85px">
        <input type="number" id="wf-max-depth" placeholder="Max" style="width:85px">
      </div>
      <div class="filter-group">
        <span class="filter-group-label">TVD (ft)</span>
        <input type="number" id="wf-min-tvd" placeholder="Min" style="width:85px">
        <input type="number" id="wf-max-tvd" placeholder="Max" style="width:85px">
      </div>
      <div class="filter-group">
        <span class="filter-group-label">Water (ft)</span>
        <input type="number" id="wf-min-wd" placeholder="Min" style="width:85px">
        <input type="number" id="wf-max-wd" placeholder="Max" style="width:85px">
      </div>
      <button class="btn-clear" id="wf-clear">Clear</button>
    </div>
    <div class="split-layout">
      <div class="split-main" id="well-table"></div>
      <div class="split-detail" id="well-detail">
        <div class="detail-panel">
          <div class="detail-panel-empty">Select a well to view details</div>
        </div>
      </div>
    </div>
  `;

  // Multi-select for area/block
  const areaMS = new MultiSelect({
    container: document.getElementById('wf-area-ms'),
    label: 'Area/Block',
    options: (opts.wellAreaBlocks || []).map(a => ({ value: a, label: a })),
    onChange: () => applyFilters(),
  });

  // Multi-select for status
  const statusMS = new MultiSelect({
    container: document.getElementById('wf-status-ms'),
    label: 'Status',
    options: opts.wellStatus.map(s => ({ value: s, label: statusLabels[s] || s })),
    onChange: () => applyFilters(),
  });

  // Multi-select for type
  const typeMS = new MultiSelect({
    container: document.getElementById('wf-type-ms'),
    label: 'Type',
    options: opts.wellType.map(t => ({ value: t, label: typeLabels[t] || t })),
    onChange: () => applyFilters(),
  });

  const columns = [
    { key: 'api_well_number', label: 'API Number', width: '140px', className: 'cell-mono' },
    { key: 'platform_name', label: 'Facility', width: '120px', format: (v) => v || '—' },
    { key: 'area_block', label: 'Area/Block', width: '120px' },
    { key: 'well_name', label: 'Well Name', width: '120px' },
    { key: 'operator_name', label: 'Operator' },
    { key: 'status_code', label: 'Status', width: '90px', format: (v) => wellStatusBadge(v) },
    { key: 'type_code', label: 'Type', width: '80px', format: (v) => typeLabels[v] || v || '—' },
    { key: 'total_measured_depth', label: 'MD', width: '80px', className: 'cell-number', format: (v) => formatDepth(v) },
    { key: 'water_depth', label: 'Water', width: '70px', className: 'cell-number', format: (v) => formatDepth(v) },
    { key: 'spud_date', label: 'Spud Date', width: '100px', format: (v) => formatDate(v) },
  ];

  const table = new DataTable({
    container: document.getElementById('well-table'),
    columns,
    fetchFn: async (page, limit, sort, order, filters) => {
      return apiGet('/wells', { page, limit, sort, order, ...filters });
    },
    onRowClick: (row) => showWellDetail(row.api_well_number),
  });

  const applyFilters = debounce(() => {
    const areaVals = areaMS.getValues();
    const statusVals = statusMS.getValues();
    const typeVals = typeMS.getValues();
    table.setFilters({
      search: document.getElementById('wf-search').value || undefined,
      area_block: areaVals.length > 0 ? areaVals.join(',') : undefined,
      status_code: statusVals.length > 0 ? statusVals.join(',') : undefined,
      type_code: typeVals.length > 0 ? typeVals.join(',') : undefined,
      spud_from: document.getElementById('wf-spud-from').value || undefined,
      spud_to: document.getElementById('wf-spud-to').value || undefined,
      min_depth: document.getElementById('wf-min-depth').value || undefined,
      max_depth: document.getElementById('wf-max-depth').value || undefined,
      min_tvd: document.getElementById('wf-min-tvd').value || undefined,
      max_tvd: document.getElementById('wf-max-tvd').value || undefined,
      min_water_depth: document.getElementById('wf-min-wd').value || undefined,
      max_water_depth: document.getElementById('wf-max-wd').value || undefined,
    });
  }, 300);

  // Wire up all non-MultiSelect inputs
  document.querySelectorAll('#well-filters input').forEach(el => el.addEventListener('input', applyFilters));
  document.getElementById('wf-clear').addEventListener('click', () => {
    document.querySelectorAll('#well-filters input').forEach(el => el.value = '');
    areaMS.clear();
    statusMS.clear();
    typeMS.clear();
    applyFilters();
  });

  // Export CSV — fetches all filtered results, not just current page
  document.getElementById('wf-export').addEventListener('click', async () => {
    const btn = document.getElementById('wf-export');
    btn.disabled = true;
    btn.textContent = 'Exporting...';

    try {
      const result = await apiGet('/wells', {
        page: 1,
        limit: 100000,
        sort: table.sort,
        order: table.order,
        ...table.filters,
      });
      const rows = result.data;
      if (!rows || rows.length === 0) return;

      const csvColumns = [
        { key: 'api_well_number', label: 'API Number' },
        { key: 'well_name', label: 'Well Name' },
        { key: 'operator_name', label: 'Operator' },
        { key: 'status_code', label: 'Status' },
        { key: 'type_code', label: 'Type' },
        { key: 'total_measured_depth', label: 'Measured Depth (ft)' },
        { key: 'water_depth', label: 'Water Depth (ft)' },
        { key: 'platform_name', label: 'Facility' },
        { key: 'area_block', label: 'Area/Block' },
        { key: 'spud_date', label: 'Spud Date' },
      ];

      const escape = (v) => {
        if (v == null) return '';
        const s = String(v);
        return s.includes(',') || s.includes('"') || s.includes('\n')
          ? '"' + s.replace(/"/g, '""') + '"'
          : s;
      };

      const header = csvColumns.map(c => escape(c.label)).join(',');
      const body = rows.map(row =>
        csvColumns.map(c => escape(row[c.key])).join(',')
      ).join('\n');

      const blob = new Blob([header + '\n' + body], { type: 'text/csv' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'wells-export.csv';
      a.click();
      URL.revokeObjectURL(url);
    } finally {
      btn.disabled = false;
      btn.textContent = '⤓ Export CSV';
    }
  });

  // Detail panel
  let detailChart = null;
  let detailMap = null;

  async function showWellDetail(id) {
    const detailEl = document.getElementById('well-detail');
    detailEl.innerHTML = '<div class="detail-panel"><div class="loading-overlay"><div class="spinner"></div>Loading...</div></div>';

    const [well, prodRes, platRes] = await Promise.all([
      apiGet(`/wells/${encodeURIComponent(id)}`),
      apiGet(`/wells/${encodeURIComponent(id)}/production`),
      apiGet(`/wells/${encodeURIComponent(id)}/platforms`),
    ]);

    detailEl.innerHTML = `
      <div class="detail-panel">
        <div class="detail-header">
          <div>
            <h3>${escapeHtml(well.well_name || well.api_well_number)}</h3>
            <div class="detail-subtitle">${well.operator_num
              ? `<a class="link-value" href="#/companies/${well.operator_num}">${escapeHtml(well.operator_name || '')}</a>`
              : escapeHtml(well.operator_name || '')}</div>
          </div>
          <button class="detail-close" id="well-detail-close">×</button>
        </div>
        <div class="detail-body">
          <div id="well-mini-map"></div>
          <div class="kv-section">
            <div class="kv-section-title">Well Information</div>
            ${kv('API Number', well.api_well_number)}
            ${kv('Status', wellStatusBadge(well.status_code))}
            ${kv('Type', typeLabels[well.type_code] || well.type_code || '—')}
            ${kv('Operator', well.operator_num
              ? `<a class="link-value" href="#/companies/${well.operator_num}">${escapeHtml(well.operator_name)}</a>`
              : (well.operator_name || '—'))}
            ${kv('Spud Date', formatDate(well.spud_date))}
            ${kv('Completion Date', formatDate(well.completion_date))}
            ${kv('Measured Depth', formatDepth(well.total_measured_depth))}
            ${kv('True Vertical Depth', formatDepth(well.true_vertical_depth))}
            ${kv('Water Depth', formatDepth(well.water_depth))}
            ${kv('Area/Block', well.area_block)}
            ${kv('Lease', well.bottom_lease_number ? `<a class="link-value" href="#/leases/${well.bottom_lease_number}">${well.bottom_lease_number}</a>` : '—')}
          </div>
          ${platRes.data.length > 0 ? `
            <div class="kv-section">
              <div class="kv-section-title">Related Platforms (${platRes.data.length})</div>
              <table class="detail-subtable">
                <thead><tr><th>Complex</th><th>Name</th><th>Type</th></tr></thead>
                <tbody>${platRes.data.map(p => `<tr>
                  <td><a class="link-value" href="#/platforms/${p.complex_id}">${p.complex_id}</a></td>
                  <td>${escapeHtml(p.structure_name || '—')}</td>
                  <td>${p.structure_type || '—'}</td>
                </tr>`).join('')}</tbody>
              </table>
            </div>
          ` : ''}
          ${prodRes.data.length > 0 ? `
            <div class="kv-section">
              <div class="kv-section-title">Production History</div>
              <div class="detail-chart" id="well-prod-chart"></div>
            </div>
          ` : ''}
        </div>
      </div>
    `;

    document.getElementById('well-detail-close').addEventListener('click', () => {
      detailEl.innerHTML = '<div class="detail-panel"><div class="detail-panel-empty">Select a well to view details</div></div>';
    });

    // Mini map
    if (well.surface_latitude && well.surface_longitude) {
      detailMap = new MiniMap(document.getElementById('well-mini-map'), {
        lat: well.surface_latitude, lng: well.surface_longitude, zoom: 11,
      });
    }

    // Production chart
    if (prodRes.data.length > 0) {
      detailChart = new ChartPanel(document.getElementById('well-prod-chart'));
      detailChart.update(productionChartConfig(prodRes.data));
    }
  }

  function kv(key, val) {
    return `<div class="kv-row"><span class="kv-key">${key}</span><span class="kv-value">${val ?? '—'}</span></div>`;
  }

  // If navigated with an ID, show that well
  if (params.id) {
    showWellDetail(params.id);
  }

  table.load();

  return () => {
    table.destroy();
    areaMS.destroy();
    statusMS.destroy();
    typeMS.destroy();
    if (detailChart) detailChart.destroy();
    if (detailMap) detailMap.destroy();
  };
}
