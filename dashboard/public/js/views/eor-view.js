import { apiGet } from '../core/api.js';
import { formatNumber, formatDate, formatDepth, escapeHtml } from '../core/utils.js';
import { DataTable } from '../components/data-table.js';
import { debounce } from '../core/utils.js';
import { initSplitResizer } from '../components/split-resizer.js';

const OP_LABELS = { A: 'Abandonment', C: 'Completion', D: 'Decommission', T: 'Temporary', Z: 'Zone Change' };
const HC_LABELS = { O: 'Oil', G: 'Gas', B: 'Both', N: 'Neither' };

const STATUS_LABELS = {
  PA: 'Plugged & Abandoned', TA: 'Temporarily Abandoned',
  COM: 'Completed', SI: 'Shut-In', DSI: 'Drilling Shut-In',
  PNC: 'Plug Not Completed', ZOI: 'Zone of Interest',
  APD: 'APD Approved',
};

function statusBadge(code) {
  if (!code) return '—';
  const label = STATUS_LABELS[code] || code;
  const cls = code === 'PA' ? 'badge badge-inactive' : code === 'COM' ? 'badge badge-active' : 'badge';
  return `<span class="${cls}" title="${label}">${escapeHtml(code)}</span>`;
}

export async function initEorView(container, params = {}) {

  container.innerHTML = `
    <div class="view-header">
      <h2>End of Operations Reports</h2>
    </div>
    <div class="filter-bar" id="eor-filters">
      <input type="text" id="ef-search" placeholder="Search EOR...">
      <input type="text" id="ef-area" placeholder="Area/Block...">
      <select id="ef-operation"><option value="">All Operations</option></select>
      <select id="ef-status"><option value="">All Statuses</option></select>
      <div class="filter-group">
        <span class="filter-group-label">Status Date</span>
        <input type="date" id="ef-date-from" title="From">
        <input type="date" id="ef-date-to" title="To">
      </div>
      <button class="btn-clear" id="ef-clear">Clear</button>
    </div>
    <div class="split-layout">
      <div class="split-main" id="eor-table"></div>
      <div class="split-detail" id="eor-detail">
        <div class="detail-panel">
          <div class="detail-panel-empty">Select an EOR to view details</div>
        </div>
      </div>
    </div>
  `;

  // Populate filter dropdowns
  try {
    const filters = await apiGet('/eor/filters');
    const opSel = document.getElementById('ef-operation');
    (filters.operations || []).forEach(op => {
      const o = document.createElement('option');
      o.value = op;
      o.textContent = OP_LABELS[op] || op;
      opSel.appendChild(o);
    });
    const stSel = document.getElementById('ef-status');
    (filters.statuses || []).forEach(st => {
      const o = document.createElement('option');
      o.value = st;
      o.textContent = STATUS_LABELS[st] || st;
      stSel.appendChild(o);
    });
  } catch (e) { /* filters optional */ }

  const columns = [
    { key: 'api_well_number', label: 'API Number', width: '130px', className: 'cell-mono' },
    { key: 'well_name', label: 'Well', width: '80px' },
    { key: 'operation_cd', label: 'Op', width: '40px', format: v => OP_LABELS[v] || v || '—' },
    { key: 'bus_asc_name', label: 'Operator' },
    { key: 'botm_area_code', label: 'Area', width: '50px' },
    { key: 'botm_block_number', label: 'Block', width: '60px' },
    { key: 'borehole_stat_cd', label: 'Status', width: '60px', format: statusBadge },
    { key: 'borehole_stat_dt', label: 'Date', width: '90px', format: v => formatDate(v) },
    { key: 'total_md', label: 'MD', width: '70px', className: 'cell-number', format: v => formatDepth(v) },
  ];

  const table = new DataTable({
    container: document.getElementById('eor-table'),
    columns,
    fetchFn: (page, limit, sort, order, filters) => apiGet('/eor', { page, limit, sort, order, ...filters }),
    onRowClick: row => showEorDetail(row.sn_eor),
  });

  const applyFilters = debounce(() => {
    table.setFilters({
      search: document.getElementById('ef-search').value || undefined,
      area_block: document.getElementById('ef-area').value || undefined,
      operation: document.getElementById('ef-operation').value || undefined,
      status: document.getElementById('ef-status').value || undefined,
      date_from: document.getElementById('ef-date-from').value || undefined,
      date_to: document.getElementById('ef-date-to').value || undefined,
    });
  }, 300);

  const filterEl = document.getElementById('eor-filters');
  filterEl.querySelectorAll('input, select').forEach(el => el.addEventListener('input', applyFilters));
  document.getElementById('ef-clear').addEventListener('click', () => {
    filterEl.querySelectorAll('input, select').forEach(el => el.value = '');
    applyFilters();
  });

  async function showEorDetail(sn) {
    const detailEl = document.getElementById('eor-detail');
    detailEl.innerHTML = '<div class="detail-panel"><div class="loading-overlay"><div class="spinner"></div>Loading...</div></div>';

    const [data, compsRes, casingsRes, geoRes, hcRes, perfRes] = await Promise.all([
      apiGet(`/eor/${sn}`),
      apiGet(`/eor/${sn}/completions`),
      apiGet(`/eor/${sn}/casings`),
      apiGet(`/eor/${sn}/geomarkers`),
      apiGet(`/eor/${sn}/hc-intervals`),
      apiGet(`/eor/${sn}/perforations`),
    ]);

    const opLabel = OP_LABELS[data.operation_cd] || data.operation_cd || '—';

    detailEl.innerHTML = `
      <div class="detail-panel">
        <div class="detail-header">
          <div>
            <h3>EOR ${data.sn_eor}</h3>
            <div class="detail-subtitle">${escapeHtml(data.well_name || '')} · ${opLabel} · ${statusBadge(data.borehole_stat_cd)}</div>
          </div>
          <button class="detail-close" id="eor-close">&times;</button>
        </div>
        <div class="detail-body">
          <div class="kv-section">
            <div class="kv-section-title">Report Info</div>
            ${kv('API Number', data.api_well_number ? `<a class="link-value" href="#/wells/${encodeURIComponent(data.api_well_number)}">${data.api_well_number}</a>` : '—')}
            ${kv('Well Name', `${escapeHtml(data.well_name || '—')} ST${data.well_nm_st_sfix || '00'} BP${data.well_nm_bp_sfix || '00'}`)}
            ${kv('Operation', opLabel)}
            ${kv('Status', (STATUS_LABELS[data.borehole_stat_cd] || data.borehole_stat_cd || '—'))}
            ${kv('Status Date', formatDate(data.borehole_stat_dt))}
            ${kv('Operator', data.company_name ? `<a class="link-value" href="#/companies/${data.company_num}">${escapeHtml(data.company_name)}</a>` : escapeHtml(data.bus_asc_name || '—'))}
          </div>
          <div class="kv-section">
            <div class="kv-section-title">Location</div>
            ${kv('Bottom', `${data.botm_area_code || ''} ${data.botm_block_number || ''}`)}
            ${kv('Bottom Lease', data.botm_lease_number ? `<a class="link-value" href="#/leases/${data.botm_lease_number}">${data.botm_lease_number}</a>` : '—')}
            ${kv('Surface', `${data.surf_area_code || ''} ${data.surf_block_number || ''}`)}
            ${kv('Surface Lease', data.surf_lease_number || '—')}
            ${kv('Lat / Lon', data.botm_latitude && data.botm_longitude ? `${data.botm_latitude.toFixed(5)}, ${data.botm_longitude.toFixed(5)}` : '—')}
          </div>
          <div class="kv-section">
            <div class="kv-section-title">Well Data</div>
            ${kv('Total MD', formatDepth(data.total_md))}
            ${kv('TVD', formatDepth(data.well_bore_tvd))}
            ${kv('Kickoff MD', formatDepth(data.kickoff_md))}
            ${kv('Subsea Completion', data.subsea_completion || '—')}
            ${kv('Subsea Protection', data.subsea_protection || '—')}
            ${kv('Obstruction Type', data.obstruction_type_cd && data.obstruction_type_cd !== 'NA' ? data.obstruction_type_cd : '—')}
          </div>
          ${data.operational_narrative ? `
            <div class="kv-section">
              <div class="kv-section-title">Narrative</div>
              <p style="font-size:12px;line-height:1.5;padding:4px 0;">${escapeHtml(data.operational_narrative)}</p>
            </div>
          ` : ''}
          ${compsRes.data.length > 0 ? `
            <div class="kv-section">
              <div class="kv-section-title">Completions (${compsRes.data.length})</div>
              <table class="detail-subtable">
                <thead><tr><th>Interval</th><th>Status</th><th>Reservoir</th><th>Area</th><th>Block</th></tr></thead>
                <tbody>${compsRes.data.map(c => `<tr>
                  <td>${escapeHtml(c.interval || '—')}</td>
                  <td>${escapeHtml(c.comp_status_cd || '—')}</td>
                  <td>${escapeHtml(c.comp_rsvr_name || '—')}</td>
                  <td>${c.comp_area_code || '—'}</td>
                  <td>${c.comp_block_number || '—'}</td>
                </tr>`).join('')}</tbody>
              </table>
            </div>
          ` : ''}
          ${casingsRes.data.length > 0 ? `
            <div class="kv-section">
              <div class="kv-section-title">Cut Casings (${casingsRes.data.length})</div>
              <table class="detail-subtable">
                <thead><tr><th>Size</th><th>Cut Date</th><th>Method</th><th>Depth</th><th>Position</th></tr></thead>
                <tbody>${casingsRes.data.map(c => `<tr>
                  <td>${c.casing_size || '—'}"</td>
                  <td>${formatDate(c.casing_cut_date)}</td>
                  <td>${c.casing_cut_method || '—'}</td>
                  <td class="cell-number">${formatDepth(c.casing_cut_depth)}</td>
                  <td>${c.casing_cut_mdl_ind || '—'}</td>
                </tr>`).join('')}</tbody>
              </table>
            </div>
          ` : ''}
          ${hcRes.data.length > 0 ? `
            <div class="kv-section">
              <div class="kv-section-title">HC-Bearing Intervals (${hcRes.data.length})</div>
              <table class="detail-subtable">
                <thead><tr><th>Interval</th><th>Type</th><th>Top MD</th><th>Bottom MD</th></tr></thead>
                <tbody>${hcRes.data.map(h => `<tr>
                  <td>${escapeHtml(h.interval_name || '—')}</td>
                  <td>${HC_LABELS[h.hydrocarbon_type_cd] || h.hydrocarbon_type_cd || '—'}</td>
                  <td class="cell-number">${formatDepth(h.top_md)}</td>
                  <td class="cell-number">${formatDepth(h.bottom_md)}</td>
                </tr>`).join('')}</tbody>
              </table>
            </div>
          ` : ''}
          ${perfRes.data.length > 0 ? `
            <div class="kv-section">
              <div class="kv-section-title">Perforations (${perfRes.data.length})</div>
              <table class="detail-subtable">
                <thead><tr><th>Top MD</th><th>Base MD</th><th>Top TVD</th><th>Base TVD</th></tr></thead>
                <tbody>${perfRes.data.map(p => `<tr>
                  <td class="cell-number">${formatDepth(p.perf_top_md)}</td>
                  <td class="cell-number">${formatDepth(p.perf_base_md)}</td>
                  <td class="cell-number">${formatDepth(p.perf_top_tvd)}</td>
                  <td class="cell-number">${formatDepth(p.perf_botm_tvd)}</td>
                </tr>`).join('')}</tbody>
              </table>
            </div>
          ` : ''}
          ${geoRes.data.length > 0 ? `
            <div class="kv-section">
              <div class="kv-section-title">Geologic Markers (${geoRes.data.length})</div>
              <table class="detail-subtable">
                <thead><tr><th>Marker</th><th>Top MD</th></tr></thead>
                <tbody>${geoRes.data.map(g => `<tr>
                  <td>${escapeHtml(g.geo_marker_name || '—')}</td>
                  <td class="cell-number">${formatDepth(g.top_md)}</td>
                </tr>`).join('')}</tbody>
              </table>
            </div>
          ` : ''}
        </div>
      </div>
    `;

    document.getElementById('eor-close').addEventListener('click', () => {
      detailEl.innerHTML = '<div class="detail-panel"><div class="detail-panel-empty">Select an EOR to view details</div></div>';
    });
  }

  function kv(key, val) {
    return `<div class="kv-row"><span class="kv-key">${key}</span><span class="kv-value">${val ?? '—'}</span></div>`;
  }

  const resizer = initSplitResizer(container.querySelector('.split-layout'));

  if (params.sn) showEorDetail(params.sn);
  table.load();

  return () => { table.destroy(); if (resizer) resizer.destroy(); };
}
