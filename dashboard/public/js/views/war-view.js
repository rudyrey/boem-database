import { apiGet } from '../core/api.js';
import { formatNumber, formatDate, formatDepth, escapeHtml } from '../core/utils.js';
import { DataTable } from '../components/data-table.js';
import { debounce } from '../core/utils.js';
import { initSplitResizer } from '../components/split-resizer.js';

const ACTIVITY_LABELS = {
  COM: 'Completion', DRL: 'Drilling', PA: 'Plugged & Abandoned',
  SI: 'Shut-In', TA: 'Temporarily Abandoned', WO: 'Workover',
  DSI: 'Drilling Shut-In', ST: 'Sidetrack',
};

function durationDays(start, end) {
  if (!start || !end) return '—';
  const d1 = new Date(start), d2 = new Date(end);
  if (isNaN(d1) || isNaN(d2)) return '—';
  const days = Math.round((d2 - d1) / 86400000);
  return days >= 0 ? `${days}d` : '—';
}

export async function initWarView(container, params = {}) {

  container.innerHTML = `
    <div class="view-header">
      <h2>Well Activity Reports</h2>
    </div>
    <div class="filter-bar" id="war-filters">
      <input type="text" id="wf-search" placeholder="Search WAR...">
      <input type="text" id="wf-area" placeholder="Area/Block...">
      <input type="text" id="wf-rig" placeholder="Rig name...">
      <select id="wf-activity"><option value="">All Activities</option></select>
      <div class="filter-group">
        <span class="filter-group-label">Date Range</span>
        <input type="date" id="wf-date-from" title="From">
        <input type="date" id="wf-date-to" title="To">
      </div>
      <button class="btn-clear" id="wf-clear">Clear</button>
    </div>
    <div class="split-layout">
      <div class="split-main" id="war-table"></div>
      <div class="split-detail" id="war-detail">
        <div class="detail-panel">
          <div class="detail-panel-empty">Select a WAR to view details</div>
        </div>
      </div>
    </div>
  `;

  // Populate filter dropdowns
  try {
    const filters = await apiGet('/war/filters');
    const actSel = document.getElementById('wf-activity');
    (filters.activities || []).forEach(a => {
      const o = document.createElement('option');
      o.value = a;
      o.textContent = ACTIVITY_LABELS[a] || a;
      actSel.appendChild(o);
    });
  } catch (e) { /* filters optional */ }

  const columns = [
    { key: 'api_well_number', label: 'API Number', width: '130px', className: 'cell-mono' },
    { key: 'well_name', label: 'Well', width: '70px' },
    { key: 'rig_name', label: 'Rig' },
    { key: 'bus_asc_name', label: 'Operator' },
    { key: 'well_activity_cd', label: 'Activity', width: '60px', format: v => ACTIVITY_LABELS[v] || v || '—' },
    { key: 'botm_area_code', label: 'Area', width: '50px' },
    { key: 'botm_block_num', label: 'Block', width: '60px' },
    { key: 'war_start_dt', label: 'Start', width: '90px', format: v => formatDate(v) },
    { key: 'war_end_dt', label: 'End', width: '90px', format: v => formatDate(v) },
    { key: 'water_depth', label: 'Water', width: '70px', className: 'cell-number', format: v => formatDepth(v) },
  ];

  const table = new DataTable({
    container: document.getElementById('war-table'),
    columns,
    fetchFn: (page, limit, sort, order, filters) => apiGet('/war', { page, limit, sort, order, ...filters }),
    onRowClick: row => showWarDetail(row.sn_war),
  });

  const applyFilters = debounce(() => {
    table.setFilters({
      search: document.getElementById('wf-search').value || undefined,
      area_block: document.getElementById('wf-area').value || undefined,
      rig: document.getElementById('wf-rig').value || undefined,
      activity: document.getElementById('wf-activity').value || undefined,
      date_from: document.getElementById('wf-date-from').value || undefined,
      date_to: document.getElementById('wf-date-to').value || undefined,
    });
  }, 300);

  const filterEl = document.getElementById('war-filters');
  filterEl.querySelectorAll('input, select').forEach(el => el.addEventListener('input', applyFilters));
  document.getElementById('wf-clear').addEventListener('click', () => {
    filterEl.querySelectorAll('input, select').forEach(el => el.value = '');
    applyFilters();
  });

  async function showWarDetail(sn) {
    const detailEl = document.getElementById('war-detail');
    detailEl.innerHTML = '<div class="detail-panel"><div class="loading-overlay"><div class="spinner"></div>Loading...</div></div>';

    const [data, tubRes] = await Promise.all([
      apiGet(`/war/${sn}`),
      apiGet(`/war/${sn}/tubulars`),
    ]);

    const actLabel = ACTIVITY_LABELS[data.well_activity_cd] || data.well_activity_cd || '—';
    const duration = durationDays(data.war_start_dt, data.war_end_dt);

    detailEl.innerHTML = `
      <div class="detail-panel">
        <div class="detail-header">
          <div>
            <h3>WAR ${data.sn_war}</h3>
            <div class="detail-subtitle">${escapeHtml(data.well_name || '')} · ${escapeHtml(data.rig_name || 'No rig')} · ${duration}</div>
          </div>
          <button class="detail-close" id="war-close">&times;</button>
        </div>
        <div class="detail-body">
          <div class="kv-section">
            <div class="kv-section-title">Report Info</div>
            ${kv('API Number', data.api_well_number ? `<a class="link-value" href="#/wells/${encodeURIComponent(data.api_well_number)}">${data.api_well_number}</a>` : '—')}
            ${kv('Well Name', `${escapeHtml(data.well_name || '—')} ST${data.well_nm_st_sfix || '00'} BP${data.well_nm_bp_sfix || '00'}`)}
            ${kv('Activity', actLabel)}
            ${kv('Operator', data.company_name ? `<a class="link-value" href="#/companies/${data.company_num}">${escapeHtml(data.company_name)}</a>` : escapeHtml(data.bus_asc_name || '—'))}
            ${kv('Contact', data.contact_name || '—')}
          </div>
          <div class="kv-section">
            <div class="kv-section-title">Timeline</div>
            ${kv('WAR Start', formatDate(data.war_start_dt))}
            ${kv('WAR End', formatDate(data.war_end_dt))}
            ${kv('Duration', duration)}
            ${kv('Activity Start', formatDate(data.well_actv_start_dt))}
            ${kv('Activity End', formatDate(data.well_actv_end_dt))}
            ${kv('TD Date', formatDate(data.total_depth_date))}
          </div>
          <div class="kv-section">
            <div class="kv-section-title">Rig & Equipment</div>
            ${kv('Rig Name', escapeHtml(data.rig_name || '—'))}
            ${kv('Water Depth', formatDepth(data.water_depth))}
            ${kv('RKB Elevation', data.rkb_elevation ? `${formatNumber(data.rkb_elevation)} ft` : '—')}
          </div>
          <div class="kv-section">
            <div class="kv-section-title">Well Data</div>
            ${kv('Drilling MD', formatDepth(data.drilling_md))}
            ${kv('Drilling TVD', formatDepth(data.drilling_tvd))}
            ${kv('Mud Weight', data.drill_fluid_wgt ? `${data.drill_fluid_wgt} ppg` : '—')}
          </div>
          <div class="kv-section">
            <div class="kv-section-title">BOP Test</div>
            ${kv('Test Date', formatDate(data.bop_test_date))}
            ${kv('Ram Pressure', data.ram_tst_prss ? `${formatNumber(data.ram_tst_prss)} psi` : '—')}
            ${kv('Annular Pressure', data.annular_tst_prss ? `${formatNumber(data.annular_tst_prss)} psi` : '—')}
          </div>
          <div class="kv-section">
            <div class="kv-section-title">Location</div>
            ${kv('Bottom', `${data.botm_area_code || ''} ${data.botm_block_num || ''}`)}
            ${kv('Bottom Lease', data.botm_lease_num ? `<a class="link-value" href="#/leases/${data.botm_lease_num}">${data.botm_lease_num}</a>` : '—')}
            ${kv('Surface', `${data.surf_area_code || ''} ${data.surf_block_num || ''}`)}
            ${kv('Surface Lease', data.surf_lease_num || '—')}
          </div>
          ${tubRes.data.length > 0 ? `
            <div class="kv-section">
              <div class="kv-section-title">Tubulars (${tubRes.data.length})</div>
              <table class="detail-subtable">
                <thead><tr><th>Type</th><th>Hole</th><th>Casing</th><th>Wt</th><th>Grade</th><th>Top</th><th>Bottom</th><th>Cement</th></tr></thead>
                <tbody>${tubRes.data.map(t => `<tr>
                  <td>${t.csng_intv_type_cd || '—'}</td>
                  <td>${t.csng_hole_size || '—'}"</td>
                  <td>${t.casing_size || '—'}"</td>
                  <td class="cell-number">${t.casing_weight || '—'}</td>
                  <td>${t.casing_grade || '—'}</td>
                  <td class="cell-number">${formatDepth(t.csng_setting_top_md)}</td>
                  <td class="cell-number">${formatDepth(t.csng_setting_botm_md)}</td>
                  <td class="cell-number">${t.csng_cement_vol ? formatNumber(t.csng_cement_vol) + ' sk' : '—'}</td>
                </tr>`).join('')}</tbody>
              </table>
            </div>
          ` : ''}
        </div>
      </div>
    `;

    document.getElementById('war-close').addEventListener('click', () => {
      detailEl.innerHTML = '<div class="detail-panel"><div class="detail-panel-empty">Select a WAR to view details</div></div>';
    });
  }

  function kv(key, val) {
    return `<div class="kv-row"><span class="kv-key">${key}</span><span class="kv-value">${val ?? '—'}</span></div>`;
  }

  const resizer = initSplitResizer(container.querySelector('.split-layout'));

  if (params.sn) showWarDetail(params.sn);
  table.load();

  return () => { table.destroy(); if (resizer) resizer.destroy(); };
}
