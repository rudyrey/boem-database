import { apiGet } from '../core/api.js';
import { formatNumber, formatDate, formatDepth, escapeHtml } from '../core/utils.js';
import { DataTable } from '../components/data-table.js';
import { MultiSelect } from '../components/multi-select.js';
import { debounce } from '../core/utils.js';
import { initSplitResizer } from '../components/split-resizer.js';

const OP_LABELS = {
  ABANDON: 'Abandonment',
  COMP: 'Completion',
  ENHANCE: 'Enhance Production',
  INFORM: 'Information',
  OTHER: 'Other',
  UTIL: 'Utility',
  WORKOVER: 'Workover',
};

function typeBadge(type) {
  const cls = type === 'APD' ? 'badge badge-drilling' : 'badge badge-modify';
  return `<span class="${cls}">${escapeHtml(type)}</span>`;
}

export async function initSubmissionsView(container, params = {}) {

  container.innerHTML = `
    <div class="view-header">
      <h2>Submissions Explorer</h2>
    </div>
    <div class="filter-bar" id="sub-filters">
      <input type="text" id="sf-search" placeholder="Search submissions...">
      <input type="text" id="sf-area" placeholder="Area/Block...">
      <div id="sf-type-ms"></div>
      <div id="sf-op-ms"></div>
      <div class="filter-group">
        <span class="filter-group-label">Status Date</span>
        <input type="date" id="sf-date-from" title="From">
        <input type="date" id="sf-date-to" title="To">
      </div>
      <button class="btn-clear" id="sf-clear">Clear</button>
    </div>
    <div class="split-layout">
      <div class="split-main" id="sub-table"></div>
      <div class="split-detail" id="sub-detail">
        <div class="detail-panel">
          <div class="detail-panel-empty">Select a submission to view details</div>
        </div>
      </div>
    </div>
  `;

  // Multi-select for submission type (APD / APM)
  const typeMS = new MultiSelect({
    container: document.getElementById('sf-type-ms'),
    label: 'Type',
    options: [
      { value: 'APD', label: 'APD — Permit to Drill' },
      { value: 'APM', label: 'APM — Permit to Modify' },
    ],
    onChange: () => applyFilters(),
  });

  // Multi-select for operation
  const opMS = new MultiSelect({
    container: document.getElementById('sf-op-ms'),
    label: 'Operation',
    options: Object.entries(OP_LABELS).map(([v, l]) => ({ value: v, label: l })),
    onChange: () => applyFilters(),
  });

  const columns = [
    { key: 'type', label: 'Type', width: '70px', format: (v) => typeBadge(v) },
    { key: 'sn', label: 'SN', width: '100px', className: 'cell-mono' },
    { key: 'api_well_number', label: 'API Number', width: '140px', className: 'cell-mono' },
    { key: 'well_name', label: 'Well Name', width: '110px' },
    { key: 'operator', label: 'Operator' },
    { key: 'operation', label: 'Operation', width: '110px', format: (v) => escapeHtml(OP_LABELS[v] || v || '—') },
    { key: 'area_block', label: 'Area/Block', width: '110px' },
    { key: 'status_date', label: 'Status Date', width: '100px', format: (v) => formatDate(v) },
    { key: 'secondary_date', label: 'Spud / Work Date', width: '115px', format: (v) => formatDate(v) },
    { key: 'water_depth', label: 'Water (ft)', width: '85px', className: 'cell-number', format: (v) => formatDepth(v) },
    { key: 'rig', label: 'Rig', width: '100px', format: (v) => escapeHtml(v || '—') },
  ];

  const table = new DataTable({
    container: document.getElementById('sub-table'),
    columns,
    fetchFn: async (page, limit, sort, order, filters) => {
      return apiGet('/submissions', { page, limit, sort, order, ...filters });
    },
    onRowClick: (row) => showSubmissionDetail(row.type, row.sn),
  });

  const applyFilters = debounce(() => {
    const typeVals = typeMS.getValues();
    const opVals = opMS.getValues();
    table.setFilters({
      search: document.getElementById('sf-search').value || undefined,
      area_block: document.getElementById('sf-area').value || undefined,
      submission_type: typeVals.length === 1 ? typeVals[0] : undefined,
      operation: opVals.length > 0 ? opVals.join(',') : undefined,
      date_from: document.getElementById('sf-date-from').value || undefined,
      date_to: document.getElementById('sf-date-to').value || undefined,
    });
  }, 300);

  document.querySelectorAll('#sub-filters input').forEach(el => el.addEventListener('input', applyFilters));
  document.getElementById('sf-clear').addEventListener('click', () => {
    document.querySelectorAll('#sub-filters input').forEach(el => el.value = '');
    typeMS.clear();
    opMS.clear();
    applyFilters();
  });

  // ─── Detail Panel ───────────────────────────────────────────────────────

  const SUBOP_LABELS = {
    ACIDIZE: 'Acidize', CASING: 'Casing Pressure Repair', CLEARNCE: 'Site Clearance',
    DESAND: 'Wash/Desand Well', FLUIDS: 'Additional Fluids For Injection',
    FRAC: 'Hydraulic Fracking', INITCOMP: 'Initial Completion',
    INJECT: 'Initial Injection Well', JET: 'Jet Well', LIFT: 'Artificial Lift',
    OTHER: 'Describe Operation(s)', OTHRABAN: 'Other Abandonment',
    OTHRCOMP: 'Other Completion', OTHRENPR: 'Other Enhance Production',
    OTHRINFO: 'Other Information', OTHRUTIL: 'Other Utility',
    OTHRWKVR: 'Other Workover', PERF: 'Modify Perforations',
    PERMABAN: 'Permanent Abandonment', PLUGBACK: 'Plugback To Sidetrack/Bypass',
    REPERF: 'Reperforation', SURFPLAT: 'Surface Location Plat',
    TEMPABAN: 'Temporary Abandonment', TUBING: 'Change Tubing',
    WELLNAME: 'Change Well Name', ZONE: 'Change Zone', ZONEISO: 'Zone Isolation',
  };

  function fmtPsi(v) { return v != null ? `${formatNumber(Math.round(v))} psi` : '—'; }
  function fmtPpg(v) { return v != null ? `${v} ppg` : '—'; }

  async function showSubmissionDetail(type, sn) {
    const detailEl = document.getElementById('sub-detail');
    detailEl.innerHTML = '<div class="detail-panel"><div class="loading-overlay"><div class="spinner"></div>Loading...</div></div>';

    const enc = encodeURIComponent(sn);

    if (type === 'APD') {
      const [d, casingRes, geoRes] = await Promise.all([
        apiGet(`/submissions/apd/${enc}`),
        apiGet(`/submissions/apd/${enc}/casing`),
        apiGet(`/submissions/apd/${enc}/geologic`),
      ]);
      renderAPDDetail(detailEl, d, casingRes.data, geoRes.data);
    } else {
      const [d, prevRes, subopRes] = await Promise.all([
        apiGet(`/submissions/apm/${enc}`),
        apiGet(`/submissions/apm/${enc}/preventers`),
        apiGet(`/submissions/apm/${enc}/suboperations`),
      ]);
      renderAPMDetail(detailEl, d, prevRes.data, subopRes.data);
    }
  }

  function renderAPDDetail(el, d, casingIntervals, geoMarkers) {
    el.innerHTML = `
      <div class="detail-panel">
        <div class="detail-header">
          <div>
            <h3>${typeBadge('APD')} ${escapeHtml(d.well_name || d.sn_apd)}</h3>
            <div class="detail-subtitle">${escapeHtml(d.operator_name || d.bus_asc_name || '')}</div>
          </div>
          <button class="detail-close" id="sub-detail-close">×</button>
        </div>
        <div class="detail-body">
          <div class="kv-section">
            <div class="kv-section-title">Permit Information</div>
            ${kv('Serial Number', d.sn_apd)}
            ${kv('Permit Type', d.permit_type)}
            ${kv('Well Type', d.well_type_code)}
            ${kv('Status Date', formatDate(d.apd_status_dt))}
            ${kv('Submitted', formatDate(d.apd_sub_status_dt))}
            ${kv('Requested Spud', formatDate(d.req_spud_date))}
          </div>
          <div class="kv-section">
            <div class="kv-section-title">Well</div>
            ${kv('API Number', d.api_well_number
              ? `<a class="link-value" href="#/wells/${d.api_well_number}">${d.api_well_number}</a>`
              : '—')}
            ${kv('Well Name', d.well_name)}
            ${kv('Water Depth', formatDepth(d.water_depth))}
          </div>
          <div class="kv-section">
            <div class="kv-section-title">Operator</div>
            ${kv('Name', d.operator_num
              ? `<a class="link-value" href="#/companies/${d.operator_num}">${escapeHtml(d.operator_name || d.bus_asc_name || '')}</a>`
              : escapeHtml(d.bus_asc_name || '—'))}
            ${kv('Company #', d.operator_num)}
          </div>
          <div class="kv-section">
            <div class="kv-section-title">Location</div>
            ${kv('Surface Area/Block', [d.surf_area_code, d.surf_block_number].filter(Boolean).join(' ') || '—')}
            ${kv('Surface Lease', d.surf_lease_number
              ? `<a class="link-value" href="#/leases/${d.surf_lease_number}">${d.surf_lease_number}</a>`
              : '—')}
            ${kv('Bottom Area/Block', [d.botm_area_code, d.botm_block_number].filter(Boolean).join(' ') || '—')}
            ${kv('Bottom Lease', d.botm_lease_number
              ? `<a class="link-value" href="#/leases/${d.botm_lease_number}">${d.botm_lease_number}</a>`
              : '—')}
          </div>
          <div class="kv-section">
            <div class="kv-section-title">Rig</div>
            ${kv('Name', d.rig_name)}
            ${kv('Type', d.rig_type_code)}
            ${kv('ID', d.rig_id_num)}
          </div>
          ${casingIntervals.length > 0 ? casingIntervals.map(intv => `
            <div class="kv-section">
              <div class="kv-section-title">Casing Interval ${intv.csng_intv_num || ''} — ${escapeHtml(intv.csng_intv_name || intv.csng_intv_type_cd || 'Unknown')}</div>
              ${kv('Hole Size', intv.csng_holesize ? `${intv.csng_holesize}"` : '—')}
              ${kv('Top MD', intv.csng_top_md != null ? `${formatNumber(intv.csng_top_md)} ft` : '—')}
              ${kv('Mud Weight', fmtPpg(intv.csng_mud_wgt_ppg))}
              ${kv('Mud Type', intv.csng_mud_type_cd)}
              ${kv('Frac Gradient', fmtPpg(intv.csng_frac_grad_ppg))}
              ${kv('Cement Volume', intv.csng_cement_vol != null ? `${formatNumber(intv.csng_cement_vol)} sacks` : '—')}
              <div class="kv-subsection-title">Pressure Tests</div>
              ${kv('BOP Stack Size', intv.csng_bop_stack_size ? `${intv.csng_bop_stack_size}"` : '—')}
              ${kv('Wellhead Rating', intv.csng_wellhead_rating ? fmtPsi(parseFloat(intv.csng_wellhead_rating)) : '—')}
              ${kv('BOP Rating', intv.csng_bop_rating ? fmtPsi(parseFloat(intv.csng_bop_rating)) : '—')}
              ${kv('Annular Rating', intv.csng_annular_rating ? fmtPsi(parseFloat(intv.csng_annular_rating)) : '—')}
              ${kv('Annular Test', fmtPsi(intv.csng_annular_test_prss))}
              ${kv('BOP/Diverter Test', fmtPsi(intv.csng_bop_div_test_prss))}
              ${kv('Mud Test', fmtPsi(intv.csng_mud_test_prss))}
              ${kv('Liner Test', fmtPsi(intv.csng_liner_test))}
              ${kv('Formation Test', fmtPsi(intv.csng_formation_test_prss))}
              ${intv.sections && intv.sections.length > 0 ? `
                <div class="kv-subsection-title">Casing Sections</div>
                <table class="detail-subtable">
                  <thead><tr><th>#</th><th>Size</th><th>Weight</th><th>Grade</th><th>MD</th><th>TVD</th><th>Burst</th><th>Collapse</th></tr></thead>
                  <tbody>${intv.sections.map(sec => `<tr>
                    <td>${sec.casing_section_num ?? '—'}</td>
                    <td>${sec.casing_size ? `${sec.casing_size}"` : '—'}</td>
                    <td>${sec.casing_weight != null ? `${sec.casing_weight} ppf` : '—'}</td>
                    <td>${escapeHtml(sec.casing_grade || '—')}</td>
                    <td>${sec.casing_section_md != null ? `${formatNumber(sec.casing_section_md)}'` : '—'}</td>
                    <td>${sec.casing_section_tvd != null ? `${formatNumber(sec.casing_section_tvd)}'` : '—'}</td>
                    <td>${fmtPsi(sec.casing_burst_psi)}</td>
                    <td>${fmtPsi(sec.casing_collapse_psi)}</td>
                  </tr>`).join('')}</tbody>
                </table>
              ` : ''}
            </div>
          `).join('') : ''}
          ${geoMarkers.length > 0 ? `
            <div class="kv-section">
              <div class="kv-section-title">Geologic Markers (${geoMarkers.length})</div>
              <table class="detail-subtable">
                <thead><tr><th>Formation</th><th>Top MD (ft)</th><th>H2S</th></tr></thead>
                <tbody>${geoMarkers.map(g => `<tr>
                  <td>${escapeHtml(g.geo_marker_name || '—')}</td>
                  <td>${g.top_md != null ? formatNumber(g.top_md) : '—'}</td>
                  <td>${escapeHtml(g.h2s_designation || '—')}</td>
                </tr>`).join('')}</tbody>
              </table>
            </div>
          ` : ''}
        </div>
      </div>
    `;
    el.querySelector('#sub-detail-close').addEventListener('click', closeDetail);
  }

  function renderAPMDetail(el, d, preventers, subops) {
    el.innerHTML = `
      <div class="detail-panel">
        <div class="detail-header">
          <div>
            <h3>${typeBadge('APM')} ${escapeHtml(d.well_name || d.sn_apm)}</h3>
            <div class="detail-subtitle">${escapeHtml(d.operator_name || d.bus_asc_name || '')}</div>
          </div>
          <button class="detail-close" id="sub-detail-close">×</button>
        </div>
        <div class="detail-body">
          <div class="kv-section">
            <div class="kv-section-title">Permit Information</div>
            ${kv('Serial Number', d.sn_apm)}
            ${kv('Operation', OP_LABELS[d.apm_op_cd] || d.apm_op_cd || '—')}
            ${kv('Well Type', d.well_type_code)}
            ${kv('Borehole Status', d.borehole_stat_cd)}
            ${kv('Status Date', formatDate(d.acc_status_date))}
            ${kv('Submitted', formatDate(d.sub_stat_date))}
            ${kv('Work Commences', formatDate(d.work_commences_date))}
            ${kv('Est. Duration', d.est_operation_days != null ? `${d.est_operation_days} days` : '—')}
            ${kv('Service Type', d.sv_type)}
          </div>
          ${subops.length > 0 ? `
            <div class="kv-section">
              <div class="kv-section-title">Sub-Operations (${subops.length})</div>
              <div class="tag-list">${subops.map(op =>
                `<span class="tag">${escapeHtml(SUBOP_LABELS[op.apm_subop_cd] || op.apm_subop_cd || '—')}</span>`
              ).join('')}</div>
            </div>
          ` : ''}
          <div class="kv-section">
            <div class="kv-section-title">Well</div>
            ${kv('API Number', d.api_well_number
              ? `<a class="link-value" href="#/wells/${d.api_well_number}">${d.api_well_number}</a>`
              : '—')}
            ${kv('Well Name', d.well_name)}
            ${kv('Water Depth', formatDepth(d.water_depth))}
          </div>
          <div class="kv-section">
            <div class="kv-section-title">Operator</div>
            ${kv('Name', d.operator_num
              ? `<a class="link-value" href="#/companies/${d.operator_num}">${escapeHtml(d.operator_name || d.bus_asc_name || '')}</a>`
              : escapeHtml(d.bus_asc_name || '—'))}
            ${kv('Company #', d.operator_num)}
          </div>
          <div class="kv-section">
            <div class="kv-section-title">Location</div>
            ${kv('Surface Area/Block', [d.surf_area_code, d.surf_block_num].filter(Boolean).join(' ') || '—')}
            ${kv('Surface Lease', d.surf_lease_num
              ? `<a class="link-value" href="#/leases/${d.surf_lease_num}">${d.surf_lease_num}</a>`
              : '—')}
            ${kv('Bottom Area/Block', [d.botm_area_code, d.botm_block_num].filter(Boolean).join(' ') || '—')}
            ${kv('Bottom Lease', d.botm_lease_num
              ? `<a class="link-value" href="#/leases/${d.botm_lease_num}">${d.botm_lease_num}</a>`
              : '—')}
          </div>
          ${preventers.length > 0 ? `
            <div class="kv-section">
              <div class="kv-section-title">BOP / Preventers (${preventers.length})</div>
              <table class="detail-subtable">
                <thead><tr><th>Type</th><th>Stack Size</th><th>Working</th><th>High Test</th><th>Low Test</th></tr></thead>
                <tbody>${preventers.map(p => `<tr>
                  <td>${escapeHtml(p.apm_preventer_cd || '—')}</td>
                  <td>${p.bop_stack_size ? `${p.bop_stack_size}"` : '—'}</td>
                  <td>${fmtPsi(p.bop_working_prss)}</td>
                  <td>${fmtPsi(p.bop_high_test_prss)}</td>
                  <td>${fmtPsi(p.bop_low_test_prss)}</td>
                </tr>`).join('')}</tbody>
              </table>
            </div>
          ` : ''}
          <div class="kv-section">
            <div class="kv-section-title">Rig</div>
            ${kv('Rig ID', d.rig_id_num)}
          </div>
        </div>
      </div>
    `;
    el.querySelector('#sub-detail-close').addEventListener('click', closeDetail);
  }

  function closeDetail() {
    document.getElementById('sub-detail').innerHTML =
      '<div class="detail-panel"><div class="detail-panel-empty">Select a submission to view details</div></div>';
  }

  function kv(key, val) {
    return `<div class="kv-row"><span class="kv-key">${key}</span><span class="kv-value">${val ?? '—'}</span></div>`;
  }

  // Deep-link: open a specific submission if routed with type+sn
  if (params.type && params.sn) {
    showSubmissionDetail(params.type.toUpperCase(), params.sn);
  }

  const resizer = initSplitResizer(container.querySelector('.split-layout'));

  table.load();

  return () => {
    table.destroy();
    if (resizer) resizer.destroy();
    typeMS.destroy();
    opMS.destroy();
  };
}
