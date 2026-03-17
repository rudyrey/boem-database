import { apiGet, getStats } from '../core/api.js';
import { formatNumber, formatDate, formatDepth, escapeHtml, flagDot, wellStatusBadge } from '../core/utils.js';
import { DataTable } from '../components/data-table.js';
import { MiniMap } from '../components/mini-map.js';
import { debounce } from '../core/utils.js';
import { initSplitResizer } from '../components/split-resizer.js';

export async function initPlatformsView(container, params = {}) {
  const stats = await getStats();

  container.innerHTML = `
    <div class="view-header">
      <h2>Platform Explorer <span class="subtitle">${formatNumber(stats.counts.platforms)} platforms</span></h2>
    </div>
    <div class="filter-bar" id="plat-filters"></div>
    <div class="split-layout">
      <div class="split-main" id="plat-table"></div>
      <div class="split-detail" id="plat-detail">
        <div class="detail-panel"><div class="detail-panel-empty">Select a platform to view details</div></div>
      </div>
    </div>
  `;

  const filterEl = document.getElementById('plat-filters');
  filterEl.innerHTML = `
    <input type="text" id="pf-search" placeholder="Search platforms...">
    <select id="pf-oil"><option value="">Oil Prod?</option><option value="Y">Yes</option><option value="N">No</option></select>
    <select id="pf-gas"><option value="">Gas Prod?</option><option value="Y">Yes</option><option value="N">No</option></select>
    <select id="pf-drill"><option value="">Drilling?</option><option value="Y">Yes</option><option value="N">No</option></select>
    <button class="btn-clear" id="pf-clear">Clear</button>
  `;

  const yesNo = v => v === 'Y' ? '<span class="flag-yes">Y</span>' : '<span class="flag-no">N</span>';

  const columns = [
    { key: 'complex_id', label: 'Complex ID', width: '90px', className: 'cell-mono' },
    { key: 'structure_name', label: 'Name' },
    { key: 'area_code', label: 'Area', width: '50px' },
    { key: 'block_number', label: 'Block', width: '70px' },
    { key: 'operator_name', label: 'Operator' },
    { key: 'structure_type', label: 'Type', width: '70px' },
    { key: 'water_depth', label: 'Water', width: '80px', className: 'cell-number', format: v => formatDepth(v) },
    { key: 'oil_producing', label: 'Oil', width: '40px', format: yesNo },
    { key: 'gas_producing', label: 'Gas', width: '40px', format: yesNo },
    { key: 'drilling', label: 'Drill', width: '40px', format: yesNo },
    { key: 'install_date', label: 'Installed', width: '90px', format: v => formatDate(v) },
  ];

  const table = new DataTable({
    container: document.getElementById('plat-table'),
    columns,
    fetchFn: (page, limit, sort, order, filters) => apiGet('/platforms', { page, limit, sort, order, ...filters }),
    onRowClick: row => showPlatformDetail(row.complex_id),
  });

  const applyFilters = debounce(() => {
    table.setFilters({
      search: document.getElementById('pf-search').value || undefined,
      oil_producing: document.getElementById('pf-oil').value || undefined,
      gas_producing: document.getElementById('pf-gas').value || undefined,
      drilling: document.getElementById('pf-drill').value || undefined,
    });
  }, 300);

  filterEl.querySelectorAll('input, select').forEach(el => el.addEventListener('input', applyFilters));
  document.getElementById('pf-clear').addEventListener('click', () => {
    filterEl.querySelectorAll('input, select').forEach(el => el.value = '');
    applyFilters();
  });

  let detailMap = null;

  async function showPlatformDetail(id) {
    const detailEl = document.getElementById('plat-detail');
    detailEl.innerHTML = '<div class="detail-panel"><div class="loading-overlay"><div class="spinner"></div>Loading...</div></div>';

    const [data, wellsRes] = await Promise.all([
      apiGet(`/platforms/${id}`),
      apiGet(`/platforms/${id}/wells`),
    ]);

    const flags = [
      ['Oil', data.oil_producing], ['Gas', data.gas_producing], ['Water', data.water_producing],
      ['Condensate', data.condensate_producing], ['Drilling', data.drilling], ['Manned 24hr', data.manned_24hr],
      ['Heliport', data.heliport], ['Compressor', data.compressor], ['Workover', data.workover],
      ['Sulfur', data.sulfur_producing], ['Power Gen', data.power_gen], ['Prod Equip', data.prod_equipment],
    ];

    const loc = data.locations?.[0];

    detailEl.innerHTML = `
      <div class="detail-panel">
        <div class="detail-header">
          <div><h3>${escapeHtml(data.structures?.[0]?.structure_name || `Complex ${data.complex_id}`)}</h3>
            <div class="detail-subtitle">${data.area_code} ${data.block_number} · ${escapeHtml(data.operator_name || '')}</div></div>
          <button class="detail-close" id="plat-close">×</button>
        </div>
        <div class="detail-body">
          ${loc ? '<div id="plat-mini-map"></div>' : ''}
          <div class="kv-section">
            <div class="kv-section-title">Platform Info</div>
            ${kv('Complex ID', data.complex_id)}
            ${kv('Lease', data.lease_number ? `<a class="link-value" href="#/leases/${data.lease_number}">${data.lease_number}</a>` : '—')}
            ${kv('Operator', data.operator_name ? `<a class="link-value" href="#/companies/${data.company_num}">${escapeHtml(data.operator_name)}</a>` : '—')}
            ${kv('Water Depth', formatDepth(data.water_depth))}
            ${kv('Rigs', data.rig_count)}
            ${kv('Cranes', data.crane_count)}
            ${kv('Beds', data.bed_count)}
          </div>
          <div class="kv-section">
            <div class="kv-section-title">Status Flags</div>
            <div class="flag-grid">${flags.map(([name, val]) =>
              `<div class="flag-item">${flagDot(val)} ${name}</div>`
            ).join('')}</div>
          </div>
          ${data.structures?.length ? `
            <div class="kv-section">
              <div class="kv-section-title">Structures (${data.structures.length})</div>
              <table class="detail-subtable">
                <thead><tr><th>Name</th><th>Type</th><th>Installed</th><th>Slots</th></tr></thead>
                <tbody>${data.structures.map(s => `<tr>
                  <td>${escapeHtml(s.structure_name || '—')}</td>
                  <td>${s.structure_type || '—'}</td>
                  <td>${formatDate(s.install_date)}</td>
                  <td>${s.slot_drill_count || 0}/${s.slot_count || 0}</td>
                </tr>`).join('')}</tbody>
              </table>
            </div>
          ` : ''}
          ${wellsRes.data.length > 0 ? `
            <div class="kv-section">
              <div class="kv-section-title">Wells (${wellsRes.data.length})</div>
              <table class="detail-subtable">
                <thead><tr><th>API Number</th><th>Name</th><th>Status</th><th>Type</th></tr></thead>
                <tbody>${wellsRes.data.slice(0, 20).map(w => `<tr>
                  <td><a class="link-value" href="#/wells/${encodeURIComponent(w.api_well_number)}">${w.api_well_number}</a></td>
                  <td>${escapeHtml(w.well_name || '—')}</td>
                  <td>${wellStatusBadge(w.status_code)}</td>
                  <td>${w.type_code || '—'}</td>
                </tr>`).join('')}</tbody>
              </table>
              ${wellsRes.data.length > 20 ? `<p style="padding:6px 0;font-size:11px;color:var(--color-text-muted)">Showing 20 of ${wellsRes.data.length}</p>` : ''}
            </div>
          ` : ''}
        </div>
      </div>
    `;

    document.getElementById('plat-close').addEventListener('click', () => {
      detailEl.innerHTML = '<div class="detail-panel"><div class="detail-panel-empty">Select a platform to view details</div></div>';
    });

    if (loc && loc.latitude && loc.longitude) {
      if (detailMap) detailMap.destroy();
      detailMap = new MiniMap(document.getElementById('plat-mini-map'), {
        lat: loc.latitude, lng: loc.longitude, zoom: 11,
      });
    }
  }

  function kv(key, val) {
    return `<div class="kv-row"><span class="kv-key">${key}</span><span class="kv-value">${val ?? '—'}</span></div>`;
  }

  const resizer = initSplitResizer(container.querySelector('.split-layout'));

  if (params.id) showPlatformDetail(params.id);
  table.load();

  return () => { table.destroy(); if (resizer) resizer.destroy(); if (detailMap) detailMap.destroy(); };
}
