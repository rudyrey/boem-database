import { apiGet, getStats } from '../core/api.js';
import { formatNumber, formatDate, escapeHtml, formatDepth } from '../core/utils.js';
import { DataTable } from '../components/data-table.js';
import { MiniMap } from '../components/mini-map.js';
import { debounce } from '../core/utils.js';
import { initSplitResizer } from '../components/split-resizer.js';

export async function initPipelinesView(container, params = {}) {
  const stats = await getStats();

  container.innerHTML = `
    <div class="view-header">
      <h2>Pipeline Explorer <span class="subtitle">${formatNumber(stats.counts.pipelines)} segments</span></h2>
    </div>
    <div class="filter-bar" id="pipe-filters"></div>
    <div class="split-layout">
      <div class="split-main" id="pipe-table"></div>
      <div class="split-detail" id="pipe-detail">
        <div class="detail-panel"><div class="detail-panel-empty">Select a pipeline to view details</div></div>
      </div>
    </div>
  `;

  const opts = stats.filterOptions;
  document.getElementById('pipe-filters').innerHTML = `
    <input type="text" id="ppf-search" placeholder="Search pipelines...">
    <select id="ppf-status"><option value="">All Status</option>
      ${opts.pipelineStatus.map(s => `<option value="${s}">${s}</option>`).join('')}</select>
    <select id="ppf-product"><option value="">All Products</option>
      ${opts.pipelineProduct.map(p => `<option value="${p}">${p}</option>`).join('')}</select>
    <button class="btn-clear" id="ppf-clear">Clear</button>
  `;

  const statusBadge = (v) => {
    if (!v) return '—';
    const cls = v === 'ACT' ? 'badge-success' : v === 'ABN' ? 'badge-muted' : 'badge-warning';
    return `<span class="badge ${cls}">${v}</span>`;
  };

  const columns = [
    { key: 'segment_num', label: 'Segment', width: '80px', className: 'cell-mono' },
    { key: 'origin_name', label: 'Origin' },
    { key: 'dest_name', label: 'Destination' },
    { key: 'product_code', label: 'Product', width: '70px' },
    { key: 'status_code', label: 'Status', width: '70px', format: statusBadge },
    { key: 'pipe_size', label: 'Size', width: '60px' },
    { key: 'segment_length', label: 'Length', width: '80px', className: 'cell-number', format: v => v ? formatNumber(v) + ' ft' : '—' },
    { key: 'operator_name', label: 'Operator' },
  ];

  const table = new DataTable({
    container: document.getElementById('pipe-table'),
    columns,
    fetchFn: (page, limit, sort, order, filters) => apiGet('/pipelines', { page, limit, sort, order, ...filters }),
    onRowClick: row => showDetail(row.segment_num),
  });

  const applyFilters = debounce(() => {
    table.setFilters({
      search: document.getElementById('ppf-search').value || undefined,
      status_code: document.getElementById('ppf-status').value || undefined,
      product_code: document.getElementById('ppf-product').value || undefined,
    });
  }, 300);

  document.getElementById('pipe-filters').querySelectorAll('input, select').forEach(el => el.addEventListener('input', applyFilters));
  document.getElementById('ppf-clear').addEventListener('click', () => {
    document.getElementById('pipe-filters').querySelectorAll('input, select').forEach(el => el.value = '');
    applyFilters();
  });

  let detailMap = null;

  async function showDetail(id) {
    const detailEl = document.getElementById('pipe-detail');
    detailEl.innerHTML = '<div class="detail-panel"><div class="loading-overlay"><div class="spinner"></div>Loading...</div></div>';

    const [pipe, geo] = await Promise.all([
      apiGet(`/pipelines/${id}`),
      apiGet(`/pipelines/${id}/geometry`),
    ]);

    detailEl.innerHTML = `
      <div class="detail-panel">
        <div class="detail-header">
          <div><h3>Segment ${escapeHtml(pipe.segment_num)}</h3>
            <div class="detail-subtitle">${escapeHtml(pipe.origin_name || '')} → ${escapeHtml(pipe.dest_name || '')}</div></div>
          <button class="detail-close" id="pipe-close">×</button>
        </div>
        <div class="detail-body">
          <div id="pipe-mini-map"></div>
          <div class="kv-section">
            <div class="kv-section-title">Pipeline Info</div>
            ${kv('Status', statusBadge(pipe.status_code))}
            ${kv('Product', pipe.product_code)}
            ${kv('Pipe Size', pipe.pipe_size ? pipe.pipe_size + '"' : '—')}
            ${kv('Length', pipe.segment_length ? formatNumber(pipe.segment_length) + ' ft' : '—')}
            ${kv('Operator', pipe.operator_name ? `<a class="link-value" href="#/companies/${pipe.facility_operator}">${escapeHtml(pipe.operator_name)}</a>` : '—')}
            ${kv('Approved', formatDate(pipe.approved_date))}
            ${kv('Built', formatDate(pipe.construction_date))}
            ${kv('Water Depth', `${formatDepth(pipe.min_water_depth)} – ${formatDepth(pipe.max_water_depth)}`)}
            ${kv('MAOP', pipe.maop_pressure ? formatNumber(pipe.maop_pressure) + ' PSI' : '—')}
            ${kv('Buried', pipe.buried_flag)}
          </div>
          <div class="kv-section">
            <div class="kv-section-title">Route</div>
            ${kv('Origin', `${pipe.origin_name || '—'} (${pipe.origin_area} ${pipe.origin_block})`)}
            ${kv('Destination', `${pipe.dest_name || '—'} (${pipe.dest_area} ${pipe.dest_block})`)}
            ${kv('Geometry Points', geo.data?.length || 0)}
          </div>
        </div>
      </div>
    `;

    document.getElementById('pipe-close').addEventListener('click', () => {
      detailEl.innerHTML = '<div class="detail-panel"><div class="detail-panel-empty">Select a pipeline to view details</div></div>';
    });

    if (geo.data?.length > 0) {
      if (detailMap) detailMap.destroy();
      detailMap = new MiniMap(document.getElementById('pipe-mini-map'));
      detailMap.addPolyline(geo.data);
    }
  }

  function kv(key, val) {
    return `<div class="kv-row"><span class="kv-key">${key}</span><span class="kv-value">${val ?? '—'}</span></div>`;
  }

  const resizer = initSplitResizer(container.querySelector('.split-layout'));

  if (params.id) showDetail(params.id);
  table.load();

  return () => { table.destroy(); if (resizer) resizer.destroy(); if (detailMap) detailMap.destroy(); };
}
