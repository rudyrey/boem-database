import { apiGet, getStats } from '../core/api.js';
import { formatNumber, formatCompact, debounce } from '../core/utils.js';
import { ChartPanel } from '../components/chart-panel.js';

export async function initProductionView(container) {
  const stats = await getStats();
  const years = Array.from({ length: 30 }, (_, i) => 2025 - i);

  container.innerHTML = `
    <div class="view-header"><h2>Production Analytics</h2></div>
    <div class="analytics-controls">
      <div class="control-group">
        <label>View By</label>
        <select id="prod-view-by">
          <option value="all">All GOM (Annual)</option>
          <option value="field">By Field</option>
          <option value="lease">By Lease</option>
          <option value="operator">By Operator</option>
        </select>
      </div>
      <div class="control-group" id="prod-entity-group" style="display:none">
        <label id="prod-entity-label">Entity</label>
        <div class="autocomplete-wrap">
          <input type="text" id="prod-entity" placeholder="Type to search..." autocomplete="off" style="min-width:240px">
          <div class="autocomplete-list" id="prod-entity-list"></div>
        </div>
      </div>
      <div class="control-group">
        <label>Top Producers</label>
        <select id="prod-top-metric">
          <option value="oil">By Oil</option>
          <option value="gas">By Gas</option>
        </select>
      </div>
      <div class="control-group">
        <label>Year From</label>
        <select id="prod-year-from">
          <option value="">Any</option>
          ${years.map(y => `<option value="${y}">${y}</option>`).join('')}
        </select>
      </div>
      <div class="control-group">
        <label>Year To</label>
        <select id="prod-year-to">
          <option value="">Any</option>
          ${years.map(y => `<option value="${y}">${y}</option>`).join('')}
        </select>
      </div>
      <div class="control-group">
        <label>Area</label>
        <input type="text" id="prod-area" placeholder="e.g. GC, MC" style="width:100px">
      </div>
      <div class="control-group" id="prod-op-filter-group">
        <label>Operator Filter</label>
        <div class="autocomplete-wrap">
          <input type="text" id="prod-op-filter" placeholder="Search operator..." autocomplete="off" style="min-width:180px">
          <div class="autocomplete-list" id="prod-op-filter-list"></div>
        </div>
      </div>
    </div>
    <div class="chart-grid">
      <div class="chart-container chart-full" style="grid-column:1/-1">
        <h3 id="prod-chart-title">Annual Production — Gulf of Mexico</h3>
        <div id="prod-main-chart" style="height:400px"></div>
      </div>
    </div>
    <div class="chart-grid">
      <div class="chart-container">
        <h3 id="top-chart-title">Top 20 Leases by Oil</h3>
        <div id="prod-top-chart" style="height:400px"></div>
      </div>
      <div class="chart-container">
        <h3>Annual Summary Stats</h3>
        <div id="prod-summary-chart" style="height:400px"></div>
      </div>
    </div>
  `;

  const mainChart = new ChartPanel(document.getElementById('prod-main-chart'));
  const topChart = new ChartPanel(document.getElementById('prod-top-chart'));
  const summaryChart = new ChartPanel(document.getElementById('prod-summary-chart'));

  // State for selected entity
  let selectedEntity = { code: null, label: null };
  let selectedOpFilter = { code: null, label: null };

  // ——— Autocomplete helper ———
  function setupAutocomplete(inputId, listId, searchType, onSelect) {
    const input = document.getElementById(inputId);
    const list = document.getElementById(listId);

    const search = debounce(async () => {
      const q = input.value.trim();
      if (q.length < 2) { list.innerHTML = ''; list.style.display = 'none'; return; }

      try {
        const res = await apiGet('/production/search', { q, type: searchType });
        if (res.data.length === 0) {
          list.innerHTML = '<div class="autocomplete-item autocomplete-empty">No results</div>';
        } else {
          list.innerHTML = res.data.map(d =>
            `<div class="autocomplete-item" data-code="${d.code}" data-label="${d.label.replace(/"/g, '&quot;')}">${highlight(d.label, q)}</div>`
          ).join('');
        }
        list.style.display = 'block';
      } catch (e) { console.error(e); }
    }, 300);

    input.addEventListener('input', search);

    list.addEventListener('click', (e) => {
      const item = e.target.closest('.autocomplete-item');
      if (!item || item.classList.contains('autocomplete-empty')) return;
      const code = item.dataset.code;
      const label = item.dataset.label;
      input.value = label;
      list.style.display = 'none';
      onSelect({ code, label });
    });

    // Close on outside click
    document.addEventListener('click', (e) => {
      if (!input.contains(e.target) && !list.contains(e.target)) {
        list.style.display = 'none';
      }
    });

    return { input, list, clear: () => { input.value = ''; list.style.display = 'none'; } };
  }

  function highlight(text, q) {
    const idx = text.toLowerCase().indexOf(q.toLowerCase());
    if (idx === -1) return text;
    return text.slice(0, idx) + '<strong>' + text.slice(idx, idx + q.length) + '</strong>' + text.slice(idx + q.length);
  }

  // ——— Entity autocomplete ———
  let entitySearchType = 'field';
  const entityAC = setupAutocomplete('prod-entity', 'prod-entity-list', entitySearchType, (sel) => {
    selectedEntity = sel;
    loadEntityChart();
  });

  // ——— Operator filter autocomplete ———
  const opFilterAC = setupAutocomplete('prod-op-filter', 'prod-op-filter-list', 'operator', (sel) => {
    selectedOpFilter = sel;
    loadTopProducers();
  });

  // Initial load
  loadAnnualChart(stats.annualSummary);
  loadTopProducers();
  loadSummaryChart(stats.annualSummary);

  // ——— View-by change ———
  const viewByEl = document.getElementById('prod-view-by');
  viewByEl.addEventListener('change', () => {
    const val = viewByEl.value;
    const entityGroup = document.getElementById('prod-entity-group');
    const entityLabel = document.getElementById('prod-entity-label');
    const entityInput = document.getElementById('prod-entity');

    if (val === 'all') {
      entityGroup.style.display = 'none';
      selectedEntity = { code: null, label: null };
      loadAnnualChart(stats.annualSummary);
    } else {
      entityGroup.style.display = '';
      entityInput.value = '';
      selectedEntity = { code: null, label: null };

      const labels = { field: 'Field', lease: 'Lease', operator: 'Operator' };
      const placeholders = { field: 'Type field name...', lease: 'Type lease number...', operator: 'Type company name...' };
      entityLabel.textContent = labels[val] || 'Entity';
      entityInput.placeholder = placeholders[val] || 'Type to search...';

      // Update autocomplete search type by recreating the handler
      entitySearchType = val;
      const list = document.getElementById('prod-entity-list');
      entityInput.oninput = debounce(async () => {
        const q = entityInput.value.trim();
        if (q.length < 2) { list.innerHTML = ''; list.style.display = 'none'; return; }
        try {
          const res = await apiGet('/production/search', { q, type: entitySearchType });
          if (res.data.length === 0) {
            list.innerHTML = '<div class="autocomplete-item autocomplete-empty">No results</div>';
          } else {
            list.innerHTML = res.data.map(d =>
              `<div class="autocomplete-item" data-code="${d.code}" data-label="${d.label.replace(/"/g, '&quot;')}">${highlight(d.label, q)}</div>`
            ).join('');
          }
          list.style.display = 'block';
        } catch (e) { console.error(e); }
      }, 300);
    }
  });

  // ——— Filter changes trigger top-producers reload ———
  document.getElementById('prod-top-metric').addEventListener('change', loadTopProducers);
  document.getElementById('prod-year-from').addEventListener('change', () => { loadTopProducers(); reloadEntityIfNeeded(); });
  document.getElementById('prod-year-to').addEventListener('change', () => { loadTopProducers(); reloadEntityIfNeeded(); });
  document.getElementById('prod-area').addEventListener('input', debounce(() => { loadTopProducers(); reloadEntityIfNeeded(); }, 400));
  document.getElementById('prod-op-filter').addEventListener('keydown', (e) => {
    if (e.key === 'Backspace' && document.getElementById('prod-op-filter').value === '') {
      selectedOpFilter = { code: null, label: null };
      loadTopProducers();
    }
  });

  function reloadEntityIfNeeded() {
    if (selectedEntity.code && document.getElementById('prod-view-by').value !== 'all') {
      loadEntityChart();
    }
  }

  // ——— Gather current filter params ———
  function getFilterParams() {
    const params = {};
    const yearFrom = document.getElementById('prod-year-from').value;
    const yearTo = document.getElementById('prod-year-to').value;
    const area = document.getElementById('prod-area').value.trim();

    if (yearFrom) params.date_from = yearFrom + '01';
    if (yearTo) params.date_to = yearTo + '12';
    if (area) params.area_block = area;
    if (selectedOpFilter.code) params.operator_num = selectedOpFilter.code;

    return params;
  }

  // ——— Entity chart load ———
  async function loadEntityChart() {
    const viewBy = document.getElementById('prod-view-by').value;
    const code = selectedEntity.code;
    const label = selectedEntity.label;
    if (!code) return;

    const filterParams = getFilterParams();

    const endpoints = {
      field: '/production/by-field',
      lease: '/production/by-lease',
      operator: '/production/by-operator',
    };

    const queryKeys = {
      field: 'field_name_code',
      lease: 'lease_number',
      operator: 'operator_num',
    };

    try {
      const params = { [queryKeys[viewBy]]: code, ...filterParams };
      const res = await apiGet(endpoints[viewBy], params);
      const data = res.data;
      if (data.length === 0) {
        document.getElementById('prod-chart-title').textContent = `No production data found for ${label}`;
        return;
      }

      document.getElementById('prod-chart-title').textContent = `Production — ${label}`;
      mainChart.update({
        data: {
          labels: data.map(d => d.production_date),
          datasets: [
            { label: 'Oil (BBL)', data: data.map(d => d.oil), borderColor: '#2c3e50', backgroundColor: 'rgba(44,62,80,0.1)', fill: true, tension: 0.3, pointRadius: 0, borderWidth: 2, yAxisID: 'y' },
            { label: 'Gas (MCF)', data: data.map(d => d.gas), borderColor: '#e74c3c', backgroundColor: 'rgba(231,76,60,0.05)', fill: false, tension: 0.3, pointRadius: 0, borderWidth: 2, yAxisID: 'y1' },
            { label: 'Water (BBL)', data: data.map(d => d.water), borderColor: '#3498db', backgroundColor: 'rgba(52,152,219,0.05)', fill: true, tension: 0.3, pointRadius: 0, borderWidth: 2, yAxisID: 'y' },
          ],
        },
        scales: {
          x: { grid: { display: false }, ticks: { font: { size: 10 }, maxTicksLimit: 20 } },
          y: { type: 'linear', position: 'left', title: { display: true, text: 'BBL', font: { size: 10 } }, grid: { color: 'rgba(0,0,0,0.05)' }, ticks: { font: { size: 10 }, callback: v => formatCompact(v) } },
          y1: { type: 'linear', position: 'right', title: { display: true, text: 'MCF', font: { size: 10 }, color: '#e74c3c' }, grid: { drawOnChartArea: false }, ticks: { font: { size: 10 }, color: '#e74c3c', callback: v => formatCompact(v) } },
        },
      });
    } catch (e) { console.error(e); }
  }

  // ——— Top producers ———
  async function loadTopProducers() {
    const metric = document.getElementById('prod-top-metric').value;
    const yearFrom = document.getElementById('prod-year-from').value;
    const yearTo = document.getElementById('prod-year-to').value;
    const area = document.getElementById('prod-area').value.trim();

    const params = { by: metric, limit: 20 };
    if (yearFrom) params.year_from = yearFrom;
    if (yearTo) params.year_to = yearTo;
    if (area) params.area_block = area;
    if (selectedOpFilter.code) params.operator_num = selectedOpFilter.code;

    const res = await apiGet('/production/top-producers', params);

    const yearLabel = yearFrom && yearTo ? `${yearFrom}–${yearTo}`
      : yearFrom ? `${yearFrom}+` : yearTo ? `–${yearTo}` : 'All Time';
    const opLabel = selectedOpFilter.label ? ` · ${selectedOpFilter.label}` : '';
    const areaLabel = area ? ` · ${area}` : '';

    document.getElementById('top-chart-title').textContent =
      `Top 20 Leases by ${metric === 'oil' ? 'Oil' : 'Gas'} (${yearLabel}${areaLabel}${opLabel})`;

    const col = metric === 'oil' ? 'total_oil' : 'total_gas';
    topChart.update({
      type: 'bar',
      data: {
        labels: res.data.map(d => d.lease_number),
        datasets: [{
          label: metric === 'oil' ? 'Oil (BBL)' : 'Gas (MCF)',
          data: res.data.map(d => d[col]),
          backgroundColor: metric === 'oil' ? '#2c3e50' : '#e74c3c',
          borderRadius: 3,
        }],
      },
      options: { indexAxis: 'y' },
      scales: {
        x: { grid: { color: 'rgba(0,0,0,0.05)' }, ticks: { font: { size: 10 }, callback: v => formatCompact(v) } },
        y: { grid: { display: false }, ticks: { font: { size: 9, family: 'monospace' } } },
      },
    });
  }

  function loadAnnualChart(data) {
    document.getElementById('prod-chart-title').textContent = 'Annual Production — Gulf of Mexico';
    mainChart.update({
      type: 'line',
      data: {
        labels: data.map(d => d.year),
        datasets: [
          { label: 'Oil (BBL)', data: data.map(d => d.total_oil), borderColor: '#2c3e50', backgroundColor: 'rgba(44,62,80,0.1)', fill: true, tension: 0.3, pointRadius: 2, borderWidth: 2, yAxisID: 'y' },
          { label: 'Gas (MCF)', data: data.map(d => d.total_gas), borderColor: '#e74c3c', backgroundColor: 'rgba(231,76,60,0.05)', fill: false, tension: 0.3, pointRadius: 2, borderWidth: 2, yAxisID: 'y1' },
          { label: 'Water (BBL)', data: data.map(d => d.total_water), borderColor: '#3498db', backgroundColor: 'rgba(52,152,219,0.05)', fill: true, tension: 0.3, pointRadius: 2, borderWidth: 2, yAxisID: 'y' },
        ],
      },
      scales: {
        x: { grid: { display: false }, ticks: { font: { size: 10 } } },
        y: { type: 'linear', position: 'left', title: { display: true, text: 'BBL', font: { size: 10 } }, grid: { color: 'rgba(0,0,0,0.05)' }, ticks: { font: { size: 10 }, callback: v => formatCompact(v) } },
        y1: { type: 'linear', position: 'right', title: { display: true, text: 'MCF', font: { size: 10 }, color: '#e74c3c' }, grid: { drawOnChartArea: false }, ticks: { font: { size: 10 }, color: '#e74c3c', callback: v => formatCompact(v) } },
      },
    });
  }

  function loadSummaryChart(data) {
    summaryChart.update({
      type: 'bar',
      data: {
        labels: data.map(d => d.year),
        datasets: [
          { label: 'Active Wells', data: data.map(d => d.well_count), backgroundColor: '#3498db', borderRadius: 2, yAxisID: 'y' },
          { label: 'Active Leases', data: data.map(d => d.lease_count), backgroundColor: '#27ae60', borderRadius: 2, yAxisID: 'y' },
        ],
      },
      scales: {
        x: { grid: { display: false }, ticks: { font: { size: 10 } } },
        y: { grid: { color: 'rgba(0,0,0,0.05)' }, ticks: { font: { size: 10 } } },
      },
    });
  }

  return () => { mainChart.destroy(); topChart.destroy(); summaryChart.destroy(); };
}
