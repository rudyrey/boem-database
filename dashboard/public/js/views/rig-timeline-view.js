import { apiGet } from '../core/api.js';
import { formatNumber, formatDate, formatDepth, escapeHtml, latestGuard } from '../core/utils.js';
import { debounce } from '../core/utils.js';

const ACTIVITY_LABELS = {
  COM: 'Completion', DRL: 'Drilling', PA: 'Plugged & Abandoned',
  SI: 'Shut-In', TA: 'Temporarily Abandoned', WO: 'Workover',
  DSI: 'Drilling Shut-In', ST: 'Sidetrack',
};

const ACTIVITY_COLORS = {
  DRL: '#3498db', COM: '#27ae60', WO: '#e67e22', PA: '#c0392b',
  TA: '#f39c12', ST: '#2980b9', SI: '#95a5a6', DSI: '#7f8c8d',
};

const OPERATOR_PALETTE = [
  '#e74c3c', '#3498db', '#2ecc71', '#f39c12', '#9b59b6',
  '#1abc9c', '#e67e22', '#34495e', '#e91e63', '#00bcd4',
  '#8bc34a', '#ff9800', '#673ab7', '#009688', '#ff5722',
  '#607d8b', '#795548', '#cddc39', '#03a9f4', '#4caf50',
];

function durationDays(start, end) {
  if (!start || !end) return null;
  const d1 = new Date(start), d2 = new Date(end);
  if (isNaN(d1) || isNaN(d2)) return null;
  return Math.max(0, Math.round((d2 - d1) / 86400000));
}

function formatDuration(days) {
  if (days == null) return '—';
  if (days < 365) return `${days}d`;
  const y = Math.floor(days / 365);
  const d = days % 365;
  return d > 0 ? `${y}y ${d}d` : `${y}y`;
}

function dateToMs(s) {
  const d = new Date(s);
  return isNaN(d) ? null : d.getTime();
}

export async function initRigTimelineView(container, params = {}) {

  // State
  let ganttData = null;
  let colorBy = 'activity';
  const operatorColorMap = {};
  let opColorIdx = 0;

  // Viewport state for zoom/pan (milliseconds)
  let viewMinMs = null, viewMaxMs = null;
  let dataMinMs = null, dataMaxMs = null;
  let _cleanupViewport = null;

  // Default date range: last 3 years
  const now = new Date();
  const defaultFrom = new Date(now.getFullYear() - 3, now.getMonth(), now.getDate())
    .toISOString().slice(0, 10);
  const defaultTo = now.toISOString().slice(0, 10);

  container.innerHTML = `
    <div class="gantt-page">
      <div class="view-header">
        <h2>Rig Activity Timeline</h2>
      </div>
      <div class="filter-bar" id="rt-filters">
        <input type="text" id="rt-search" placeholder="Search rig name...">
        <label class="filter-checkbox">
          <input type="checkbox" id="rt-hide-generic" checked>
          <span>Hide generic (*)</span>
        </label>
        <div class="filter-group">
          <span class="filter-group-label">Date Range</span>
          <input type="date" id="rt-date-from" value="${defaultFrom}" title="From">
          <input type="date" id="rt-date-to" value="${defaultTo}" title="To">
        </div>
        <select id="rt-color-by">
          <option value="activity">Color by Activity</option>
          <option value="operator">Color by Operator</option>
        </select>
        <button class="btn-clear" id="rt-clear">Clear</button>
      </div>
      <div class="gantt-legend" id="rt-legend"></div>
      <div class="gantt-panel" id="gantt-panel">
        <div class="loading-overlay"><div class="spinner"></div>Loading...</div>
      </div>
      <div class="gantt-detail" id="rt-detail">
        <div class="gantt-detail-empty">Click an activity bar to view details</div>
      </div>
    </div>
  `;

  // ---- Filters ----
  const applyFilters = debounce(() => loadGantt(), 400);
  const filterEl = document.getElementById('rt-filters');
  filterEl.querySelectorAll('input, select').forEach(el =>
    el.addEventListener(el.type === 'checkbox' ? 'change' : 'input', applyFilters)
  );

  document.getElementById('rt-color-by').addEventListener('change', () => {
    colorBy = document.getElementById('rt-color-by').value;
    if (ganttData) renderGantt();
  });

  document.getElementById('rt-clear').addEventListener('click', () => {
    document.getElementById('rt-search').value = '';
    document.getElementById('rt-hide-generic').checked = true;
    document.getElementById('rt-date-from').value = defaultFrom;
    document.getElementById('rt-date-to').value = defaultTo;
    applyFilters();
  });

  // ---- Load data ----
  const ganttGuard = latestGuard();
  async function loadGantt() {
    const isCurrent = ganttGuard();
    const panel = document.getElementById('gantt-panel');
    panel.innerHTML = '<div class="loading-overlay"><div class="spinner"></div>Loading...</div>';

    try {
      const data = await apiGet('/rig-timeline/gantt', {
        search: document.getElementById('rt-search').value || undefined,
        hide_generic: document.getElementById('rt-hide-generic').checked ? 'true' : undefined,
        date_from: document.getElementById('rt-date-from').value || undefined,
        date_to: document.getElementById('rt-date-to').value || undefined,
        rig_limit: 5000,
      });
      if (!isCurrent()) return; // filters changed while loading
      ganttData = data;
      // Reset operator colors on reload
      Object.keys(operatorColorMap).forEach(k => delete operatorColorMap[k]);
      opColorIdx = 0;
      renderGantt();
    } catch (e) {
      if (!isCurrent()) return;
      panel.innerHTML = '<div class="loading-overlay">Failed to load data</div>';
    }
  }

  // ---- Color helpers ----
  function getOperatorColor(name) {
    if (!name) return '#95a5a6';
    if (!operatorColorMap[name]) {
      operatorColorMap[name] = OPERATOR_PALETTE[opColorIdx % OPERATOR_PALETTE.length];
      opColorIdx++;
    }
    return operatorColorMap[name];
  }

  function getBarColor(job) {
    if (colorBy === 'operator') return getOperatorColor(job.operator);
    const primary = job.activities[0] || '';
    return ACTIVITY_COLORS[primary] || '#95a5a6';
  }

  // ---- Date ticks ----
  function generateTicks(minMs, maxMs) {
    const rangeMs = maxMs - minMs;
    const rangeDays = rangeMs / 86400000;
    const ticks = [];
    const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    const startD = new Date(minMs);
    const endD = new Date(maxMs);

    if (rangeDays > 365 * 5) {
      // Yearly
      for (let y = startD.getFullYear(); y <= endD.getFullYear(); y++) {
        const d = new Date(y, 0, 1);
        const ms = d.getTime();
        if (ms >= minMs && ms <= maxMs) {
          ticks.push({ pct: (ms - minMs) / rangeMs * 100, label: String(y) });
        }
      }
    } else if (rangeDays > 365) {
      // Quarterly
      for (let y = startD.getFullYear(); y <= endD.getFullYear(); y++) {
        for (let q = 0; q < 4; q++) {
          const d = new Date(y, q * 3, 1);
          const ms = d.getTime();
          if (ms >= minMs && ms <= maxMs) {
            ticks.push({ pct: (ms - minMs) / rangeMs * 100, label: `Q${q+1}'${String(y).slice(2)}` });
          }
        }
      }
    } else {
      // Monthly
      for (let d = new Date(startD.getFullYear(), startD.getMonth(), 1);
           d <= endD;
           d = new Date(d.getFullYear(), d.getMonth() + 1, 1)) {
        const ms = d.getTime();
        if (ms >= minMs) {
          ticks.push({ pct: (ms - minMs) / rangeMs * 100, label: `${months[d.getMonth()]} ${d.getFullYear()}` });
        }
      }
    }
    // Filter out ticks that are too close together
    const minGap = rangeDays > 365 * 3 ? 12 : 10;
    const filtered = [];
    for (const t of ticks) {
      if (filtered.length === 0 || t.pct - filtered[filtered.length - 1].pct >= minGap) {
        filtered.push(t);
      }
    }
    return filtered;
  }

  // ---- Render Gantt ----
  function renderGantt() {
    const panel = document.getElementById('gantt-panel');
    const { rigs, dateRange } = ganttData;

    if (!rigs || rigs.length === 0) {
      panel.innerHTML = '<div class="loading-overlay">No rigs found matching filters</div>';
      renderLegend();
      return;
    }

    const minMs = dateToMs(dateRange.min);
    const maxMs = dateToMs(dateRange.max);
    if (!minMs || !maxMs || maxMs <= minMs) {
      panel.innerHTML = '<div class="loading-overlay">No data in date range</div>';
      return;
    }
    const rangeMs = maxMs - minMs;
    const ticks = generateTicks(minMs, maxMs);

    // Today marker
    const todayMs = Date.now();
    const todayPct = todayMs >= minMs && todayMs <= maxMs
      ? (todayMs - minMs) / rangeMs * 100 : null;

    panel.innerHTML = `
      <div class="gantt-scroll">
        <div class="gantt-header-row">
          <div class="gantt-label-col">Rig Name</div>
          <div class="gantt-timeline-header">
            ${ticks.map(t => `<div class="gantt-tick" style="left:${t.pct}%"><span>${t.label}</span></div>`).join('')}
            ${todayPct != null ? `<div class="gantt-today" style="left:${todayPct}%" title="Today"></div>` : ''}
          </div>
        </div>
        <div class="gantt-body">
          ${rigs.map((rig, ri) => `
            <div class="gantt-row${ri % 2 ? ' gantt-row-alt' : ''}" data-rig="${escapeHtml(rig.rig_name)}">
              <div class="gantt-label-col" title="${escapeHtml(rig.rig_name)}">${escapeHtml(rig.rig_name)}</div>
              <div class="gantt-timeline-cell">
                ${ticks.map(t => `<div class="gantt-gridline" style="left:${t.pct}%"></div>`).join('')}
                ${todayPct != null ? `<div class="gantt-today-line" style="left:${todayPct}%"></div>` : ''}
                ${rig.jobs.map((job, ji) => {
                  const sMs = dateToMs(job.start_dt);
                  const eMs = dateToMs(job.end_dt);
                  if (!sMs || !eMs) return '';
                  const left = Math.max(0, (sMs - minMs) / rangeMs * 100);
                  const width = Math.max(0.3, Math.min(100 - left, (eMs - sMs) / rangeMs * 100));
                  const wellLabel = job.well_name || job.api_well_number || '';
                  const days = durationDays(job.start_dt, job.end_dt);
                  const tip = `${wellLabel}\n${formatDate(job.start_dt)} – ${formatDate(job.end_dt)}` +
                    (days != null ? ` (${formatDuration(days)})` : '') +
                    `\n${job.activities.map(a => ACTIVITY_LABELS[a] || a).join(', ')}` +
                    (job.operator ? `\n${job.operator}` : '');
                  // Render segments within the bar
                  const jobDur = eMs - sMs;
                  let segmentsHtml = '';
                  if (colorBy === 'activity' && job.segments && job.segments.length > 1 && jobDur > 0) {
                    segmentsHtml = job.segments.map(seg => {
                      const segStart = Math.max(0, (dateToMs(seg.start) - sMs) / jobDur * 100);
                      const segEnd = Math.min(100, (dateToMs(seg.end) - sMs) / jobDur * 100);
                      const segWidth = Math.max(0, segEnd - segStart);
                      const segColor = ACTIVITY_COLORS[seg.activity] || '#95a5a6';
                      return `<div style="position:absolute;left:${segStart}%;width:${segWidth}%;top:0;bottom:0;background:${segColor}"></div>`;
                    }).join('');
                  }
                  const barBg = (colorBy === 'activity' && job.segments && job.segments.length > 1) ? 'transparent' : getBarColor(job);
                  return `<div class="gantt-bar" style="left:${left}%;width:${width}%;background:${barBg};overflow:hidden"
                    title="${escapeHtml(tip)}" data-ri="${ri}" data-ji="${ji}">${segmentsHtml}</div>`;
                }).join('')}
              </div>
            </div>
          `).join('')}
        </div>
        <div class="gantt-footer">
          Showing ${rigs.length} of ${ganttData.totalRigs} rigs
          ${ganttData.totalRigs > rigs.length + (ganttData.rigOffset || 0)
            ? `<button class="btn-clear" id="gantt-more">Show More</button>` : ''}
          <button class="btn-clear" id="gantt-reset-zoom" style="display:none">Reset Zoom</button>
          <span class="gantt-zoom-hint" style="font-style:italic">Ctrl+scroll to zoom, Ctrl+drag to pan</span>
        </div>
      </div>
    `;

    // Bar click handlers
    panel.querySelectorAll('.gantt-bar').forEach(bar => {
      bar.addEventListener('click', (e) => {
        e.stopPropagation();
        const ri = parseInt(bar.dataset.ri);
        const ji = parseInt(bar.dataset.ji);
        const rig = rigs[ri];
        if (rig) {
          panel.querySelectorAll('.gantt-bar').forEach(b => b.classList.remove('gantt-bar-selected'));
          bar.classList.add('gantt-bar-selected');
          showJobDetail(rig.jobs[ji], rig.rig_name);
        }
      });
    });

    // Show more
    const moreBtn = document.getElementById('gantt-more');
    if (moreBtn) {
      moreBtn.addEventListener('click', async () => {
        const isCurrent = ganttGuard();
        try {
          moreBtn.textContent = 'Loading...';
          moreBtn.disabled = true;
          const moreData = await apiGet('/rig-timeline/gantt', {
            search: document.getElementById('rt-search').value || undefined,
            hide_generic: document.getElementById('rt-hide-generic').checked ? 'true' : undefined,
            date_from: document.getElementById('rt-date-from').value || undefined,
            date_to: document.getElementById('rt-date-to').value || undefined,
            rig_limit: 40,
            rig_offset: ganttData.rigs.length + (ganttData.rigOffset || 0),
          });
          if (!isCurrent()) return; // filters changed while loading more
          ganttData.rigs.push(...moreData.rigs);
          if (moreData.dateRange.min && (!ganttData.dateRange.min || moreData.dateRange.min < ganttData.dateRange.min))
            ganttData.dateRange.min = moreData.dateRange.min;
          if (moreData.dateRange.max && (!ganttData.dateRange.max || moreData.dateRange.max > ganttData.dateRange.max))
            ganttData.dateRange.max = moreData.dateRange.max;
          renderGantt();
        } catch (e) {
          moreBtn.textContent = 'Error';
        }
      });
    }

    // Reset zoom button
    const resetBtn = document.getElementById('gantt-reset-zoom');
    if (resetBtn) {
      resetBtn.addEventListener('click', () => {
        viewMinMs = dataMinMs;
        viewMaxMs = dataMaxMs;
        updateViewport();
      });
    }

    renderLegend();
    initViewport(minMs, maxMs);
  }

  // ---- Zoom / Pan viewport ----
  function initViewport(minMs, maxMs) {
    // Cleanup previous listeners
    if (_cleanupViewport) { _cleanupViewport(); _cleanupViewport = null; }

    dataMinMs = minMs;
    dataMaxMs = maxMs;
    viewMinMs = minMs;
    viewMaxMs = maxMs;

    const scrollEl = document.querySelector('.gantt-scroll');
    if (!scrollEl) return;

    let isDragging = false;
    let dragStartX = 0;
    let dragStartViewMin = 0;
    let dragStartViewMax = 0;

    function onWheel(e) {
      if (!e.ctrlKey && !e.metaKey) return;
      e.preventDefault();

      const timelineCells = scrollEl.querySelectorAll('.gantt-timeline-cell');
      if (!timelineCells.length) return;

      // Use first visible timeline cell to get mouse position in timeline
      const rect = timelineCells[0].getBoundingClientRect();
      const mouseX = (e.clientX - rect.left) / rect.width; // 0..1 fraction
      const viewRange = viewMaxMs - viewMinMs;
      const mouseMs = viewMinMs + mouseX * viewRange;

      // Zoom factor
      const zoomFactor = e.deltaY > 0 ? 1.15 : 1 / 1.15;
      let newRange = viewRange * zoomFactor;

      // Clamp range: min 30 days, max 3x data range (allow panning beyond data)
      const minRange = 30 * 86400000;
      const maxRange = (dataMaxMs - dataMinMs) * 3;
      newRange = Math.max(minRange, Math.min(maxRange, newRange));

      // Scale around mouse position (no clamping to data bounds)
      viewMinMs = mouseMs - mouseX * newRange;
      viewMaxMs = mouseMs + (1 - mouseX) * newRange;

      updateViewport();
    }

    function onMouseDown(e) {
      if (!e.ctrlKey && !e.metaKey) return;
      // Only start drag on timeline area
      const cell = e.target.closest('.gantt-timeline-cell, .gantt-timeline-header');
      if (!cell) return;
      e.preventDefault();
      isDragging = true;
      dragStartX = e.clientX;
      dragStartViewMin = viewMinMs;
      dragStartViewMax = viewMaxMs;
      scrollEl.style.cursor = 'grabbing';
    }

    function onMouseMove(e) {
      if (!isDragging) return;
      e.preventDefault();
      const timelineCells = scrollEl.querySelectorAll('.gantt-timeline-cell');
      if (!timelineCells.length) return;
      const rect = timelineCells[0].getBoundingClientRect();
      const dx = e.clientX - dragStartX;
      const viewRange = dragStartViewMax - dragStartViewMin;
      const pxToMs = viewRange / rect.width;
      const shiftMs = -dx * pxToMs;

      viewMinMs = dragStartViewMin + shiftMs;
      viewMaxMs = dragStartViewMax + shiftMs;

      updateViewport();
    }

    function onMouseUp() {
      if (!isDragging) return;
      isDragging = false;
      scrollEl.style.cursor = '';
    }

    scrollEl.addEventListener('wheel', onWheel, { passive: false });
    scrollEl.addEventListener('mousedown', onMouseDown);
    window.addEventListener('mousemove', onMouseMove);
    window.addEventListener('mouseup', onMouseUp);

    _cleanupViewport = () => {
      scrollEl.removeEventListener('wheel', onWheel);
      scrollEl.removeEventListener('mousedown', onMouseDown);
      window.removeEventListener('mousemove', onMouseMove);
      window.removeEventListener('mouseup', onMouseUp);
    };
  }

  const _debouncedTickUpdate = debounce(() => updateTicks(), 60);

  function updateViewport() {
    const viewRange = viewMaxMs - viewMinMs;
    if (viewRange <= 0) return;

    // Reposition all bars
    document.querySelectorAll('.gantt-bar').forEach(bar => {
      const ri = parseInt(bar.dataset.ri);
      const ji = parseInt(bar.dataset.ji);
      const rig = ganttData.rigs[ri];
      if (!rig) return;
      const job = rig.jobs[ji];
      if (!job) return;

      const sMs = dateToMs(job.start_dt);
      const eMs = dateToMs(job.end_dt);
      if (!sMs || !eMs) return;

      const left = (sMs - viewMinMs) / viewRange * 100;
      const width = Math.max(0.3, (eMs - sMs) / viewRange * 100);

      // Hide bars completely out of view
      if (left + width < -5 || left > 105) {
        bar.style.display = 'none';
      } else {
        bar.style.display = '';
        bar.style.left = left + '%';
        bar.style.width = width + '%';
      }
    });

    // Show/hide reset zoom button
    const isZoomed = Math.abs(viewMinMs - dataMinMs) > 86400000 || Math.abs(viewMaxMs - dataMaxMs) > 86400000;
    const resetBtn = document.getElementById('gantt-reset-zoom');
    if (resetBtn) resetBtn.style.display = isZoomed ? '' : 'none';

    _debouncedTickUpdate();
  }

  function updateTicks() {
    const ticks = generateTicks(viewMinMs, viewMaxMs);
    const viewRange = viewMaxMs - viewMinMs;
    const todayMs = Date.now();
    const todayPct = todayMs >= viewMinMs && todayMs <= viewMaxMs
      ? (todayMs - viewMinMs) / viewRange * 100 : null;

    // Update header ticks
    const header = document.querySelector('.gantt-timeline-header');
    if (header) {
      header.innerHTML =
        ticks.map(t => `<div class="gantt-tick" style="left:${t.pct}%"><span>${t.label}</span></div>`).join('') +
        (todayPct != null ? `<div class="gantt-today" style="left:${todayPct}%" title="Today"></div>` : '');
    }

    // Update gridlines in each row
    document.querySelectorAll('.gantt-timeline-cell').forEach(cell => {
      // Remove old gridlines and today lines
      cell.querySelectorAll('.gantt-gridline, .gantt-today-line').forEach(el => el.remove());
      // Add new gridlines
      for (const t of ticks) {
        const gl = document.createElement('div');
        gl.className = 'gantt-gridline';
        gl.style.left = t.pct + '%';
        cell.insertBefore(gl, cell.firstChild);
      }
      if (todayPct != null) {
        const tl = document.createElement('div');
        tl.className = 'gantt-today-line';
        tl.style.left = todayPct + '%';
        cell.insertBefore(tl, cell.firstChild);
      }
    });
  }

  // ---- Legend ----
  function renderLegend() {
    const legend = document.getElementById('rt-legend');
    if (colorBy === 'activity') {
      legend.innerHTML = Object.entries(ACTIVITY_LABELS)
        .map(([code, label]) =>
          `<span class="legend-item"><span class="legend-dot" style="background:${ACTIVITY_COLORS[code] || '#95a5a6'}"></span>${label}</span>`)
        .join('');
    } else {
      // Show operators from current data
      const ops = new Set();
      if (ganttData?.rigs) {
        for (const rig of ganttData.rigs)
          for (const job of rig.jobs)
            if (job.operator) ops.add(job.operator);
      }
      legend.innerHTML = [...ops].slice(0, 20)
        .map(op =>
          `<span class="legend-item"><span class="legend-dot" style="background:${getOperatorColor(op)}"></span>${escapeHtml(op.length > 25 ? op.slice(0, 22) + '...' : op)}</span>`)
        .join('') + (ops.size > 20 ? `<span class="legend-item">+${ops.size - 20} more</span>` : '');
    }
  }

  // ---- Detail panel ----
  const detailGuard = latestGuard();
  async function showJobDetail(job, rigName) {
    const isCurrent = detailGuard();
    const detail = document.getElementById('rt-detail');
    const days = durationDays(job.start_dt, job.end_dt);

    detail.innerHTML = `
      <div class="gantt-detail-header">
        <div>
          <h3>${escapeHtml(job.well_name || job.api_well_number || 'Unknown')}</h3>
          <div class="detail-subtitle">${escapeHtml(rigName)} · ${formatDate(job.start_dt)} — ${formatDate(job.end_dt)}${days != null ? ` (${formatDuration(days)})` : ''}</div>
        </div>
        <button class="detail-close" id="rt-detail-close">&times;</button>
      </div>
      <div class="gantt-detail-body">
        <div class="kv-section">
          <div class="kv-section-title">Well Info</div>
          ${kv('API Number', job.api_well_number ? `<a class="link-value" href="#/wells/${encodeURIComponent(job.api_well_number)}">${job.api_well_number}</a>` : '—')}
          ${kv('Well Name', escapeHtml(job.well_name || '—'))}
          ${kv('Operator', job.operator ? `<a class="link-value" href="#/companies/${job.company_num}">${escapeHtml(job.operator)}</a>` : '—')}
          ${kv('Location', `${job.area_code || ''} ${job.block_num || ''}`.trim() || '—')}
        </div>
        <div class="kv-section">
          <div class="kv-section-title">Activity</div>
          ${kv('Activities', job.activities.map(a =>
            `<span class="badge" style="background:${(ACTIVITY_COLORS[a] || '#95a5a6')}22;color:${ACTIVITY_COLORS[a] || '#95a5a6'}">${ACTIVITY_LABELS[a] || a}</span>`
          ).join(' ') || '—')}
          ${kv('Duration', days != null ? formatDuration(days) : '—')}
          ${kv('WAR Reports', formatNumber(job.report_count))}
          ${kv('Max MD', formatDepth(job.max_md))}
          ${kv('Water Depth', formatDepth(job.water_depth))}
        </div>
        <div class="kv-section">
          <div class="kv-section-title">Related Data</div>
          <div class="gantt-detail-links">
            ${job.apd_count > 0 ? `<a class="cross-link cross-link-apd" href="#" data-scroll="rt-apd-records">APD (${job.apd_count})</a>` : ''}
            ${job.apm_count > 0 ? `<a class="cross-link cross-link-apm" href="#" data-scroll="rt-apm-records">APM (${job.apm_count})</a>` : ''}
            ${job.eor_count > 0 && job.eor_sns?.length ? `<a class="cross-link cross-link-eor" href="#/eor/${encodeURIComponent(job.eor_sns[0])}">EOR (${job.eor_count})</a>` : ''}
            ${job.lease_num ? `<a class="cross-link" href="#/leases/${encodeURIComponent(job.lease_num)}">Lease ${escapeHtml(job.lease_num)}</a>` : ''}
            ${!job.apd_count && !job.apm_count && !job.eor_count && !job.lease_num ? '<span style="color:var(--color-text-muted);font-size:var(--font-size-xs)">No related records</span>' : ''}
          </div>
        </div>
      </div>
      ${job.apd_count > 0 ? `<div id="rt-apd-records">
        <div class="kv-section-title" style="padding:var(--space-sm) var(--space-md) 0">Drilling Permits (APD)</div>
        <div style="padding:var(--space-sm) var(--space-md);color:var(--color-text-muted);font-size:var(--font-size-xs)">Loading...</div>
      </div>` : ''}
      ${job.apm_count > 0 ? `<div id="rt-apm-records">
        <div class="kv-section-title" style="padding:var(--space-sm) var(--space-md) 0">Permit Modifications (APM)</div>
        <div style="padding:var(--space-sm) var(--space-md);color:var(--color-text-muted);font-size:var(--font-size-xs)">Loading...</div>
      </div>` : ''}
      <div class="gantt-detail-wars" id="rt-war-records">
        <div class="kv-section-title" style="padding:var(--space-sm) var(--space-md) 0">Weekly Activity Reports</div>
        <div style="padding:var(--space-sm) var(--space-md);color:var(--color-text-muted);font-size:var(--font-size-xs)">Loading...</div>
      </div>
    `;

    detail.classList.add('expanded');

    document.getElementById('rt-detail-close').addEventListener('click', () => {
      detailGuard(); // invalidate any in-flight record fetches for this panel
      detail.innerHTML = '<div class="gantt-detail-empty">Click an activity bar to view details</div>';
      detail.classList.remove('expanded');
      document.querySelectorAll('.gantt-bar-selected').forEach(b => b.classList.remove('gantt-bar-selected'));
    });

    // APD/APM chips scroll to their record tables further down in this panel
    detail.querySelectorAll('[data-scroll]').forEach(chip => {
      chip.addEventListener('click', (e) => {
        e.preventDefault();
        document.getElementById(chip.dataset.scroll)?.scrollIntoView({ block: 'start' });
      });
    });

    // Fetch WAR, APD, APM records in parallel
    const api = job.api_well_number;
    const fetches = [
      apiGet('/rig-timeline/wars', {
        rig: rigName, api: api || undefined,
        start: job.start_dt || undefined, end: job.end_dt || undefined,
      }).catch(() => ({ data: [] })),
    ];
    if (job.apd_count > 0 && api) fetches.push(apiGet(`/wells/${encodeURIComponent(api)}/apds`).catch(() => ({ data: [] })));
    if (job.apm_count > 0 && api) fetches.push(apiGet(`/wells/${encodeURIComponent(api)}/apms`).catch(() => ({ data: [] })));

    const results = await Promise.all(fetches);
    if (!isCurrent()) return; // another bar was selected (or panel closed) meanwhile
    const wars = results[0].data || [];
    const apds = (job.apd_count > 0 && api) ? (results[1]?.data || []) : [];
    const apms = (job.apm_count > 0 && api) ? (results[job.apd_count > 0 ? 2 : 1]?.data || []) : [];

    // Render APD section
    const apdSection = document.getElementById('rt-apd-records');
    if (apdSection) {
      if (apds.length === 0) {
        apdSection.innerHTML = `
          <div class="kv-section-title" style="padding:var(--space-sm) var(--space-md) 0">Drilling Permits (APD)</div>
          <div style="padding:var(--space-sm) var(--space-md);color:var(--color-text-muted);font-size:var(--font-size-xs)">No records found</div>`;
      } else {
        apdSection.innerHTML = `
          <div class="kv-section-title" style="padding:var(--space-sm) var(--space-md) 0">Drilling Permits — APD (${apds.length})</div>
          <div style="overflow-x:auto;padding:0 var(--space-sm) var(--space-sm)">
            <table class="detail-subtable">
              <thead><tr>
                <th></th><th>Permit #</th><th>Type</th><th>Well Type</th><th>Status Date</th>
                <th>Req. Spud</th><th>Rig</th><th>Rig Type</th>
                <th>Location</th><th>Operator</th>
              </tr></thead>
              <tbody>
                ${apds.map((a, i) => `<tr class="clickable-row" data-apd-sn="${escapeHtml(a.sn_apd)}" data-idx="${i}">
                  <td class="expand-arrow">&#9654;</td>
                  <td><code>${a.sn_apd || '—'}</code></td>
                  <td>${escapeHtml(a.permit_type || '—')}</td>
                  <td>${escapeHtml(a.well_type_code || '—')}</td>
                  <td>${formatDate(a.apd_status_dt)}</td>
                  <td>${formatDate(a.req_spud_date)}</td>
                  <td>${escapeHtml(a.rig_name || '—')}</td>
                  <td>${escapeHtml(a.rig_type_code || '—')}</td>
                  <td>${[a.botm_area_code, a.botm_block_number].filter(Boolean).join(' ') || '—'}</td>
                  <td>${escapeHtml(a.bus_asc_name || '—')}</td>
                </tr>`).join('')}
              </tbody>
            </table>
          </div>`;
        // APD expand click handlers
        apdSection.querySelectorAll('tr[data-apd-sn]').forEach(row => {
          row.addEventListener('click', () => toggleApdDetail(row));
        });
      }
    }

    // Render APM section
    const apmSection = document.getElementById('rt-apm-records');
    if (apmSection) {
      if (apms.length === 0) {
        apmSection.innerHTML = `
          <div class="kv-section-title" style="padding:var(--space-sm) var(--space-md) 0">Permit Modifications (APM)</div>
          <div style="padding:var(--space-sm) var(--space-md);color:var(--color-text-muted);font-size:var(--font-size-xs)">No records found</div>`;
      } else {
        apmSection.innerHTML = `
          <div class="kv-section-title" style="padding:var(--space-sm) var(--space-md) 0">Permit Modifications — APM (${apms.length})</div>
          <div style="overflow-x:auto;padding:0 var(--space-sm) var(--space-sm)">
            <table class="detail-subtable">
              <thead><tr>
                <th></th><th>Permit #</th><th>Operation</th><th>Well Type</th><th>Status Date</th>
                <th>Work Starts</th><th>Est. Days</th><th>Borehole</th>
                <th>Location</th><th>Operator</th>
              </tr></thead>
              <tbody>
                ${apms.map((a, i) => `<tr class="clickable-row" data-apm-sn="${escapeHtml(a.sn_apm)}" data-idx="${i}">
                  <td class="expand-arrow">&#9654;</td>
                  <td><code>${a.sn_apm || '—'}</code></td>
                  <td>${escapeHtml(a.apm_op_cd || '—')}</td>
                  <td>${escapeHtml(a.well_type_code || '—')}</td>
                  <td>${formatDate(a.acc_status_date)}</td>
                  <td>${formatDate(a.work_commences_date)}</td>
                  <td>${a.est_operation_days != null ? a.est_operation_days + 'd' : '—'}</td>
                  <td>${escapeHtml(a.borehole_stat_cd || '—')}</td>
                  <td>${[a.botm_area_code, a.botm_block_num].filter(Boolean).join(' ') || '—'}</td>
                  <td>${escapeHtml(a.bus_asc_name || '—')}</td>
                </tr>`).join('')}
              </tbody>
            </table>
          </div>`;
        // APM expand click handlers
        apmSection.querySelectorAll('tr[data-apm-sn]').forEach(row => {
          row.addEventListener('click', () => toggleApmDetail(row));
        });
      }
    }

    // Render WAR section
    const warSection = document.getElementById('rt-war-records');
    if (!warSection) return;
    if (wars.length === 0) {
      warSection.innerHTML = `
        <div class="kv-section-title" style="padding:var(--space-sm) var(--space-md) 0">Weekly Activity Reports</div>
        <div style="padding:var(--space-sm) var(--space-md);color:var(--color-text-muted);font-size:var(--font-size-xs)">No records found</div>`;
    } else {
      warSection.innerHTML = `
        <div class="kv-section-title" style="padding:var(--space-sm) var(--space-md) 0">Weekly Activity Reports (${wars.length})</div>
        <div style="overflow-x:auto;padding:0 var(--space-sm) var(--space-sm)">
          <table class="detail-subtable">
            <thead><tr>
              <th>Report Period</th><th>Activity</th><th>MD (ft)</th><th>TVD (ft)</th>
              <th>Mud Wt</th><th>Water Depth</th><th>BOP Test</th><th>TD Date</th><th>Contact</th>
            </tr></thead>
            <tbody>
              ${wars.map(w => {
                const act = w.well_activity_cd;
                const actColor = ACTIVITY_COLORS[act] || '#95a5a6';
                return `<tr>
                  <td><span style="white-space:nowrap">${formatDate(w.war_start_dt)} — ${formatDate(w.war_end_dt)}</span></td>
                  <td><span class="badge" style="background:${actColor}22;color:${actColor}">${ACTIVITY_LABELS[act] || act || '—'}</span></td>
                  <td>${formatDepth(w.drilling_md)}</td>
                  <td>${formatDepth(w.drilling_tvd)}</td>
                  <td>${w.drill_fluid_wgt ? w.drill_fluid_wgt + ' ppg' : '—'}</td>
                  <td>${formatDepth(w.water_depth)}</td>
                  <td>${w.bop_test_date ? formatDate(w.bop_test_date) : '—'}</td>
                  <td>${w.total_depth_date ? formatDate(w.total_depth_date) : '—'}</td>
                  <td>${escapeHtml(w.contact_name || '—')}</td>
                </tr>`;
              }).join('')}
            </tbody>
          </table>
        </div>`;
    }
  }

  // ---- Expandable APD detail ----
  async function toggleApdDetail(row) {
    const sn = row.dataset.apdSn;
    const existing = row.nextElementSibling;
    if (existing && existing.classList.contains('expand-detail-row')) {
      existing.remove();
      row.querySelector('.expand-arrow').innerHTML = '&#9654;';
      return;
    }
    row.querySelector('.expand-arrow').innerHTML = '&#9660;';
    const cols = row.children.length;
    const detailRow = document.createElement('tr');
    detailRow.className = 'expand-detail-row';
    detailRow.innerHTML = `<td colspan="${cols}" class="expand-detail-cell"><div class="expand-loading">Loading...</div></td>`;
    row.after(detailRow);

    try {
      const [apd, casingRes, geoRes] = await Promise.all([
        apiGet(`/submissions/apd/${encodeURIComponent(sn)}`),
        apiGet(`/submissions/apd/${encodeURIComponent(sn)}/casing`).catch(() => ({ data: [] })),
        apiGet(`/submissions/apd/${encodeURIComponent(sn)}/geologic`).catch(() => ({ data: [] })),
      ]);
      const casing = casingRes.data || [];
      const geo = geoRes.data || [];
      const cell = detailRow.querySelector('.expand-detail-cell');

      cell.innerHTML = `
        <div class="expand-detail-content">
          <div class="expand-detail-grid">
            <div class="kv-section">
              <div class="kv-section-title">Permit Details</div>
              ${kv('Serial #', apd.sn_apd)}
              ${kv('Permit Type', apd.permit_type)}
              ${kv('Well Type', apd.well_type_code)}
              ${kv('Status Date', formatDate(apd.apd_status_dt))}
              ${kv('Submission Date', formatDate(apd.apd_sub_status_dt))}
              ${kv('Req. Spud Date', formatDate(apd.req_spud_date))}
            </div>
            <div class="kv-section">
              <div class="kv-section-title">Rig & Operator</div>
              ${kv('Rig', apd.rig_name || '—')}
              ${kv('Rig Type', apd.rig_type_code || '—')}
              ${kv('Rig ID', apd.rig_id_num || '—')}
              ${kv('Operator', apd.operator_name || apd.bus_asc_name || '—')}
              ${kv('Operator #', apd.operator_num || '—')}
              ${kv('Water Depth', formatDepth(apd.water_depth))}
            </div>
            <div class="kv-section">
              <div class="kv-section-title">Location</div>
              ${kv('Surface', [apd.surf_area_code, apd.surf_block_number].filter(Boolean).join(' ') || '—')}
              ${kv('Surface Lease', apd.surf_lease_number || '—')}
              ${kv('Bottom', [apd.botm_area_code, apd.botm_block_number].filter(Boolean).join(' ') || '—')}
              ${kv('Bottom Lease', apd.botm_lease_number || '—')}
            </div>
          </div>
          ${casing.length > 0 ? `
            <div class="kv-section">
              <div class="kv-section-title">Casing Program (${casing.length} intervals)</div>
              <table class="detail-subtable">
                <thead><tr>
                  <th>#</th><th>Type</th><th>Name</th><th>Hole Size</th>
                  <th>Top MD</th><th>Mud Wt</th><th>Mud Type</th>
                  <th>Frac Grad</th><th>BOP/Preventer</th><th>Sections</th>
                </tr></thead>
                <tbody>
                  ${casing.map(c => `<tr>
                    <td>${c.csng_intv_num ?? '—'}</td>
                    <td>${escapeHtml(c.csng_intv_type_cd || '—')}</td>
                    <td>${escapeHtml(c.csng_intv_name || '—')}</td>
                    <td>${c.csng_holesize || '—'}</td>
                    <td>${formatDepth(c.csng_top_md)}</td>
                    <td>${c.csng_mud_wgt_ppg ? c.csng_mud_wgt_ppg + ' ppg' : '—'}</td>
                    <td>${escapeHtml(c.csng_mud_type_cd || '—')}</td>
                    <td>${c.csng_frac_grad_ppg ? c.csng_frac_grad_ppg + ' ppg' : '—'}</td>
                    <td>${escapeHtml(c.csng_preventer_cd || '—')}</td>
                    <td>${c.sections?.length || 0}</td>
                  </tr>
                  ${c.sections && c.sections.length > 0 ? `<tr class="expand-subsection"><td colspan="10">
                    <table class="detail-subtable" style="margin:0">
                      <thead><tr><th>Sec #</th><th>Size</th><th>Weight</th><th>Grade</th><th>Burst</th><th>Collapse</th><th>MD</th><th>TVD</th><th>Pore Prss</th></tr></thead>
                      <tbody>${c.sections.map(s => `<tr>
                        <td>${s.casing_section_num ?? ''}</td>
                        <td>${s.casing_size || '—'}</td>
                        <td>${s.casing_weight ? s.casing_weight + ' lb/ft' : '—'}</td>
                        <td>${s.casing_grade || '—'}</td>
                        <td>${s.casing_burst_psi ? formatNumber(s.casing_burst_psi) + ' psi' : '—'}</td>
                        <td>${s.casing_collapse_psi ? formatNumber(s.casing_collapse_psi) + ' psi' : '—'}</td>
                        <td>${formatDepth(s.casing_section_md)}</td>
                        <td>${formatDepth(s.casing_section_tvd)}</td>
                        <td>${s.casing_pore_prss_ppg ? s.casing_pore_prss_ppg + ' ppg' : '—'}</td>
                      </tr>`).join('')}</tbody>
                    </table>
                  </td></tr>` : ''}`).join('')}
                </tbody>
              </table>
            </div>` : ''}
          ${geo.length > 0 ? `
            <div class="kv-section">
              <div class="kv-section-title">Geologic Markers (${geo.length})</div>
              <table class="detail-subtable">
                <thead><tr><th>Marker</th><th>Top MD (ft)</th><th>H2S</th><th>H2S Activation TVD</th></tr></thead>
                <tbody>
                  ${geo.map(g => `<tr>
                    <td>${escapeHtml(g.geo_marker_name || '—')}</td>
                    <td>${formatDepth(g.top_md)}</td>
                    <td>${escapeHtml(g.h2s_designation || '—')}</td>
                    <td>${g.h2s_actvtn_plan_tvd || '—'}</td>
                  </tr>`).join('')}
                </tbody>
              </table>
            </div>` : ''}
        </div>
      `;
    } catch (e) {
      detailRow.querySelector('.expand-detail-cell').innerHTML = '<div class="expand-loading">Failed to load details</div>';
    }
  }

  // ---- Expandable APM detail ----
  async function toggleApmDetail(row) {
    const sn = row.dataset.apmSn;
    const existing = row.nextElementSibling;
    if (existing && existing.classList.contains('expand-detail-row')) {
      existing.remove();
      row.querySelector('.expand-arrow').innerHTML = '&#9654;';
      return;
    }
    row.querySelector('.expand-arrow').innerHTML = '&#9660;';
    const cols = row.children.length;
    const detailRow = document.createElement('tr');
    detailRow.className = 'expand-detail-row';
    detailRow.innerHTML = `<td colspan="${cols}" class="expand-detail-cell"><div class="expand-loading">Loading...</div></td>`;
    row.after(detailRow);

    try {
      const [apm, prevRes, subopRes] = await Promise.all([
        apiGet(`/submissions/apm/${encodeURIComponent(sn)}`),
        apiGet(`/submissions/apm/${encodeURIComponent(sn)}/preventers`).catch(() => ({ data: [] })),
        apiGet(`/submissions/apm/${encodeURIComponent(sn)}/suboperations`).catch(() => ({ data: [] })),
      ]);
      const preventers = prevRes.data || [];
      const subops = subopRes.data || [];
      const cell = detailRow.querySelector('.expand-detail-cell');

      cell.innerHTML = `
        <div class="expand-detail-content">
          <div class="expand-detail-grid">
            <div class="kv-section">
              <div class="kv-section-title">Permit Details</div>
              ${kv('Serial #', apm.sn_apm)}
              ${kv('Operation', apm.apm_op_cd || '—')}
              ${kv('Well Type', apm.well_type_code || '—')}
              ${kv('Borehole Status', apm.borehole_stat_cd || '—')}
              ${kv('Status Date', formatDate(apm.acc_status_date))}
              ${kv('Submission Date', formatDate(apm.sub_stat_date))}
              ${kv('Work Commences', formatDate(apm.work_commences_date))}
              ${kv('Est. Duration', apm.est_operation_days != null ? apm.est_operation_days + ' days' : '—')}
            </div>
            <div class="kv-section">
              <div class="kv-section-title">Rig & Operator</div>
              ${kv('Rig ID', apm.rig_id_num || '—')}
              ${kv('SV Type', apm.sv_type || '—')}
              ${kv('Operator', apm.operator_name || apm.bus_asc_name || '—')}
              ${kv('Operator #', apm.operator_num || '—')}
              ${kv('Water Depth', formatDepth(apm.water_depth))}
            </div>
            <div class="kv-section">
              <div class="kv-section-title">Location</div>
              ${kv('Surface', [apm.surf_area_code, apm.surf_block_num].filter(Boolean).join(' ') || '—')}
              ${kv('Surface Lease', apm.surf_lease_num || '—')}
              ${kv('Bottom', [apm.botm_area_code, apm.botm_block_num].filter(Boolean).join(' ') || '—')}
              ${kv('Bottom Lease', apm.botm_lease_num || '—')}
            </div>
          </div>
          ${subops.length > 0 ? `
            <div class="kv-section">
              <div class="kv-section-title">Sub-Operations (${subops.length})</div>
              <div class="tag-list">
                ${subops.map(s => `<span class="tag">${escapeHtml(s.apm_subop_cd || '—')}</span>`).join('')}
              </div>
            </div>` : ''}
          ${preventers.length > 0 ? `
            <div class="kv-section">
              <div class="kv-section-title">BOP / Preventers (${preventers.length})</div>
              <table class="detail-subtable">
                <thead><tr>
                  <th>Type</th><th>Stack Size</th><th>Working Prss</th>
                  <th>High Test</th><th>Low Test</th>
                </tr></thead>
                <tbody>
                  ${preventers.map(p => `<tr>
                    <td>${escapeHtml(p.apm_preventer_cd || '—')}</td>
                    <td>${p.bop_stack_size || '—'}</td>
                    <td>${p.bop_working_prss ? formatNumber(p.bop_working_prss) + ' psi' : '—'}</td>
                    <td>${p.bop_high_test_prss ? formatNumber(p.bop_high_test_prss) + ' psi' : '—'}</td>
                    <td>${p.bop_low_test_prss ? formatNumber(p.bop_low_test_prss) + ' psi' : '—'}</td>
                  </tr>`).join('')}
                </tbody>
              </table>
            </div>` : ''}
        </div>
      `;
    } catch (e) {
      detailRow.querySelector('.expand-detail-cell').innerHTML = '<div class="expand-loading">Failed to load details</div>';
    }
  }

  function kv(key, val) {
    return `<div class="kv-row"><span class="kv-key">${key}</span><span class="kv-value">${val ?? '—'}</span></div>`;
  }

  // ---- Init ----
  loadGantt();

  return () => {
    if (_cleanupViewport) { _cleanupViewport(); _cleanupViewport = null; }
  };
}
