import { getStats } from '../core/api.js';
import { formatNumber, formatCompact } from '../core/utils.js';
import { ChartPanel, productionChartConfig } from '../components/chart-panel.js';

export async function initDashboardView(container) {
  container.innerHTML = `
    <div class="view-header"><h2>Dashboard</h2></div>
    <div class="stat-cards" id="stat-cards"></div>
    <div class="chart-grid">
      <div class="chart-container">
        <h3>Annual Production (1996–2025)</h3>
        <div id="annual-chart" style="height:320px"></div>
      </div>
      <div class="chart-container">
        <h3>Top 10 Producing Leases (All Time)</h3>
        <div id="top-chart" style="height:320px"></div>
      </div>
    </div>
  `;

  const stats = await getStats();
  const c = stats.counts;
  const cum = stats.cumulativeProduction;

  // Stat cards
  document.getElementById('stat-cards').innerHTML = `
    <div class="stat-card">
      <div class="stat-label">Total Wells</div>
      <div class="stat-value">${formatNumber(c.wells)}</div>
      <div class="stat-sub">Tracked in database</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Active Leases</div>
      <div class="stat-value stat-success">${formatNumber(c.activeLeases)}</div>
      <div class="stat-sub">of ${formatNumber(c.leases)} total</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Platforms</div>
      <div class="stat-value stat-accent">${formatNumber(c.platforms)}</div>
      <div class="stat-sub">${formatNumber(c.producingPlatforms)} producing</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Cumulative Oil</div>
      <div class="stat-value stat-info">${formatCompact(cum.total_oil)} bbl</div>
      <div class="stat-sub">Since 1996 · ${formatCompact(cum.total_gas)} MCF gas</div>
    </div>
  `;

  // Annual production chart
  const annualChart = new ChartPanel(document.getElementById('annual-chart'));
  const aData = stats.annualSummary;
  annualChart.update({
    type: 'line',
    data: {
      labels: aData.map(d => d.year),
      datasets: [
        { label: 'Oil (BBL)', data: aData.map(d => d.total_oil), borderColor: '#2c3e50', backgroundColor: 'rgba(44,62,80,0.1)', fill: true, tension: 0.3, pointRadius: 2, borderWidth: 2 },
        { label: 'Gas (MCF)', data: aData.map(d => d.total_gas), borderColor: '#e74c3c', backgroundColor: 'rgba(231,76,60,0.05)', fill: true, tension: 0.3, pointRadius: 2, borderWidth: 2 },
        { label: 'Water (BBL)', data: aData.map(d => d.total_water), borderColor: '#3498db', backgroundColor: 'rgba(52,152,219,0.05)', fill: true, tension: 0.3, pointRadius: 2, borderWidth: 2 },
      ],
    },
    scales: {
      x: { grid: { display: false }, ticks: { font: { size: 10 } } },
      y: { grid: { color: 'rgba(0,0,0,0.05)' }, ticks: { font: { size: 10 }, callback: v => formatCompact(v) } },
    },
  });

  // Top producers chart
  const topChart = new ChartPanel(document.getElementById('top-chart'));
  const tData = stats.topProducers;
  topChart.update({
    type: 'bar',
    data: {
      labels: tData.map(d => d.lease_number),
      datasets: [{
        label: 'Total Oil (BBL)',
        data: tData.map(d => d.total_oil),
        backgroundColor: '#e67e22',
        borderRadius: 4,
      }],
    },
    options: { indexAxis: 'y' },
    scales: {
      x: { grid: { color: 'rgba(0,0,0,0.05)' }, ticks: { font: { size: 10 }, callback: v => formatCompact(v) } },
      y: { grid: { display: false }, ticks: { font: { size: 10, family: 'monospace' } } },
    },
  });

  return () => { annualChart.destroy(); topChart.destroy(); };
}
