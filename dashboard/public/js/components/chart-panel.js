/**
 * Reusable Chart.js wrapper.
 * Manages canvas lifecycle and chart instance.
 */
export class ChartPanel {
  constructor(container, type = 'line') {
    this.container = container;
    this.type = type;
    this.chart = null;
    this.canvas = document.createElement('canvas');
    this.container.appendChild(this.canvas);
  }

  update(config) {
    if (this.chart) this.chart.destroy();

    this.chart = new Chart(this.canvas, {
      type: config.type || this.type,
      data: config.data,
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: {
            position: 'top',
            labels: { font: { size: 11, family: '-apple-system, sans-serif' }, padding: 12, usePointStyle: true },
          },
          tooltip: {
            backgroundColor: 'rgba(44, 62, 80, 0.9)',
            titleFont: { size: 12 },
            bodyFont: { size: 11 },
            padding: 10,
            cornerRadius: 4,
            callbacks: config.tooltipCallbacks || {},
          },
        },
        scales: config.scales || {},
        ...(config.options || {}),
      },
    });
  }

  destroy() {
    if (this.chart) { this.chart.destroy(); this.chart = null; }
    this.canvas.remove();
  }
}

/** Compact axis tick formatter */
function compactTick(v) {
  if (v >= 1e6) return (v / 1e6).toFixed(1) + 'M';
  if (v >= 1e3) return (v / 1e3).toFixed(0) + 'K';
  return v;
}

/**
 * Create a production time-series chart config.
 * Gas is placed on a secondary Y-axis (right) since it's measured in MCF
 * while Oil/Water are in BBL.
 */
export function productionChartConfig(data, { showWater = true } = {}) {
  const labels = data.map(d => d.production_date || d.year);

  const datasets = [
    {
      label: 'Oil (BBL)',
      data: data.map(d => d.oil || 0),
      borderColor: '#2c3e50',
      backgroundColor: 'rgba(44, 62, 80, 0.1)',
      fill: true,
      tension: 0.3,
      pointRadius: 0,
      borderWidth: 2,
      yAxisID: 'y',
    },
    {
      label: 'Gas (MCF)',
      data: data.map(d => d.gas || 0),
      borderColor: '#e74c3c',
      backgroundColor: 'rgba(231, 76, 60, 0.05)',
      fill: false,
      tension: 0.3,
      pointRadius: 0,
      borderWidth: 2,
      yAxisID: 'y1',
    },
  ];

  if (showWater) {
    datasets.push({
      label: 'Water (BBL)',
      data: data.map(d => d.water || 0),
      borderColor: '#3498db',
      backgroundColor: 'rgba(52, 152, 219, 0.1)',
      fill: true,
      tension: 0.3,
      pointRadius: 0,
      borderWidth: 2,
      yAxisID: 'y',
    });
  }

  return {
    data: { labels, datasets },
    scales: {
      x: {
        grid: { display: false },
        ticks: { font: { size: 10 }, maxTicksLimit: 15 },
      },
      y: {
        type: 'linear',
        position: 'left',
        title: { display: true, text: 'BBL', font: { size: 10 } },
        grid: { color: 'rgba(0,0,0,0.05)' },
        ticks: { font: { size: 10 }, callback: compactTick },
      },
      y1: {
        type: 'linear',
        position: 'right',
        title: { display: true, text: 'MCF', font: { size: 10 }, color: '#e74c3c' },
        grid: { drawOnChartArea: false },
        ticks: { font: { size: 10 }, color: '#e74c3c', callback: compactTick },
      },
    },
    tooltipCallbacks: {
      label: (ctx) => {
        const v = ctx.parsed.y;
        return `${ctx.dataset.label}: ${v >= 1e6 ? (v / 1e6).toFixed(2) + 'M' : v.toLocaleString()}`;
      },
    },
  };
}
