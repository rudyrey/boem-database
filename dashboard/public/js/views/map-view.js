import { apiGet } from '../core/api.js';
import { eventBus } from '../core/event-bus.js';
import { escapeHtml, formatNumber, formatDepth, wellStatusBadge } from '../core/utils.js';
import { MapController } from '../map/map-controller.js';
import { PlatformLayer } from '../map/layers/platform-layer.js';
import { WellLayer } from '../map/layers/well-layer.js';
import { PipelineLayer } from '../map/layers/pipeline-layer.js';

export async function initMapView(container) {
  container.innerHTML = `
    <div style="position:relative;height:calc(100vh - var(--header-height) - 2*var(--space-lg))">
      <div id="map-full" class="map-full"></div>
      <div id="map-detail" class="map-detail-panel" style="display:none"></div>
    </div>
  `;

  const mapCtrl = new MapController(document.getElementById('map-full'));
  const detailEl = document.getElementById('map-detail');

  // Platform layer
  const platformLayer = new PlatformLayer(mapCtrl.map, async (p) => {
    const detail = await apiGet(`/platforms/${p.complex_id}`);
    showDetail('platform', detail);
  });

  // Well layer
  const wellLayer = new WellLayer(mapCtrl.map, async (w) => {
    const detail = await apiGet(`/wells/${w.api_well_number}`);
    showDetail('well', detail);
  });

  // Pipeline layer
  const pipelineLayer = new PipelineLayer(mapCtrl.map, async (seg) => {
    const detail = await apiGet(`/pipelines/${seg.segment_num}`);
    showDetail('pipeline', detail);
  });

  mapCtrl.addOverlay('Platforms', platformLayer.getLayer());
  mapCtrl.addOverlay('Wells', wellLayer.getLayer());
  mapCtrl.addOverlay('Pipelines', pipelineLayer.getLayer());

  // Add wells layer by default
  mapCtrl.map.addLayer(wellLayer.getLayer());
  mapCtrl.map.addLayer(pipelineLayer.getLayer());

  // Load data on bounds change
  const loadData = (bounds) => {
    const b = `${bounds.south},${bounds.west},${bounds.north},${bounds.east}`;
    platformLayer.load(b);
    if (mapCtrl.map.hasLayer(wellLayer.getLayer())) wellLayer.load(b);
    if (mapCtrl.map.hasLayer(pipelineLayer.getLayer())) pipelineLayer.load(b);
  };

  const unsub = eventBus.on('map:boundsChanged', loadData);

  // Initial load
  platformLayer.load(mapCtrl.getBounds());
  wellLayer.load(mapCtrl.getBounds());
  pipelineLayer.load(mapCtrl.getBounds());

  function showDetail(type, data) {
    let html = `<div class="detail-header">
      <div><h3>${escapeHtml(getTitle(type, data))}</h3>
        <div class="detail-subtitle">${escapeHtml(getSubtitle(type, data))}</div></div>
      <button class="detail-close" id="map-detail-close">×</button>
    </div><div class="detail-body">`;

    if (type === 'platform') {
      html += kvRow('Complex ID', data.complex_id);
      html += kvRow('Area/Block', `${data.area_code} ${data.block_number}`);
      html += kvRow('Operator', data.operator_name);
      html += kvRow('Water Depth', formatDepth(data.water_depth));
      html += kvRow('Oil Producing', data.oil_producing === 'Y' ? '✓' : '—');
      html += kvRow('Gas Producing', data.gas_producing === 'Y' ? '✓' : '—');
      html += kvRow('Drilling', data.drilling === 'Y' ? '✓' : '—');
      if (data.structures?.length) {
        html += `<div class="kv-section-title" style="margin-top:12px">Structures</div>`;
        for (const s of data.structures) {
          html += `<div style="font-size:12px;padding:4px 0;border-bottom:1px solid #ecf0f1">
            ${escapeHtml(s.structure_name || '—')} · ${s.structure_type || ''} · Installed ${s.install_date || '—'}
          </div>`;
        }
      }
      html += `<div class="detail-actions"><a class="detail-action-btn" href="#/platforms/${data.complex_id}">Full Detail →</a></div>`;
    } else if (type === 'well') {
      html += kvRow('API', data.api_well_number);
      html += kvRow('Well Name', data.well_name);
      html += kvRow('Operator', data.operator_name);
      html += kvRow('Status', wellStatusBadge(data.status_code));
      html += kvRow('Spud Date', data.spud_date);
      html += kvRow('Depth', formatDepth(data.total_measured_depth));
      html += kvRow('Water Depth', formatDepth(data.water_depth));
      html += `<div class="detail-actions"><a class="detail-action-btn" href="#/wells/${encodeURIComponent(data.api_well_number)}">Full Detail →</a></div>`;
    } else if (type === 'pipeline') {
      html += kvRow('Segment', data.segment_num);
      html += kvRow('Origin', data.origin_name);
      html += kvRow('Destination', data.dest_name);
      html += kvRow('Product', data.product_code);
      html += kvRow('Status', data.status_code);
      html += kvRow('Operator', data.operator_name);
      html += `<div class="detail-actions"><a class="detail-action-btn" href="#/pipelines/${data.segment_num}">Full Detail →</a></div>`;
    }

    html += '</div>';
    detailEl.innerHTML = html;
    detailEl.style.display = 'block';
    document.getElementById('map-detail-close')?.addEventListener('click', () => { detailEl.style.display = 'none'; });
  }

  function getTitle(type, d) {
    if (type === 'platform') return d.structures?.[0]?.structure_name || `Complex ${d.complex_id}`;
    if (type === 'well') return d.well_name || d.api_well_number;
    return `Segment ${d.segment_num}`;
  }
  function getSubtitle(type, d) {
    if (type === 'platform') return `${d.area_code} ${d.block_number}`;
    if (type === 'well') return d.operator_name || '';
    return `${d.origin_name || ''} → ${d.dest_name || ''}`;
  }
  function kvRow(key, val) {
    return `<div class="kv-row"><span class="kv-key">${key}</span><span class="kv-value">${val ?? '—'}</span></div>`;
  }

  return () => {
    unsub();
    platformLayer.destroy();
    wellLayer.destroy();
    pipelineLayer.destroy();
    mapCtrl.destroy();
  };
}
