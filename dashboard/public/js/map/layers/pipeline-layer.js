import { apiGet, apiPost } from '../../core/api.js';

/**
 * Pipeline polylines — loaded on demand by visible segments.
 */
export class PipelineLayer {
  constructor(map, onClick) {
    this.map = map;
    this.onClick = onClick;
    this.group = L.layerGroup();
    this._loadedSegments = new Set();
    this._lines = new Map();
  }

  async load(bounds) {
    try {
      const res = await apiGet('/pipelines/map', { bounds });
      const newSegs = res.data.filter(s => !this._loadedSegments.has(s.segment_num));

      if (newSegs.length === 0) return;

      // Batch load geometry
      const segNums = newSegs.map(s => s.segment_num).slice(0, 100);
      const geoRes = await apiPost('/pipelines/geometry-batch', { segment_nums: segNums });

      for (const seg of newSegs) {
        const coords = geoRes.data[seg.segment_num];
        if (!coords || coords.length < 2) continue;

        const latlngs = coords.map(c => [c.lat, c.lng]);
        const colors = { 'BLKO': '#2c3e50', 'G/C': '#e74c3c', 'BLKG': '#3498db' };
        const color = colors[seg.product_code] || '#2980b9';
        const opacity = seg.status_code === 'ACT' ? 0.7 : 0.3;

        const line = L.polyline(latlngs, {
          color, weight: 2, opacity,
          dashArray: seg.status_code !== 'ACT' ? '5 5' : null,
        });

        line.bindTooltip(`Seg ${seg.segment_num}<br>${seg.origin_name || ''} → ${seg.dest_name || ''}`);
        line.on('click', () => { if (this.onClick) this.onClick(seg); });

        this.group.addLayer(line);
        this._lines.set(seg.segment_num, line);
        this._loadedSegments.add(seg.segment_num);
      }
    } catch (e) {
      console.error('Pipeline layer error:', e);
    }
  }

  getLayer() { return this.group; }

  clear() {
    this.group.clearLayers();
    this._loadedSegments.clear();
    this._lines.clear();
  }

  destroy() {
    this.clear();
  }
}
