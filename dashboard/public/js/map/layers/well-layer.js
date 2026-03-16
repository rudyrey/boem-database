import { apiGet } from '../../core/api.js';

/**
 * Well markers with clustering — loaded on bounds change.
 */
export class WellLayer {
  constructor(map, onClick) {
    this.map = map;
    this.onClick = onClick;
    this.cluster = L.markerClusterGroup({
      maxClusterRadius: 50,
      disableClusteringAtZoom: 13,
      iconCreateFunction: (cluster) => {
        const count = cluster.getChildCount();
        const size = count > 500 ? 44 : count > 100 ? 36 : count > 20 ? 30 : 24;
        return L.divIcon({
          html: `<div>${count > 999 ? Math.round(count / 1000) + 'K' : count}</div>`,
          className: 'marker-cluster-custom',
          iconSize: [size, size],
        });
      },
    });
  }

  async load(bounds) {
    try {
      const res = await apiGet('/wells/map', { bounds });
      this.cluster.clearLayers();

      const markers = [];
      for (const w of res.data) {
        if (!w.lat || !w.lng) continue;
        const colors = { C: '#27ae60', D: '#3498db', O: '#27ae60', R: '#c0392b', E: '#95a5a6' };
        const color = colors[w.status_code] || '#f1c40f';

        const marker = L.circleMarker([w.lat, w.lng], {
          radius: 3.5,
          fillColor: color,
          fillOpacity: 0.7,
          color: '#fff',
          weight: 1,
        });

        marker.on('click', () => {
          if (this.onClick) this.onClick(w);
        });

        markers.push(marker);
      }

      this.cluster.addLayers(markers);
    } catch (e) {
      console.error('Well layer error:', e);
    }
  }

  getLayer() { return this.cluster; }

  destroy() {
    this.cluster.clearLayers();
  }
}
