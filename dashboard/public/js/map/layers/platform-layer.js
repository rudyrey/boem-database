import { apiGet } from '../../core/api.js';

/**
 * Platform markers with clustering.
 */
export class PlatformLayer {
  constructor(map, onClick) {
    this.map = map;
    this.onClick = onClick;
    this.cluster = L.markerClusterGroup({
      maxClusterRadius: 40,
      spiderfyOnMaxZoom: true,
      disableClusteringAtZoom: 12,
      iconCreateFunction: (cluster) => {
        const count = cluster.getChildCount();
        const size = count > 100 ? 40 : count > 20 ? 32 : 26;
        return L.divIcon({
          html: `<div>${count}</div>`,
          className: 'marker-cluster-custom',
          iconSize: [size, size],
        });
      },
    });
    this.map.addLayer(this.cluster);
    this._loaded = false;
  }

  async load(bounds) {
    try {
      const res = await apiGet('/platforms/map', { bounds });
      this.cluster.clearLayers();

      const markers = [];
      for (const p of res.data) {
        if (!p.lat || !p.lng) continue;
        const producing = p.oil_producing === 'Y' || p.gas_producing === 'Y';
        const color = p.drilling === 'Y' ? '#3498db' : producing ? '#e67e22' : '#95a5a6';

        const marker = L.circleMarker([p.lat, p.lng], {
          radius: 5,
          fillColor: color,
          fillOpacity: 0.8,
          color: '#fff',
          weight: 1.5,
        });

        marker.bindTooltip(
          `<b>${p.structure_name || p.complex_id}</b><br>${p.structure_type || ''}`
        );

        marker.on('click', () => {
          if (this.onClick) this.onClick(p);
        });

        markers.push(marker);
      }

      this.cluster.addLayers(markers);
      this._loaded = true;
    } catch (e) {
      console.error('Platform layer error:', e);
    }
  }

  getLayer() { return this.cluster; }

  destroy() {
    this.cluster.clearLayers();
    this.map.removeLayer(this.cluster);
  }
}
