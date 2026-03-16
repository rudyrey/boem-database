import { CONFIG } from '../core/config.js';

/**
 * Small Leaflet map for detail panels.
 */
export class MiniMap {
  constructor(container, { lat, lng, zoom = 10, markerLabel = '' } = {}) {
    this.container = container;
    this.container.classList.add('mini-map');

    this.map = L.map(this.container, {
      center: [lat || 27.5, lng || -90.5],
      zoom,
      zoomControl: false,
      attributionControl: false,
      dragging: true,
      scrollWheelZoom: false,
    });

    L.tileLayer(CONFIG.TILES.ocean.url, { maxZoom: 16 }).addTo(this.map);

    if (lat && lng) {
      this.marker = L.circleMarker([lat, lng], {
        radius: 7,
        fillColor: '#e67e22',
        fillOpacity: 0.9,
        color: '#fff',
        weight: 2,
      }).addTo(this.map);

      if (markerLabel) this.marker.bindTooltip(markerLabel, { permanent: true, direction: 'top', offset: [0, -10] });
    }

    // Invalidate size after container is visible
    setTimeout(() => this.map.invalidateSize(), 100);
  }

  setView(lat, lng, zoom) {
    if (lat && lng) {
      this.map.setView([lat, lng], zoom || 10);
      if (this.marker) this.marker.setLatLng([lat, lng]);
      else {
        this.marker = L.circleMarker([lat, lng], {
          radius: 7, fillColor: '#e67e22', fillOpacity: 0.9, color: '#fff', weight: 2,
        }).addTo(this.map);
      }
    }
  }

  addPolyline(points, color = '#2980b9') {
    if (this._polyline) this.map.removeLayer(this._polyline);
    if (!points || points.length === 0) return;
    const latlngs = points.map(p => [p.lat, p.lng]);
    this._polyline = L.polyline(latlngs, { color, weight: 3, opacity: 0.8 }).addTo(this.map);
    this.map.fitBounds(this._polyline.getBounds(), { padding: [20, 20] });
  }

  destroy() {
    if (this.map) { this.map.remove(); this.map = null; }
  }
}
