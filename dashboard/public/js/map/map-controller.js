import { CONFIG } from '../core/config.js';
import { eventBus } from '../core/event-bus.js';
import { debounce } from '../core/utils.js';

/**
 * Creates and manages a Leaflet map instance.
 */
export class MapController {
  constructor(container, options = {}) {
    const { center, zoom, minZoom, maxZoom } = { ...CONFIG.MAP, ...options };

    this.map = L.map(container, { center, zoom, minZoom, maxZoom, zoomControl: true });

    // Base layers
    this.baseLayers = {};
    for (const [key, cfg] of Object.entries(CONFIG.TILES)) {
      this.baseLayers[cfg.label] = L.tileLayer(cfg.url, { attribution: cfg.attribution, maxZoom: 16 });
    }
    this.baseLayers['Ocean'].addTo(this.map);

    // Overlay layers managed externally
    this.overlays = {};

    // Emit bounds on move
    const emitBounds = debounce(() => {
      const b = this.map.getBounds();
      eventBus.emit('map:boundsChanged', {
        south: b.getSouth(), west: b.getWest(),
        north: b.getNorth(), east: b.getEast(),
        zoom: this.map.getZoom(),
      });
    }, 400);

    this.map.on('moveend', emitBounds);
    this.map.on('zoomend', emitBounds);

    // Layer control
    this.layerControl = L.control.layers(this.baseLayers, {}, { position: 'topright', collapsed: true });
    this.layerControl.addTo(this.map);

    setTimeout(() => this.map.invalidateSize(), 200);
  }

  addOverlay(name, layer) {
    this.overlays[name] = layer;
    this.layerControl.addOverlay(layer, name);
  }

  getBounds() {
    const b = this.map.getBounds();
    return `${b.getSouth()},${b.getWest()},${b.getNorth()},${b.getEast()}`;
  }

  fitBounds(bounds) {
    this.map.fitBounds(bounds, { padding: [30, 30] });
  }

  destroy() {
    if (this.map) { this.map.remove(); this.map = null; }
  }
}
