export const CONFIG = {
  API_BASE: '/api',
  MAP: {
    center: [27.5, -90.5],
    zoom: 7,
    minZoom: 5,
    maxZoom: 16,
  },
  TILES: {
    ocean: {
      url: 'https://server.arcgisonline.com/ArcGIS/rest/services/Ocean/World_Ocean_Base/MapServer/tile/{z}/{y}/{x}',
      attribution: 'Esri, GEBCO, NOAA',
      label: 'Ocean',
    },
    satellite: {
      url: 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
      attribution: 'Esri, Maxar, Earthstar',
      label: 'Satellite',
    },
    osm: {
      url: 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
      attribution: '&copy; OpenStreetMap contributors',
      label: 'OpenStreetMap',
    },
  },
  PAGE_SIZE: 50,
};
