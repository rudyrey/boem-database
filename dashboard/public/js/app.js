/**
 * BOEM Data Dashboard — SPA Entry Point
 *
 * Initialises the hash router, global search, and status bar.
 * Each route handler lazily imports its view module so the initial
 * page load stays lightweight.
 */

import { Router } from './core/router.js';
import { SearchBox } from './components/search-box.js';
import { getStats } from './core/api.js';
import { formatCompact } from './core/utils.js';
import { showToast } from './core/toast.js';

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------
(async function boot() {
  const container = document.getElementById('view-container');
  const router = new Router();

  // Global search box (header)
  new SearchBox(document.getElementById('global-search'));

  // ---------------------------------------------------------------------------
  // Route definitions  — each calls its view's init function with the container
  // ---------------------------------------------------------------------------

  router.add('/dashboard', async () => {
    const { initDashboardView } = await import('./views/dashboard-view.js');
    return initDashboardView(container);
  });

  router.add('/map', async () => {
    const { initMapView } = await import('./views/map-view.js');
    return initMapView(container);
  });

  router.add('/wells', async () => {
    const { initWellsView } = await import('./views/wells-view.js');
    return initWellsView(container);
  });

  router.add('/wells/:id', async (params) => {
    const { initWellsView } = await import('./views/wells-view.js');
    return initWellsView(container, params);
  });

  router.add('/leases', async () => {
    const { initLeasesView } = await import('./views/leases-view.js');
    return initLeasesView(container);
  });

  router.add('/leases/:id', async (params) => {
    const { initLeasesView } = await import('./views/leases-view.js');
    return initLeasesView(container, params);
  });

  router.add('/platforms', async () => {
    const { initPlatformsView } = await import('./views/platforms-view.js');
    return initPlatformsView(container);
  });

  router.add('/platforms/:id', async (params) => {
    const { initPlatformsView } = await import('./views/platforms-view.js');
    return initPlatformsView(container, params);
  });

  router.add('/pipelines', async () => {
    const { initPipelinesView } = await import('./views/pipelines-view.js');
    return initPipelinesView(container);
  });

  router.add('/pipelines/:id', async (params) => {
    const { initPipelinesView } = await import('./views/pipelines-view.js');
    return initPipelinesView(container, params);
  });

  router.add('/production', async () => {
    const { initProductionView } = await import('./views/production-view.js');
    return initProductionView(container);
  });

  router.add('/companies', async () => {
    const { initCompaniesView } = await import('./views/companies-view.js');
    return initCompaniesView(container);
  });

  router.add('/companies/:id', async (params) => {
    const { initCompaniesView } = await import('./views/companies-view.js');
    return initCompaniesView(container, params);
  });

  // ---------------------------------------------------------------------------
  // Status bar — show DB summary on load
  // ---------------------------------------------------------------------------
  try {
    const stats = await getStats();
    const statusBar = document.getElementById('status-bar');
    statusBar.textContent =
      `${formatCompact(stats.counts.wells)} wells · ` +
      `${formatCompact(stats.counts.activeLeases)} active leases · ` +
      `${formatCompact(stats.counts.platforms)} platforms · ` +
      `${formatCompact(stats.counts.productionRecords)} production records`;
    statusBar.style.opacity = '1';
  } catch (e) {
    document.getElementById('status-bar').textContent = 'API offline';
    showToast('Could not connect to API. Make sure the server is running.', 'error');
  }

  // Start router (reads current hash and renders first view)
  router.start();
})();
