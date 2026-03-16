/**
 * Hash-based SPA router.
 * Routes: { pattern: string, handler: (params) => void }
 */
export class Router {
  constructor() {
    this._routes = [];
    this._currentCleanup = null;
    window.addEventListener('hashchange', () => this._resolve());
  }

  add(pattern, handler) {
    // Convert "/wells/:id" to regex with named groups
    const paramNames = [];
    const regexStr = pattern.replace(/:(\w+)/g, (_, name) => {
      paramNames.push(name);
      return '([^/]+)';
    });
    this._routes.push({
      regex: new RegExp(`^${regexStr}$`),
      paramNames,
      handler,
    });
    return this;
  }

  start() {
    if (!window.location.hash) window.location.hash = '#/dashboard';
    this._resolve();
  }

  async _resolve() {
    const hash = window.location.hash.slice(1) || '/dashboard';

    // Cleanup previous view
    if (this._currentCleanup) {
      this._currentCleanup();
      this._currentCleanup = null;
    }

    for (const route of this._routes) {
      const match = hash.match(route.regex);
      if (match) {
        const params = {};
        route.paramNames.forEach((name, i) => { params[name] = match[i + 1]; });

        // Update sidebar active state immediately
        this._updateNav(hash);

        try {
          const cleanup = await route.handler(params);
          if (typeof cleanup === 'function') this._currentCleanup = cleanup;
        } catch (e) {
          console.error('[Router] View error:', e);
        }
        return;
      }
    }
  }

  _updateNav(hash) {
    document.querySelectorAll('.nav-item').forEach(el => {
      const route = el.dataset.route;
      if (route) {
        const isActive = hash === `/${route}` || hash.startsWith(`/${route}/`);
        el.classList.toggle('active', isActive);
      }
    });
  }

  navigate(path) {
    window.location.hash = '#' + path;
  }
}
