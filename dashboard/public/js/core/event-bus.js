class EventBus {
  constructor() { this._listeners = new Map(); }

  on(event, callback) {
    if (!this._listeners.has(event)) this._listeners.set(event, new Set());
    this._listeners.get(event).add(callback);
    return () => this.off(event, callback);
  }

  off(event, callback) {
    const l = this._listeners.get(event);
    if (l) l.delete(callback);
  }

  emit(event, data) {
    const l = this._listeners.get(event);
    if (l) for (const cb of l) { try { cb(data); } catch (e) { console.error(`EventBus "${event}":`, e); } }
  }

  once(event, callback) {
    const w = (data) => { this.off(event, w); callback(data); };
    this.on(event, w);
  }
}

export const eventBus = new EventBus();
