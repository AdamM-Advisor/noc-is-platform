import { create } from 'zustand';

const TTL = 300000;

const useCacheStore = create((set, get) => ({
  cache: {},

  get: (key) => {
    const entry = get().cache[key];
    if (!entry) return null;
    if (Date.now() - entry.timestamp > TTL) {
      const newCache = { ...get().cache };
      delete newCache[key];
      set({ cache: newCache });
      return null;
    }
    return entry.data;
  },

  set: (key, data) => {
    set({
      cache: {
        ...get().cache,
        [key]: { data, timestamp: Date.now() },
      },
    });
  },

  invalidate: (key) => {
    const newCache = { ...get().cache };
    delete newCache[key];
    set({ cache: newCache });
  },

  clearAll: () => {
    set({ cache: {} });
  },

  getAge: (key) => {
    const entry = get().cache[key];
    if (!entry) return null;
    return Date.now() - entry.timestamp;
  },
}));

export default useCacheStore;
