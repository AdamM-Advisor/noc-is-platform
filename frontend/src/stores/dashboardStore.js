import { create } from 'zustand';
import axios from 'axios';

const useDashboardStore = create((set, get) => ({
  period: '',
  viewLevel: 'area',
  parentFilter: null,

  periods: [],
  parentOptions: [],
  data: null,
  loading: false,
  error: null,

  setPeriod: (p) => set({ period: p }),
  setViewLevel: (l) => set({ viewLevel: l, parentFilter: null }),
  setParentFilter: (f) => set({ parentFilter: f }),

  fetchPeriods: async () => {
    try {
      const res = await axios.get('/api/dashboard/periods');
      const periods = res.data.periods || [];
      set({ periods });
      if (!get().period && periods.length > 0) {
        set({ period: periods[0] });
      }
    } catch {}
  },

  fetchParentOptions: async () => {
    const { viewLevel } = get();
    try {
      const res = await axios.get('/api/dashboard/parent-options', { params: { view_level: viewLevel } });
      set({ parentOptions: res.data.parents || [] });
    } catch {
      set({ parentOptions: [] });
    }
  },

  fetchDashboard: async () => {
    const { period, viewLevel, parentFilter } = get();
    if (!period) return;
    set({ loading: true, error: null });
    try {
      const res = await axios.post('/api/dashboard/overview', {
        period,
        view_level: viewLevel,
        parent_filter: parentFilter || null,
      });
      set({ data: res.data, loading: false });
    } catch (err) {
      set({ loading: false, error: err.response?.data?.detail || 'Gagal memuat dashboard' });
    }
  },
}));

export default useDashboardStore;
