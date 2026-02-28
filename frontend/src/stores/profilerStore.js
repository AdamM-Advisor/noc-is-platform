import { create } from 'zustand';
import axios from 'axios';

const useProfilerStore = create((set, get) => ({
  filters: {
    entityLevel: 'area',
    entityId: '',
    granularity: 'monthly',
    dateFrom: '',
    dateTo: '',
    typeTicket: '',
    severities: [],
    faultLevel: '',
    rcCategory: '',
  },

  filterOptions: null,
  filterOptionsLoading: false,

  profileData: null,
  profileLoading: false,
  profileError: null,

  childrenData: null,
  childrenLoading: false,
  childrenSort: 'risk_score',
  childrenOrder: 'desc',
  childrenPage: 1,

  peerData: null,
  peerLoading: false,
  peerKpi: 'sla_pct',

  setFilters: (updates) => {
    set((s) => ({ filters: { ...s.filters, ...updates } }));
  },

  resetFilters: () => {
    set({
      filters: {
        entityLevel: 'area',
        entityId: '',
        granularity: 'monthly',
        dateFrom: '',
        dateTo: '',
        typeTicket: '',
        severities: [],
        faultLevel: '',
        rcCategory: '',
      },
      profileData: null,
      childrenData: null,
      peerData: null,
      profileError: null,
    });
  },

  fetchFilterOptions: async () => {
    if (get().filterOptions && !get().filterOptionsLoading) return;
    set({ filterOptionsLoading: true });
    try {
      const res = await axios.get('/api/profiler/filter-options');
      set({ filterOptions: res.data, filterOptionsLoading: false });
    } catch {
      set({ filterOptionsLoading: false });
    }
  },

  generateProfile: async () => {
    const { filters } = get();
    if (!filters.entityId) return;

    set({ profileLoading: true, profileError: null });
    try {
      const res = await axios.post('/api/profiler/generate', {
        entity_level: filters.entityLevel,
        entity_id: filters.entityId,
        granularity: filters.granularity,
        date_from: filters.dateFrom,
        date_to: filters.dateTo,
        type_ticket: filters.typeTicket,
        severities: filters.severities,
        fault_level: filters.faultLevel,
        rc_category: filters.rcCategory,
      });
      set({ profileData: res.data, profileLoading: false });

      get().fetchChildren();
      get().fetchPeerRanking();
    } catch (err) {
      set({ profileLoading: false, profileError: err.response?.data?.detail || 'Gagal generate profil' });
    }
  },

  fetchChildren: async (sort, order, page) => {
    const { filters, childrenSort, childrenOrder, childrenPage } = get();
    const s = sort || childrenSort;
    const o = order || childrenOrder;
    const p = page || childrenPage;

    set({ childrenLoading: true, childrenSort: s, childrenOrder: o, childrenPage: p });
    try {
      const res = await axios.get('/api/profiler/children', {
        params: {
          entity_level: filters.entityLevel,
          entity_id: filters.entityId,
          date_from: filters.dateFrom,
          date_to: filters.dateTo,
          type_ticket: filters.typeTicket,
          severities: filters.severities.join(','),
          fault_level: filters.faultLevel,
          sort: s,
          order: o,
          page: p,
          per_page: 20,
        },
      });
      set({ childrenData: res.data, childrenLoading: false });
    } catch {
      set({ childrenLoading: false });
    }
  },

  fetchPeerRanking: async (kpi) => {
    const { filters, peerKpi } = get();
    const k = kpi || peerKpi;
    set({ peerLoading: true, peerKpi: k });
    try {
      const res = await axios.get('/api/profiler/peer-ranking', {
        params: {
          entity_level: filters.entityLevel,
          entity_id: filters.entityId,
          kpi: k,
          date_from: filters.dateFrom,
          date_to: filters.dateTo,
          type_ticket: filters.typeTicket,
          severities: filters.severities.join(','),
          fault_level: filters.faultLevel,
        },
      });
      set({ peerData: res.data, peerLoading: false });
    } catch {
      set({ peerLoading: false });
    }
  },

  drillDown: (childLevel, childId) => {
    const { filters } = get();
    set({
      filters: { ...filters, entityLevel: childLevel, entityId: childId },
      profileData: null,
      childrenData: null,
      peerData: null,
    });
    setTimeout(() => get().generateProfile(), 0);
  },

  navigateBreadcrumb: (level, id) => {
    const { filters } = get();
    set({
      filters: { ...filters, entityLevel: level, entityId: id },
      profileData: null,
      childrenData: null,
      peerData: null,
    });
    setTimeout(() => get().generateProfile(), 0);
  },
}));

export default useProfilerStore;
