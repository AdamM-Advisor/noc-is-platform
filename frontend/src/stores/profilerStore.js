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

  trendData: null,
  trendLoading: false,
  trendKpis: ['sla_pct'],
  trendMultiData: {},

  heatmapData: null,
  heatmapLoading: false,

  childTrendData: null,
  childTrendLoading: false,

  annotations: [],

  gangguanOverview: null,
  gangguanCrossDim: null,
  gangguanLoading: false,
  gangguanDistribution: null,
  gangguanTopSites: null,
  gangguanFaultHeatmap: null,

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
      trendData: null,
      trendMultiData: {},
      trendKpis: ['sla_pct'],
      heatmapData: null,
      childTrendData: null,
      annotations: [],
      gangguanOverview: null,
      gangguanCrossDim: null,
      gangguanDistribution: null,
      gangguanTopSites: null,
      gangguanFaultHeatmap: null,
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
      get().fetchTemporalData();
      get().fetchGangguanData();
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

  fetchTrend: async (kpi) => {
    const { filters } = get();
    if (!filters.entityId) return;
    set({ trendLoading: true });
    try {
      const res = await axios.get('/api/profiler/trends', {
        params: {
          entity_level: filters.entityLevel,
          entity_id: filters.entityId,
          kpi: kpi || 'sla_pct',
          granularity: filters.granularity,
          date_from: filters.dateFrom,
          date_to: filters.dateTo,
          type_ticket: filters.typeTicket,
          severities: filters.severities.join(','),
          fault_level: filters.faultLevel,
        },
      });
      set((s) => ({
        trendData: res.data,
        trendLoading: false,
        trendMultiData: { ...s.trendMultiData, [kpi || 'sla_pct']: res.data },
      }));
    } catch {
      set({ trendLoading: false });
    }
  },

  fetchMultiTrends: async (kpiList) => {
    const { filters } = get();
    if (!filters.entityId) return;
    set({ trendLoading: true });
    const results = {};
    for (const kpi of kpiList) {
      try {
        const res = await axios.get('/api/profiler/trends', {
          params: {
            entity_level: filters.entityLevel,
            entity_id: filters.entityId,
            kpi,
            granularity: filters.granularity,
            date_from: filters.dateFrom,
            date_to: filters.dateTo,
            type_ticket: filters.typeTicket,
            severities: filters.severities.join(','),
            fault_level: filters.faultLevel,
          },
        });
        results[kpi] = res.data;
      } catch { /* skip failed */ }
    }
    set({
      trendMultiData: results,
      trendData: results[kpiList[0]] || null,
      trendLoading: false,
    });
  },

  setTrendKpis: (kpis) => {
    set({ trendKpis: kpis });
  },

  fetchHeatmap: async () => {
    const { filters } = get();
    if (!filters.entityId) return;
    set({ heatmapLoading: true });
    try {
      const res = await axios.get('/api/profiler/heatmap', {
        params: {
          entity_level: filters.entityLevel,
          entity_id: filters.entityId,
          granularity: filters.granularity,
          date_from: filters.dateFrom,
          date_to: filters.dateTo,
          type_ticket: filters.typeTicket,
          severities: filters.severities.join(','),
          fault_level: filters.faultLevel,
        },
      });
      set({ heatmapData: res.data, heatmapLoading: false });
    } catch {
      set({ heatmapLoading: false });
    }
  },

  fetchChildTrends: async (kpi) => {
    const { filters } = get();
    if (!filters.entityId || filters.entityLevel === 'site') return;
    set({ childTrendLoading: true });
    try {
      const res = await axios.get('/api/profiler/child-trends', {
        params: {
          entity_level: filters.entityLevel,
          entity_id: filters.entityId,
          kpi: kpi || 'sla_pct',
          granularity: filters.granularity,
          date_from: filters.dateFrom,
          date_to: filters.dateTo,
          type_ticket: filters.typeTicket,
          severities: filters.severities.join(','),
          fault_level: filters.faultLevel,
        },
      });
      set({ childTrendData: res.data, childTrendLoading: false });
    } catch {
      set({ childTrendLoading: false });
    }
  },

  fetchAnnotations: async () => {
    const { filters, profileData } = get();
    try {
      const params = {};
      if (filters.dateFrom) params.from = filters.dateFrom + '-01';
      if (filters.dateTo) params.to = filters.dateTo + '-28';
      const areaId = profileData?.identity?.parent_chain?.find(p => p.level === 'area')?.id || (filters.entityLevel === 'area' ? filters.entityId : '');
      if (areaId) params.area_id = areaId;
      const res = await axios.get('/api/external/annotations', { params });
      set({ annotations: res.data || [] });
    } catch {
      set({ annotations: [] });
    }
  },

  fetchGangguanOverview: async () => {
    const { filters } = get();
    if (!filters.entityId) return;
    set({ gangguanLoading: true });
    try {
      const res = await axios.get('/api/profiler/gangguan/overview', {
        params: {
          entity_level: filters.entityLevel,
          entity_id: filters.entityId,
          date_from: filters.dateFrom,
          date_to: filters.dateTo,
          type_ticket: filters.typeTicket,
          severities: filters.severities.join(','),
        },
      });
      set({ gangguanOverview: res.data, gangguanLoading: false });
    } catch {
      set({ gangguanLoading: false });
    }
  },

  fetchGangguanCrossDim: async (faultLevel, rcCategory) => {
    const { filters } = get();
    if (!filters.entityId) return;
    set({ gangguanLoading: true });
    try {
      const res = await axios.get('/api/profiler/gangguan/cross-dimension', {
        params: {
          entity_level: filters.entityLevel,
          entity_id: filters.entityId,
          fault_level: faultLevel || '',
          rc_category: rcCategory || '',
          date_from: filters.dateFrom,
          date_to: filters.dateTo,
          type_ticket: filters.typeTicket,
          severities: filters.severities.join(','),
        },
      });
      set({ gangguanCrossDim: res.data, gangguanLoading: false });
    } catch {
      set({ gangguanLoading: false });
    }
  },

  fetchGangguanDistribution: async (entityLevel, entityId, faultLevel, rcCategory) => {
    try {
      const { filters } = get();
      const res = await axios.get('/api/profiler/gangguan/distribution', {
        params: {
          entity_level: entityLevel || filters.entityLevel,
          entity_id: entityId || filters.entityId,
          fault_level: faultLevel || '',
          rc_category: rcCategory || '',
          date_from: filters.dateFrom,
          date_to: filters.dateTo,
          type_ticket: filters.typeTicket,
          severities: filters.severities.join(','),
        },
      });
      set({ gangguanDistribution: res.data });
    } catch { /* */ }
  },

  fetchGangguanTopSites: async (faultLevel, rcCategory) => {
    const { filters } = get();
    try {
      const res = await axios.get('/api/profiler/gangguan/top-sites', {
        params: {
          entity_level: filters.entityLevel,
          entity_id: filters.entityId,
          fault_level: faultLevel || filters.faultLevel || '',
          rc_category: rcCategory || filters.rcCategory || '',
          date_from: filters.dateFrom,
          date_to: filters.dateTo,
          type_ticket: filters.typeTicket,
          severities: filters.severities.join(','),
        },
      });
      set({ gangguanTopSites: res.data });
    } catch { /* */ }
  },

  fetchGangguanFaultHeatmap: async (faultLevel) => {
    const { filters } = get();
    try {
      const res = await axios.get('/api/profiler/heatmap', {
        params: {
          entity_level: filters.entityLevel,
          entity_id: filters.entityId,
          granularity: filters.granularity,
          date_from: filters.dateFrom,
          date_to: filters.dateTo,
          type_ticket: filters.typeTicket,
          severities: filters.severities.join(','),
          fault_level: faultLevel || filters.faultLevel || '',
        },
      });
      set({ gangguanFaultHeatmap: res.data });
    } catch { /* */ }
  },

  fetchGangguanData: async () => {
    const { filters } = get();
    if (filters.faultLevel || filters.rcCategory) {
      get().fetchGangguanCrossDim(filters.faultLevel, filters.rcCategory);
      get().fetchGangguanTopSites(filters.faultLevel, filters.rcCategory);
      if (filters.faultLevel) {
        get().fetchGangguanFaultHeatmap(filters.faultLevel);
      }
    } else {
      get().fetchGangguanOverview();
    }
  },

  fetchTemporalData: async () => {
    const state = get();
    state.fetchMultiTrends(state.trendKpis);
    state.fetchHeatmap();
    state.fetchChildTrends(state.trendKpis[0]);
    state.fetchAnnotations();
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
