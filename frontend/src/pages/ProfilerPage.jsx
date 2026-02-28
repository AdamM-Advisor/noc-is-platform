import { useState, useEffect, useMemo, useCallback } from 'react';
import { useSearchParams } from 'react-router-dom';
import { Search, RefreshCw, RotateCcw, ChevronRight, TrendingUp, TrendingDown, Minus, AlertTriangle, ArrowUpDown } from 'lucide-react';
import useProfilerStore from '../stores/profilerStore';
import TemporalPanel from '../components/profiler/TemporalPanel';

const LEVEL_OPTIONS = [
  { value: 'area', label: 'Area' },
  { value: 'regional', label: 'Regional' },
  { value: 'nop', label: 'NOP' },
  { value: 'to', label: 'TO' },
  { value: 'site', label: 'Site' },
];

const GRAN_OPTIONS = [
  { value: 'monthly', label: 'Bulan' },
  { value: 'weekly', label: 'Minggu' },
];

const SEVERITY_OPTIONS = ['Critical', 'Major', 'Minor', 'Low'];

const KPI_LABELS = {
  sla_pct: 'SLA',
  avg_mttr_min: 'MTTR',
  escalation_pct: 'Eskalasi',
  auto_resolve_pct: 'Auto-resolve',
  repeat_pct: 'Repeat',
  total_tickets: 'Volume',
};

function StatusBadge({ status, color }) {
  const colors = {
    green: 'bg-green-100 text-green-700',
    yellow: 'bg-yellow-100 text-yellow-700',
    orange: 'bg-orange-100 text-orange-700',
    red: 'bg-red-100 text-red-700',
    amber: 'bg-amber-100 text-amber-700',
    blue: 'bg-blue-100 text-blue-700',
    gray: 'bg-gray-100 text-gray-600',
  };
  return (
    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${colors[color] || colors.gray}`}>
      {status}
    </span>
  );
}

function KpiCard({ label, value, unit, target, interpretation, trend, onClick }) {
  const statusColors = {
    good: 'border-green-300 bg-green-50',
    warning: 'border-yellow-300 bg-yellow-50',
    alert: 'border-orange-300 bg-orange-50',
    critical: 'border-red-300 bg-red-50',
    neutral: 'border-gray-200 bg-gray-50',
  };
  const dotColors = {
    good: 'bg-green-500',
    warning: 'bg-yellow-500',
    alert: 'bg-orange-500',
    critical: 'bg-red-500',
    neutral: 'bg-gray-400',
  };
  const st = interpretation?.status || 'neutral';
  return (
    <div
      className={`rounded-lg border-2 p-4 cursor-pointer hover:shadow-md transition-shadow ${statusColors[st]}`}
      onClick={onClick}
    >
      <div className="flex items-center justify-between mb-1">
        <span className="text-xs font-medium text-gray-500 uppercase">{label}</span>
        <span className={`w-2.5 h-2.5 rounded-full ${dotColors[st]}`} />
      </div>
      <div className="text-2xl font-bold text-gray-900">
        {typeof value === 'number' ? (unit === '%' ? value.toFixed(1) : value >= 1000 ? `${(value / 1000).toFixed(1)}K` : Math.round(value)) : value}
        {unit && <span className="text-sm font-normal text-gray-500 ml-1">{unit}</span>}
      </div>
      {target !== undefined && (
        <div className="text-xs text-gray-500 mt-1">target: {target}{unit}</div>
      )}
      {trend && <div className="text-xs text-gray-500 mt-0.5">{trend}</div>}
      {interpretation && (
        <div className="text-xs mt-1.5 text-gray-600">{interpretation.text}</div>
      )}
    </div>
  );
}

function BehaviorBadge({ behavior }) {
  if (!behavior) return null;
  const colors = {
    CHRONIC: 'bg-red-100 text-red-800 border-red-200',
    DETERIORATING: 'bg-orange-100 text-orange-800 border-orange-200',
    SPORADIC: 'bg-amber-100 text-amber-800 border-amber-200',
    SEASONAL: 'bg-yellow-100 text-yellow-800 border-yellow-200',
    IMPROVING: 'bg-blue-100 text-blue-800 border-blue-200',
    HEALTHY: 'bg-green-100 text-green-800 border-green-200',
  };
  return (
    <span className={`inline-flex items-center gap-1 px-3 py-1 rounded-full text-sm font-semibold border ${colors[behavior.label] || colors.HEALTHY}`}>
      {behavior.icon} {behavior.label}
    </span>
  );
}

function TrendIcon({ direction }) {
  if (direction === 'up') return <TrendingUp size={16} className="text-green-500" />;
  if (direction === 'down') return <TrendingDown size={16} className="text-red-500" />;
  return <Minus size={16} className="text-gray-400" />;
}

export default function ProfilerPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const {
    filters, setFilters, resetFilters,
    filterOptions, fetchFilterOptions,
    profileData, profileLoading, profileError, generateProfile,
    childrenData, childrenLoading, childrenSort, childrenOrder, fetchChildren,
    peerData, peerLoading, peerKpi, fetchPeerRanking,
    drillDown, navigateBreadcrumb,
    trendData, trendLoading, trendMultiData, trendKpis,
    heatmapData, heatmapLoading,
    childTrendData, childTrendLoading,
    annotations,
    setTrendKpis, fetchMultiTrends, fetchChildTrends,
  } = useProfilerStore();

  useEffect(() => {
    fetchFilterOptions();
  }, []);

  useEffect(() => {
    const level = searchParams.get('level');
    const id = searchParams.get('id');
    if (level && id) {
      const updates = { entityLevel: level, entityId: id };
      const gran = searchParams.get('gran');
      const from = searchParams.get('from');
      const to = searchParams.get('to');
      if (gran) updates.granularity = gran;
      if (from) updates.dateFrom = from;
      if (to) updates.dateTo = to;
      setFilters(updates);
      setTimeout(() => useProfilerStore.getState().generateProfile(), 100);
    }
  }, []);

  const updateUrl = useCallback(() => {
    const f = useProfilerStore.getState().filters;
    const params = {};
    if (f.entityLevel) params.level = f.entityLevel;
    if (f.entityId) params.id = f.entityId;
    if (f.granularity) params.gran = f.granularity;
    if (f.dateFrom) params.from = f.dateFrom;
    if (f.dateTo) params.to = f.dateTo;
    setSearchParams(params, { replace: true });
  }, [setSearchParams]);

  const handleGenerate = () => {
    generateProfile();
    updateUrl();
  };

  const handleReset = () => {
    resetFilters();
    setSearchParams({}, { replace: true });
  };

  return (
    <div className="space-y-6">
      <SelectorPanel
        filters={filters}
        setFilters={setFilters}
        options={filterOptions}
        onGenerate={handleGenerate}
        onReset={handleReset}
        loading={profileLoading}
      />

      {profileError && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-red-700 text-sm">
          {profileError}
        </div>
      )}

      {profileData && (
        <>
          <Breadcrumb
            chain={profileData.identity.parent_chain}
            onNavigate={navigateBreadcrumb}
            filters={filters}
          />

          <IdentityPanel
            identity={profileData.identity}
            behavior={profileData.behavior}
            overallStatus={profileData.overall_status}
            childComposition={profileData.child_composition}
            narrative={profileData.summary_narrative}
          />

          <KpiPanel
            kpis={profileData.kpis}
            recommendations={profileData.recommendations}
          />

          <TemporalPanel
            trendData={trendData}
            trendMultiData={trendMultiData}
            trendKpis={trendKpis}
            trendLoading={trendLoading}
            heatmapData={heatmapData}
            heatmapLoading={heatmapLoading}
            childTrendData={childTrendData}
            childTrendLoading={childTrendLoading}
            annotations={annotations}
            entityLevel={filters.entityLevel}
            onAddKpi={(kpi) => {
              const newKpis = [...trendKpis, kpi].slice(0, 4);
              setTrendKpis(newKpis);
              fetchMultiTrends(newKpis);
            }}
            onRemoveKpi={(kpi) => {
              const newKpis = trendKpis.filter(k => k !== kpi);
              if (newKpis.length === 0) return;
              setTrendKpis(newKpis);
              fetchMultiTrends(newKpis);
            }}
            onChildTrendKpiChange={(kpi) => {
              fetchChildTrends(kpi);
            }}
          />

          <ChildrenPanel
            data={childrenData}
            loading={childrenLoading}
            sort={childrenSort}
            order={childrenOrder}
            onSort={(s) => {
              const newOrder = s === childrenSort && childrenOrder === 'desc' ? 'asc' : 'desc';
              fetchChildren(s, newOrder, 1);
            }}
            onDrillDown={(childLevel, childId) => {
              drillDown(childLevel, childId);
              updateUrl();
            }}
            onPageChange={(p) => fetchChildren(childrenSort, childrenOrder, p)}
            entityLevel={filters.entityLevel}
          />

          <PeerRankingPanel
            data={peerData}
            loading={peerLoading}
            kpi={peerKpi}
            onKpiChange={(k) => fetchPeerRanking(k)}
          />
        </>
      )}

      {!profileData && !profileLoading && (
        <div className="bg-white rounded-lg border p-12 text-center text-gray-500">
          <Search size={48} className="mx-auto mb-4 text-gray-300" />
          <h3 className="text-lg font-medium text-gray-700 mb-2">NOC-IS Profiler</h3>
          <p>Pilih entitas dan periode, lalu klik "Generate Profile" untuk memulai analisis.</p>
        </div>
      )}
    </div>
  );
}

function SelectorPanel({ filters, setFilters, options, onGenerate, onReset, loading }) {
  const [cascadeArea, setCascadeArea] = useState('');
  const [cascadeRegional, setCascadeRegional] = useState('');
  const [cascadeNop, setCascadeNop] = useState('');
  const [cascadeTo, setCascadeTo] = useState('');

  const handleLevelChange = (level) => {
    setFilters({ entityLevel: level, entityId: '' });
    setCascadeArea('');
    setCascadeRegional('');
    setCascadeNop('');
    setCascadeTo('');
  };

  const levelIdx = LEVEL_OPTIONS.findIndex(l => l.value === filters.entityLevel);

  const areas = options?.areas || [];
  const regionals = useMemo(() => {
    if (!options?.regionals) return [];
    if (cascadeArea) return options.regionals.filter(r => r.area_id === cascadeArea);
    return options.regionals;
  }, [options, cascadeArea]);
  const nops = useMemo(() => {
    if (!options?.nops) return [];
    if (cascadeRegional) return options.nops.filter(n => n.regional_id === cascadeRegional);
    return options.nops;
  }, [options, cascadeRegional]);
  const tos = useMemo(() => {
    if (!options?.tos) return [];
    if (cascadeNop) return options.tos.filter(t => t.nop_id === cascadeNop);
    return options.tos;
  }, [options, cascadeNop]);

  const showArea = levelIdx >= 1;
  const showRegional = levelIdx >= 2;
  const showNop = levelIdx >= 3;
  const showTo = levelIdx >= 4;

  const handleCascadeArea = (val) => {
    setCascadeArea(val);
    setCascadeRegional('');
    setCascadeNop('');
    setCascadeTo('');
    if (filters.entityLevel === 'area') setFilters({ entityId: val });
    else setFilters({ entityId: '' });
  };

  const handleCascadeRegional = (val) => {
    setCascadeRegional(val);
    setCascadeNop('');
    setCascadeTo('');
    if (filters.entityLevel === 'regional') setFilters({ entityId: val });
    else setFilters({ entityId: '' });
  };

  const handleCascadeNop = (val) => {
    setCascadeNop(val);
    setCascadeTo('');
    if (filters.entityLevel === 'nop') setFilters({ entityId: val });
    else setFilters({ entityId: '' });
  };

  const handleCascadeTo = (val) => {
    setCascadeTo(val);
    if (filters.entityLevel === 'to') setFilters({ entityId: val });
    else setFilters({ entityId: '' });
  };

  return (
    <div className="bg-white rounded-lg border shadow-sm p-5 sticky top-0 z-10 space-y-4">
      <div className="flex items-center gap-2 text-lg font-semibold text-gray-800 mb-2">
        <Search size={20} className="text-blue-600" />
        NOC-IS PROFILER
      </div>

      <div className="border rounded-lg p-4 space-y-3">
        <div className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Entitas (Siapa)</div>
        <div className="flex flex-wrap gap-2">
          {LEVEL_OPTIONS.map(opt => (
            <label key={opt.value} className="flex items-center gap-1.5 cursor-pointer">
              <input
                type="radio"
                name="entityLevel"
                value={opt.value}
                checked={filters.entityLevel === opt.value}
                onChange={() => handleLevelChange(opt.value)}
                className="text-blue-600"
              />
              <span className="text-sm">{opt.label}</span>
            </label>
          ))}
        </div>

        <div className="flex flex-wrap gap-2 items-center">
          {filters.entityLevel === 'area' && (
            <select className="border rounded-lg px-3 py-2 text-sm flex-1 min-w-[180px]" value={filters.entityId} onChange={(e) => setFilters({ entityId: e.target.value })}>
              <option value="">-- Pilih Area --</option>
              {areas.map(a => <option key={a.id} value={a.id}>{a.name}</option>)}
            </select>
          )}

          {showArea && filters.entityLevel !== 'area' && (
            <>
              <select className="border rounded-lg px-3 py-2 text-sm min-w-[150px]" value={cascadeArea} onChange={(e) => handleCascadeArea(e.target.value)}>
                <option value="">Area: Semua</option>
                {areas.map(a => <option key={a.id} value={a.id}>{a.name}</option>)}
              </select>
              <span className="text-gray-300">→</span>
            </>
          )}

          {filters.entityLevel === 'regional' && (
            <select className="border rounded-lg px-3 py-2 text-sm flex-1 min-w-[180px]" value={filters.entityId} onChange={(e) => setFilters({ entityId: e.target.value })}>
              <option value="">-- Pilih Regional --</option>
              {regionals.map(r => <option key={r.id} value={r.id}>{r.name}</option>)}
            </select>
          )}

          {showRegional && filters.entityLevel !== 'regional' && (
            <>
              <select className="border rounded-lg px-3 py-2 text-sm min-w-[150px]" value={cascadeRegional} onChange={(e) => handleCascadeRegional(e.target.value)}>
                <option value="">Regional: Semua</option>
                {regionals.map(r => <option key={r.id} value={r.id}>{r.name}</option>)}
              </select>
              <span className="text-gray-300">→</span>
            </>
          )}

          {filters.entityLevel === 'nop' && (
            <select className="border rounded-lg px-3 py-2 text-sm flex-1 min-w-[180px]" value={filters.entityId} onChange={(e) => setFilters({ entityId: e.target.value })}>
              <option value="">-- Pilih NOP --</option>
              {nops.map(n => <option key={n.id} value={n.id}>{n.name}</option>)}
            </select>
          )}

          {showNop && filters.entityLevel !== 'nop' && (
            <>
              <select className="border rounded-lg px-3 py-2 text-sm min-w-[150px]" value={cascadeNop} onChange={(e) => handleCascadeNop(e.target.value)}>
                <option value="">NOP: Semua</option>
                {nops.map(n => <option key={n.id} value={n.id}>{n.name}</option>)}
              </select>
              <span className="text-gray-300">→</span>
            </>
          )}

          {filters.entityLevel === 'to' && (
            <select className="border rounded-lg px-3 py-2 text-sm flex-1 min-w-[180px]" value={filters.entityId} onChange={(e) => setFilters({ entityId: e.target.value })}>
              <option value="">-- Pilih TO --</option>
              {tos.map(t => <option key={t.id} value={t.id}>{t.name}</option>)}
            </select>
          )}

          {filters.entityLevel === 'site' && (
            <>
              {showArea && (
                <>
                  <select className="border rounded-lg px-3 py-2 text-sm min-w-[130px]" value={cascadeArea} onChange={(e) => handleCascadeArea(e.target.value)}>
                    <option value="">Area</option>
                    {areas.map(a => <option key={a.id} value={a.id}>{a.name}</option>)}
                  </select>
                  <span className="text-gray-300">→</span>
                </>
              )}
              <input
                type="text"
                className="border rounded-lg px-3 py-2 text-sm flex-1 min-w-[180px]"
                placeholder="Ketik Site ID..."
                value={filters.entityId}
                onChange={(e) => setFilters({ entityId: e.target.value })}
              />
            </>
          )}
        </div>
      </div>

      <div className="grid md:grid-cols-2 gap-4">
        <div className="border rounded-lg p-4 space-y-3">
          <div className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Waktu (Kapan)</div>
          <div className="flex flex-wrap gap-2">
            {GRAN_OPTIONS.map(opt => (
              <label key={opt.value} className="flex items-center gap-1.5 cursor-pointer">
                <input
                  type="radio"
                  name="granularity"
                  value={opt.value}
                  checked={filters.granularity === opt.value}
                  onChange={(e) => setFilters({ granularity: e.target.value })}
                  className="text-blue-600"
                />
                <span className="text-sm">{opt.label}</span>
              </label>
            ))}
          </div>
          <div className="flex items-center gap-2">
            <select
              className="border rounded-lg px-3 py-2 text-sm flex-1"
              value={filters.dateFrom}
              onChange={(e) => setFilters({ dateFrom: e.target.value })}
            >
              <option value="">Dari...</option>
              {options?.periods?.map(p => <option key={p} value={p}>{p}</option>)}
            </select>
            <span className="text-gray-400">s/d</span>
            <select
              className="border rounded-lg px-3 py-2 text-sm flex-1"
              value={filters.dateTo}
              onChange={(e) => setFilters({ dateTo: e.target.value })}
            >
              <option value="">Sampai...</option>
              {options?.periods?.map(p => <option key={p} value={p}>{p}</option>)}
            </select>
          </div>
        </div>

        <div className="border rounded-lg p-4 space-y-3">
          <div className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Gangguan (Apa)</div>
          <div className="flex flex-wrap gap-2">
            {['', 'Event', 'Incident'].map(t => (
              <label key={t} className="flex items-center gap-1.5 cursor-pointer">
                <input
                  type="radio"
                  name="typeTicket"
                  value={t}
                  checked={filters.typeTicket === t}
                  onChange={(e) => setFilters({ typeTicket: e.target.value })}
                  className="text-blue-600"
                />
                <span className="text-sm">{t || 'Semua'}</span>
              </label>
            ))}
          </div>
          <div className="flex flex-wrap gap-2">
            {SEVERITY_OPTIONS.map(s => (
              <label key={s} className="flex items-center gap-1.5 cursor-pointer">
                <input
                  type="checkbox"
                  checked={filters.severities.includes(s)}
                  onChange={(e) => {
                    const newSev = e.target.checked
                      ? [...filters.severities, s]
                      : filters.severities.filter(v => v !== s);
                    setFilters({ severities: newSev });
                  }}
                  className="rounded text-blue-600"
                />
                <span className="text-sm">{s}</span>
              </label>
            ))}
          </div>
          <div className="flex gap-2">
            <select
              className="border rounded-lg px-3 py-2 text-sm flex-1"
              value={filters.faultLevel}
              onChange={(e) => setFilters({ faultLevel: e.target.value })}
            >
              <option value="">Fault Level: Semua</option>
              {options?.fault_levels?.map(f => <option key={f} value={f}>{f}</option>)}
            </select>
          </div>
        </div>
      </div>

      <div className="flex items-center gap-3 pt-2">
        <button
          onClick={onGenerate}
          disabled={!filters.entityId || loading}
          className="bg-blue-600 text-white px-5 py-2.5 rounded-lg text-sm font-medium hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
        >
          {loading ? <RefreshCw size={16} className="animate-spin" /> : <Search size={16} />}
          Generate Profile
        </button>
        <button
          onClick={onReset}
          className="border border-gray-300 text-gray-600 px-4 py-2.5 rounded-lg text-sm font-medium hover:bg-gray-50 flex items-center gap-2"
        >
          <RotateCcw size={16} />
          Reset
        </button>
      </div>
    </div>
  );
}

function Breadcrumb({ chain, onNavigate, filters }) {
  if (!chain || chain.length === 0) return null;
  const granLabel = GRAN_OPTIONS.find(g => g.value === filters.granularity)?.label || filters.granularity;
  return (
    <div className="bg-white rounded-lg border p-3 space-y-1">
      <div className="flex items-center gap-1 text-sm flex-wrap">
        <span className="text-gray-400">📍</span>
        {chain.map((item, idx) => (
          <span key={item.id} className="flex items-center gap-1">
            {idx > 0 && <ChevronRight size={14} className="text-gray-300" />}
            <button
              onClick={() => onNavigate(item.level, item.id)}
              className={`hover:text-blue-600 hover:underline ${idx === chain.length - 1 ? 'font-semibold text-gray-900' : 'text-gray-500'}`}
            >
              {item.name}
            </button>
          </span>
        ))}
      </div>
      <div className="text-xs text-gray-400">
        🕐 {filters.dateFrom || '...'} - {filters.dateTo || '...'} • {granLabel}
        {filters.typeTicket && <> • {filters.typeTicket}</>}
        {!filters.typeTicket && <> • Semua Gangguan</>}
      </div>
    </div>
  );
}

function IdentityPanel({ identity, behavior, overallStatus, childComposition, narrative }) {
  const statusColors = {
    'SEHAT': 'text-green-600',
    'PERLU PERHATIAN': 'text-yellow-600',
    'MONITORING': 'text-blue-600',
  };

  const totalSites = Object.values(identity.site_composition || {}).reduce((a, b) => a + b, 0);

  return (
    <div className="bg-white rounded-lg border p-5 space-y-4">
      <div className="flex items-start justify-between flex-wrap gap-3">
        <div>
          <h2 className="text-xl font-bold text-gray-900">
            PROFIL: {identity.level.toUpperCase()} {identity.name}
          </h2>
          <div className="flex items-center gap-3 mt-1">
            <span className={`text-sm font-medium ${statusColors[overallStatus] || 'text-gray-600'}`}>
              Status: {overallStatus}
            </span>
          </div>
        </div>
        <BehaviorBadge behavior={behavior} />
      </div>

      <div className="grid md:grid-cols-2 gap-4">
        <div className="border rounded-lg p-4 space-y-2">
          <h3 className="text-sm font-semibold text-gray-700">Info</h3>
          <div className="text-sm space-y-1 text-gray-600">
            <div>Level: <span className="font-medium text-gray-900">{identity.level.toUpperCase()}</span></div>
            <div>Nama: <span className="font-medium text-gray-900">{identity.name}</span></div>
            {identity.parent_chain?.length > 1 && (
              <div>Parent: <span className="font-medium text-gray-900">{identity.parent_chain[0]?.name}</span></div>
            )}
            {Object.entries(identity.child_counts || {}).map(([k, v]) => (
              <div key={k}>{k.toUpperCase()}: <span className="font-medium text-gray-900">{v} aktif</span></div>
            ))}
            {totalSites > 0 && (
              <div className="pt-2 space-y-1">
                {Object.entries(identity.site_composition || {}).map(([cls, cnt]) => (
                  <div key={cls} className="flex justify-between text-xs">
                    <span>{cls}</span>
                    <span className="font-medium">{cnt.toLocaleString()} ({(cnt / totalSites * 100).toFixed(1)}%)</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>

        <div className="border rounded-lg p-4 space-y-3">
          <h3 className="text-sm font-semibold text-gray-700">Behavior</h3>
          <div className="flex items-center gap-2">
            <span className="text-2xl">{behavior?.icon}</span>
            <div>
              <div className="font-semibold text-gray-900">{behavior?.label}</div>
              <div className="text-xs text-gray-500">"{behavior?.reason}"</div>
            </div>
          </div>
          {childComposition && childComposition.total > 0 && (
            <div className="space-y-1.5 pt-2">
              <div className="text-xs font-medium text-gray-500">
                Komposisi {childComposition.type_label} ({childComposition.total}):
              </div>
              {Object.entries(childComposition.by_behavior || {}).map(([label, count]) => {
                const meta = BEHAVIOR_META[label] || {};
                const pct = (count / childComposition.total) * 100;
                return (
                  <div key={label} className="flex items-center gap-2 text-xs">
                    <span className="w-24">{meta.icon || ''} {label}</span>
                    <div className="flex-1 bg-gray-100 rounded-full h-3">
                      <div
                        className={`h-3 rounded-full ${meta.barColor || 'bg-gray-400'}`}
                        style={{ width: `${pct}%` }}
                      />
                    </div>
                    <span className="w-6 text-right font-medium">{count}</span>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>

      {narrative && (
        <div className="bg-blue-50 border border-blue-100 rounded-lg p-4">
          <div className="text-sm text-gray-700 italic">"{narrative}"</div>
        </div>
      )}
    </div>
  );
}

const BEHAVIOR_META = {
  CHRONIC: { icon: '🔴', barColor: 'bg-red-500' },
  DETERIORATING: { icon: '📉', barColor: 'bg-orange-500' },
  SPORADIC: { icon: '🟠', barColor: 'bg-amber-500' },
  SEASONAL: { icon: '🟡', barColor: 'bg-yellow-500' },
  IMPROVING: { icon: '📈', barColor: 'bg-blue-500' },
  HEALTHY: { icon: '🟢', barColor: 'bg-green-500' },
};

function KpiPanel({ kpis, recommendations }) {
  if (!kpis) return null;
  return (
    <div className="bg-white rounded-lg border p-5 space-y-4">
      <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide">KPI Performance</h3>
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
        <KpiCard
          label="Volume"
          value={kpis.volume?.avg || 0}
          unit=""
          interpretation={kpis.volume?.interpretation}
          trend={kpis.volume?.mom_change ? `${kpis.volume.mom_change > 0 ? '▲' : '▼'}${Math.abs(kpis.volume.mom_change).toFixed(1)}% MoM` : null}
        />
        <KpiCard
          label="SLA"
          value={kpis.sla_pct?.value || 0}
          unit="%"
          target={kpis.sla_pct?.target}
          interpretation={kpis.sla_pct?.interpretation}
        />
        <KpiCard
          label="MTTR"
          value={kpis.avg_mttr_min?.value || 0}
          unit=" min"
          interpretation={kpis.avg_mttr_min?.interpretation}
        />
        <KpiCard
          label="Eskalasi"
          value={kpis.escalation_pct?.value || 0}
          unit="%"
          interpretation={kpis.escalation_pct?.interpretation}
        />
        <KpiCard
          label="Auto-resolve"
          value={kpis.auto_resolve_pct?.value || 0}
          unit="%"
          interpretation={kpis.auto_resolve_pct?.interpretation}
        />
        <KpiCard
          label="Repeat"
          value={kpis.repeat_pct?.value || 0}
          unit="%"
          interpretation={kpis.repeat_pct?.interpretation}
        />
      </div>

      {recommendations && recommendations.length > 0 && (
        <div className="border-t pt-4">
          <h4 className="text-sm font-semibold text-gray-700 mb-2 flex items-center gap-2">
            <AlertTriangle size={16} className="text-yellow-500" />
            Rekomendasi
          </h4>
          <ul className="space-y-1.5">
            {recommendations.map((rec, idx) => (
              <li key={idx} className="flex items-start gap-2 text-sm">
                <span>{rec.icon}</span>
                <span className="text-gray-700">{rec.text}</span>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

function ChildrenPanel({ data, loading, sort, order, onSort, onDrillDown, onPageChange, entityLevel }) {
  if (!data) return null;
  const childLevel = data.child_level;
  if (childLevel === 'ticket') {
    return (
      <div className="bg-white rounded-lg border p-5">
        <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide">Detail Tiket</h3>
        <p className="text-sm text-gray-500 mt-2">Site level — data tiket individual tersedia di halaman lain.</p>
      </div>
    );
  }

  const sortHeaders = [
    { key: 'name', label: 'Nama' },
    { key: 'total_tickets', label: 'Volume' },
    { key: 'sla_pct', label: 'SLA %' },
    { key: 'avg_mttr_min', label: 'MTTR' },
    { key: 'risk_score', label: 'Risk' },
    { key: 'escalation_pct', label: 'Esc %' },
  ];

  const totalPages = Math.ceil((data.total || 0) / (data.per_page || 20));

  return (
    <div className="bg-white rounded-lg border p-5 space-y-4">
      <div className="flex items-center justify-between flex-wrap gap-2">
        <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide">
          Child Entities: {data.total} {childLevel?.toUpperCase()}
        </h3>
      </div>

      {loading ? (
        <div className="text-center py-8 text-gray-400">
          <RefreshCw size={24} className="animate-spin mx-auto mb-2" />
          Memuat...
        </div>
      ) : (
        <>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b">
                  {sortHeaders.map(h => (
                    <th
                      key={h.key}
                      className="text-left py-2 px-3 text-xs font-medium text-gray-500 uppercase cursor-pointer hover:text-gray-700"
                      onClick={() => onSort(h.key)}
                    >
                      <span className="flex items-center gap-1">
                        {h.label}
                        {sort === h.key && <ArrowUpDown size={12} className="text-blue-500" />}
                      </span>
                    </th>
                  ))}
                  <th className="text-left py-2 px-3 text-xs font-medium text-gray-500 uppercase">Trend</th>
                  <th className="text-left py-2 px-3 text-xs font-medium text-gray-500 uppercase">Behavior</th>
                </tr>
              </thead>
              <tbody>
                {(data.data || []).map(child => (
                  <tr
                    key={child.entity_id}
                    className="border-b hover:bg-blue-50 cursor-pointer transition-colors"
                    onClick={() => onDrillDown(childLevel, child.entity_id)}
                  >
                    <td className="py-2.5 px-3 font-medium text-blue-600 hover:underline">{child.name}</td>
                    <td className="py-2.5 px-3">{child.total_tickets >= 1000 ? `${(child.total_tickets / 1000).toFixed(1)}K` : child.total_tickets}</td>
                    <td className="py-2.5 px-3">
                      <span className={child.sla_pct < 90 ? 'text-red-600 font-medium' : ''}>{child.sla_pct}%</span>
                    </td>
                    <td className="py-2.5 px-3">{Math.round(child.avg_mttr_min)}m</td>
                    <td className="py-2.5 px-3">
                      <RiskBadge score={child.risk_score} />
                    </td>
                    <td className="py-2.5 px-3">{child.escalation_pct}%</td>
                    <td className="py-2.5 px-3"><TrendIcon direction={child.trend_direction} /></td>
                    <td className="py-2.5 px-3">
                      <span className="text-xs">{child.behavior_icon} {child.behavior_label}</span>
                    </td>
                  </tr>
                ))}
                {(!data.data || data.data.length === 0) && (
                  <tr>
                    <td colSpan={8} className="py-8 text-center text-gray-400">Tidak ada data child entity</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>

          {totalPages > 1 && (
            <div className="flex items-center justify-center gap-2 pt-2">
              <button
                disabled={data.page <= 1}
                onClick={() => onPageChange(data.page - 1)}
                className="px-3 py-1.5 text-sm border rounded disabled:opacity-50"
              >
                ‹ Prev
              </button>
              <span className="text-sm text-gray-500">
                Hal {data.page} dari {totalPages}
              </span>
              <button
                disabled={data.page >= totalPages}
                onClick={() => onPageChange(data.page + 1)}
                className="px-3 py-1.5 text-sm border rounded disabled:opacity-50"
              >
                Next ›
              </button>
            </div>
          )}

          {data.narrative && (
            <div className="bg-gray-50 border rounded-lg p-3 text-sm text-gray-600 italic">
              "{data.narrative}"
            </div>
          )}
        </>
      )}
    </div>
  );
}

function RiskBadge({ score }) {
  let color = 'bg-green-100 text-green-700';
  let icon = '🟢';
  if (score >= 60) {
    color = 'bg-red-100 text-red-700';
    icon = '🔴';
  } else if (score >= 35) {
    color = 'bg-yellow-100 text-yellow-700';
    icon = '🟡';
  }
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium ${color}`}>
      {icon} {score}
    </span>
  );
}

function PeerRankingPanel({ data, loading, kpi, onKpiChange }) {
  if (!data) return null;

  const maxVal = Math.max(...(data.peers || []).map(p => p.value), 1);

  return (
    <div className="bg-white rounded-lg border p-5 space-y-4">
      <div className="flex items-center justify-between flex-wrap gap-2">
        <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide">
          Peer Ranking: {data.total} Peers
        </h3>
        <select
          className="border rounded-lg px-3 py-1.5 text-sm"
          value={kpi}
          onChange={(e) => onKpiChange(e.target.value)}
        >
          {Object.entries(KPI_LABELS).map(([k, v]) => (
            <option key={k} value={k}>{v}</option>
          ))}
        </select>
      </div>

      {loading ? (
        <div className="text-center py-8 text-gray-400">
          <RefreshCw size={24} className="animate-spin mx-auto mb-2" />
          Memuat...
        </div>
      ) : (
        <>
          <div className="space-y-1.5">
            {(data.peers || []).map((peer, idx) => {
              const barWidth = maxVal > 0 ? (peer.value / maxVal) * 100 : 0;
              return (
                <div
                  key={peer.id}
                  className={`flex items-center gap-3 py-1.5 px-2 rounded ${peer.is_current ? 'bg-blue-50 border border-blue-200' : ''}`}
                >
                  <span className="w-5 text-xs text-gray-400 text-right">{idx + 1}</span>
                  <span className={`w-32 text-sm truncate ${peer.is_current ? 'font-bold text-blue-700' : 'text-gray-700'}`}>
                    {peer.is_current && '►'}{peer.name}
                  </span>
                  <div className="flex-1 bg-gray-100 rounded-full h-5 relative">
                    <div
                      className={`h-5 rounded-full transition-all ${peer.is_current ? 'bg-blue-500' : 'bg-gray-300'}`}
                      style={{ width: `${Math.max(barWidth, 2)}%` }}
                    />
                  </div>
                  <span className={`w-16 text-sm text-right ${peer.is_current ? 'font-bold text-blue-700' : 'text-gray-600'}`}>
                    {peer.value.toFixed(1)}
                  </span>
                </div>
              );
            })}
          </div>

          {data.narrative && (
            <div className="bg-gray-50 border rounded-lg p-3 text-sm text-gray-600 italic">
              "{data.narrative}"
            </div>
          )}
        </>
      )}
    </div>
  );
}
