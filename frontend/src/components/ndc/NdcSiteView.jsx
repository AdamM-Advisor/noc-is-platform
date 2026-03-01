import { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import NdcDistribution from './NdcDistribution';

function NdcSiteView() {
  const [entityLevel, setEntityLevel] = useState('site');
  const [entityId, setEntityId] = useState('');
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);

  const [hierarchyOptions, setHierarchyOptions] = useState(null);
  const [cascadeRegional, setCascadeRegional] = useState('');
  const [cascadeNop, setCascadeNop] = useState('');
  const [cascadeTo, setCascadeTo] = useState('');
  const [siteOptions, setSiteOptions] = useState([]);
  const [sitesLoading, setSitesLoading] = useState(false);
  const siteAbortRef = useRef(null);

  const levels = [
    { value: 'site', label: 'Site' },
    { value: 'to', label: 'TO' },
    { value: 'nop', label: 'NOP' },
    { value: 'regional', label: 'Regional' },
  ];

  useEffect(() => {
    fetch('/api/profiler/filter-options')
      .then(r => r.json())
      .then(d => setHierarchyOptions(d))
      .catch(() => {});
  }, []);

  const allRegionals = hierarchyOptions?.regionals || [];
  const nops = useMemo(() => {
    if (!hierarchyOptions?.nops) return [];
    if (cascadeRegional) return hierarchyOptions.nops.filter(n => n.regional_id === cascadeRegional);
    return hierarchyOptions.nops;
  }, [hierarchyOptions, cascadeRegional]);
  const tos = useMemo(() => {
    if (!hierarchyOptions?.tos) return [];
    if (cascadeNop) return hierarchyOptions.tos.filter(t => t.nop_id === cascadeNop);
    return hierarchyOptions.tos;
  }, [hierarchyOptions, cascadeNop]);

  const fetchSites = useCallback(async (toId) => {
    if (siteAbortRef.current) siteAbortRef.current.abort();
    if (!toId) { setSiteOptions([]); return; }
    const controller = new AbortController();
    siteAbortRef.current = controller;
    setSitesLoading(true);
    try {
      const res = await fetch(`/api/profiler/sites?to_id=${encodeURIComponent(toId)}`, { signal: controller.signal });
      const d = await res.json();
      setSiteOptions(d);
    } catch (e) {
      if (e.name !== 'AbortError') setSiteOptions([]);
    } finally {
      setSitesLoading(false);
    }
  }, []);

  const resetCascade = () => {
    setCascadeRegional('');
    setCascadeNop('');
    setCascadeTo('');
    setSiteOptions([]);
    setEntityId('');
    setData(null);
  };

  const handleLevelChange = (val) => {
    setEntityLevel(val);
    resetCascade();
  };

  const handleCascadeRegional = (val) => {
    setCascadeRegional(val);
    setCascadeNop('');
    setCascadeTo('');
    setSiteOptions([]);
    if (entityLevel === 'regional') {
      setEntityId(val);
      if (val) loadEntity('regional', val);
      else setData(null);
    } else {
      setEntityId('');
      setData(null);
    }
  };

  const handleCascadeNop = (val) => {
    setCascadeNop(val);
    setCascadeTo('');
    setSiteOptions([]);
    if (entityLevel === 'nop') {
      setEntityId(val);
      if (val) loadEntity('nop', val);
      else setData(null);
    } else {
      setEntityId('');
      setData(null);
    }
  };

  const handleCascadeTo = (val) => {
    setCascadeTo(val);
    setSiteOptions([]);
    if (entityLevel === 'to') {
      setEntityId(val);
      if (val) loadEntity('to', val);
      else setData(null);
    } else if (entityLevel === 'site') {
      setEntityId('');
      setData(null);
      if (val) fetchSites(val);
    }
  };

  const handleSiteSelect = (val) => {
    setEntityId(val);
    if (val) loadEntity('site', val);
    else setData(null);
  };

  const loadEntity = async (level, id) => {
    if (!id) return;
    setLoading(true);
    try {
      const url = level === 'site' ? `/api/ndc/site/${id}` : `/api/ndc/entity/${level}/${id}`;
      const res = await fetch(url);
      const d = await res.json();
      setData(d);
    } catch (e) {
      console.error('Failed to fetch entity NDC:', e);
      setData([]);
    } finally {
      setLoading(false);
    }
  };

  const showRegional = true;
  const showNop = entityLevel === 'site' || entityLevel === 'to' || entityLevel === 'nop';
  const showTo = entityLevel === 'site' || entityLevel === 'to';
  const showSite = entityLevel === 'site';

  return (
    <div>
      <div className="mb-4">
        <h3 className="text-sm font-semibold text-[#0F172A]">NDC Distribution per Entity</h3>
        <p className="text-xs text-[#475569] mt-1">Lihat distribusi NDC untuk site, TO, NOP, atau regional tertentu</p>
      </div>

      <div className="flex flex-wrap items-center gap-2 mb-6">
        <select
          value={entityLevel}
          onChange={e => handleLevelChange(e.target.value)}
          className="text-sm border border-gray-200 rounded-lg px-3 py-2 bg-white"
        >
          {levels.map(l => <option key={l.value} value={l.value}>{l.label}</option>)}
        </select>

        {showRegional && (
          <>
            <span className="text-gray-300">→</span>
            <select className="text-sm border border-gray-200 rounded-lg px-3 py-2 bg-white min-w-[130px]" value={entityLevel === 'regional' ? entityId : cascadeRegional} onChange={e => handleCascadeRegional(e.target.value)}>
              <option value="">-- Regional --</option>
              {allRegionals.map(r => <option key={r.id} value={r.id}>{r.name}</option>)}
            </select>
          </>
        )}

        {showNop && (
          <>
            <span className="text-gray-300">→</span>
            <select className="text-sm border border-gray-200 rounded-lg px-3 py-2 bg-white min-w-[130px]" value={entityLevel === 'nop' ? entityId : cascadeNop} onChange={e => handleCascadeNop(e.target.value)} disabled={!cascadeRegional && entityLevel !== 'nop'}>
              <option value="">-- NOP --</option>
              {nops.map(n => <option key={n.id} value={n.id}>{n.name}</option>)}
            </select>
          </>
        )}

        {showTo && (
          <>
            <span className="text-gray-300">→</span>
            <select className="text-sm border border-gray-200 rounded-lg px-3 py-2 bg-white min-w-[130px]" value={entityLevel === 'to' ? entityId : cascadeTo} onChange={e => handleCascadeTo(e.target.value)} disabled={entityLevel === 'to' ? !cascadeNop : !cascadeNop}>
              <option value="">-- TO --</option>
              {tos.map(t => <option key={t.id} value={t.id}>{t.name}</option>)}
            </select>
          </>
        )}

        {showSite && (
          <>
            <span className="text-gray-300">→</span>
            <select className="text-sm border border-gray-200 rounded-lg px-3 py-2 bg-white min-w-[160px] flex-1 max-w-xs" value={entityId} onChange={e => handleSiteSelect(e.target.value)} disabled={!cascadeTo || sitesLoading}>
              <option value="">{sitesLoading ? 'Memuat...' : '-- Pilih Site --'}</option>
              {siteOptions.map(s => <option key={s.id} value={s.id}>{s.name} ({s.id})</option>)}
            </select>
          </>
        )}
      </div>

      {loading && (
        <div className="text-center py-8">
          <div className="animate-spin w-6 h-6 border-2 border-[#1E40AF] border-t-transparent rounded-full mx-auto mb-2" />
          <p className="text-sm text-[#475569]">Memuat...</p>
        </div>
      )}

      {!loading && data && (
        <div>
          {Array.isArray(data) && data.length > 0 ? (
            <NdcDistribution data={data} entityLevel={entityLevel} entityId={entityId} />
          ) : (
            <p className="text-sm text-[#475569] text-center py-8">
              Tidak ada data NDC untuk {entityLevel} "{entityId}"
            </p>
          )}
        </div>
      )}
    </div>
  );
}

export default NdcSiteView;
