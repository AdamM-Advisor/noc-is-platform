import { useState, useCallback } from 'react';
import { Search } from 'lucide-react';
import NdcDistribution from './NdcDistribution';

function NdcSiteView() {
  const [entityLevel, setEntityLevel] = useState('site');
  const [entityId, setEntityId] = useState('');
  const [searchInput, setSearchInput] = useState('');
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);

  const levels = [
    { value: 'site', label: 'Site' },
    { value: 'to', label: 'TO' },
    { value: 'nop', label: 'NOP' },
    { value: 'regional', label: 'Regional' },
  ];

  const doSearch = useCallback(async () => {
    if (!searchInput.trim()) return;
    setLoading(true);
    setEntityId(searchInput.trim());
    try {
      let url;
      if (entityLevel === 'site') {
        url = `/api/ndc/site/${searchInput.trim()}`;
      } else {
        url = `/api/ndc/entity/${entityLevel}/${searchInput.trim()}`;
      }
      const res = await fetch(url);
      const d = await res.json();
      setData(d);
    } catch (e) {
      console.error('Failed to fetch entity NDC:', e);
      setData([]);
    } finally {
      setLoading(false);
    }
  }, [entityLevel, searchInput]);

  return (
    <div>
      <div className="mb-4">
        <h3 className="text-sm font-semibold text-[#0F172A]">NDC Distribution per Entity</h3>
        <p className="text-xs text-[#475569] mt-1">Lihat distribusi NDC untuk site, TO, NOP, atau regional tertentu</p>
      </div>

      <div className="flex items-center gap-3 mb-6">
        <select
          value={entityLevel}
          onChange={e => { setEntityLevel(e.target.value); setData(null); }}
          className="text-sm border border-gray-200 rounded-lg px-3 py-2 bg-white"
        >
          {levels.map(l => <option key={l.value} value={l.value}>{l.label}</option>)}
        </select>
        <div className="relative flex-1 max-w-xs">
          <input
            type="text"
            value={searchInput}
            onChange={e => setSearchInput(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && doSearch()}
            placeholder={`Masukkan ID ${entityLevel}...`}
            className="w-full pl-3 pr-9 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-100 focus:border-[#1E40AF]"
          />
          <button
            onClick={doSearch}
            className="absolute right-2 top-1/2 -translate-y-1/2 text-[#475569] hover:text-[#0F172A]"
          >
            <Search size={16} />
          </button>
        </div>
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
