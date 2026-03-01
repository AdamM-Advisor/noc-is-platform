import { useState, useEffect, useCallback } from 'react';
import { RefreshCw, BookOpen, AlertTriangle, Grid3x3 } from 'lucide-react';
import NdcTable from '../components/ndc/NdcTable';
import NdcFilter from '../components/ndc/NdcFilter';
import ConfusionMatrix from '../components/ndc/ConfusionMatrix';
import NdcSiteView from '../components/ndc/NdcSiteView';

const TABS = [
  { id: 'browser', label: 'NDC Browser', icon: BookOpen },
  { id: 'confusion', label: 'Confusion Matrix', icon: Grid3x3 },
  { id: 'site', label: 'NDC per Site', icon: AlertTriangle },
];

function NdcBrowserPage() {
  const [activeTab, setActiveTab] = useState('browser');
  const [entries, setEntries] = useState([]);
  const [total, setTotal] = useState(0);
  const [coverage, setCoverage] = useState(0);
  const [categories, setCategories] = useState([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [filters, setFilters] = useState({
    category: '', priority: '', status: '', search: '',
    sort_by: 'total_tickets', sort_dir: 'desc',
  });

  const fetchEntries = useCallback(async () => {
    setLoading(true);
    try {
      const params = new URLSearchParams();
      if (filters.category) params.set('category', filters.category);
      if (filters.priority) params.set('priority', filters.priority);
      if (filters.status) params.set('status', filters.status);
      if (filters.search) params.set('search', filters.search);
      params.set('sort_by', filters.sort_by);
      params.set('sort_dir', filters.sort_dir);
      params.set('limit', '200');
      const res = await fetch(`/api/ndc?${params}`);
      const data = await res.json();
      setEntries(data.entries || []);
      setTotal(data.total || 0);
      setCoverage(data.coverage_pct || 0);
      setCategories(data.categories || []);
    } catch (e) {
      console.error('Failed to fetch NDC entries:', e);
    } finally {
      setLoading(false);
    }
  }, [filters]);

  useEffect(() => { fetchEntries(); }, [fetchEntries]);

  const handleRefresh = async () => {
    setRefreshing(true);
    try {
      await fetch('/api/ndc/refresh', { method: 'POST' });
      const poll = setInterval(async () => {
        const r = await fetch('/api/ndc/refresh-status');
        const d = await r.json();
        if (d.status === 'idle' && d.entries > 0) {
          clearInterval(poll);
          setRefreshing(false);
          fetchEntries();
        }
      }, 3000);
      setTimeout(() => { clearInterval(poll); setRefreshing(false); }, 120000);
    } catch (e) {
      setRefreshing(false);
    }
  };

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-xl font-semibold text-[#0F172A]">NDC Knowledge Base</h1>
          <p className="text-sm text-[#475569] mt-1">
            Ensiklopedia pola gangguan jaringan — auto-generated dari data tiket
          </p>
        </div>
        <button
          onClick={handleRefresh}
          disabled={refreshing}
          className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-white bg-[#1E40AF] rounded-lg hover:bg-[#1E3A8A] disabled:opacity-50 transition-colors"
        >
          <RefreshCw size={16} className={refreshing ? 'animate-spin' : ''} />
          {refreshing ? 'Memproses...' : 'Refresh NDC'}
        </button>
      </div>

      <div className="flex items-center gap-4 mb-4 text-sm">
        <span className="px-3 py-1 rounded-full bg-blue-50 text-[#1E40AF] font-medium border border-blue-100">
          {total} entries
        </span>
        <span className="px-3 py-1 rounded-full bg-gray-50 text-[#475569] border border-gray-200">
          Coverage: {coverage}% tiket
        </span>
      </div>

      <div className="border-b border-gray-200 mb-4">
        <nav className="flex gap-0">
          {TABS.map(tab => {
            const Icon = tab.icon;
            return (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`flex items-center gap-2 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
                  activeTab === tab.id
                    ? 'border-[#1E40AF] text-[#1E40AF]'
                    : 'border-transparent text-[#475569] hover:text-[#0F172A] hover:border-gray-300'
                }`}
              >
                <Icon size={16} />
                {tab.label}
              </button>
            );
          })}
        </nav>
      </div>

      {activeTab === 'browser' && (
        <div>
          <NdcFilter
            filters={filters}
            onChange={setFilters}
            categories={categories}
          />
          <NdcTable
            entries={entries}
            loading={loading}
            sort_by={filters.sort_by}
            sort_dir={filters.sort_dir}
            onSort={(col) => {
              setFilters(f => ({
                ...f,
                sort_by: col,
                sort_dir: f.sort_by === col && f.sort_dir === 'desc' ? 'asc' : 'desc',
              }));
            }}
          />
        </div>
      )}

      {activeTab === 'confusion' && <ConfusionMatrix />}
      {activeTab === 'site' && <NdcSiteView />}
    </div>
  );
}

export default NdcBrowserPage;
