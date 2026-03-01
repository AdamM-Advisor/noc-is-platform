import { useState, useEffect, useCallback } from 'react';
import { RefreshCw, BookOpen, AlertTriangle, Grid3x3, Shield, Clock, ChevronDown, ChevronUp, History, X } from 'lucide-react';
import NdcTable from '../components/ndc/NdcTable';
import NdcFilter from '../components/ndc/NdcFilter';
import ConfusionMatrix from '../components/ndc/ConfusionMatrix';
import NdcSiteView from '../components/ndc/NdcSiteView';

const TABS = [
  { id: 'browser', label: 'NDC Browser', icon: BookOpen },
  { id: 'confusion', label: 'Confusion Matrix', icon: Grid3x3 },
  { id: 'site', label: 'NDC per Site', icon: AlertTriangle },
];

function RefreshConfirmModal({ onConfirm, onCancel, lastRefresh, refreshing }) {
  const [step, setStep] = useState(1);
  const [confirmText, setConfirmText] = useState('');

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-md mx-4">
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200">
          <div className="flex items-center gap-2">
            <Shield size={20} className="text-amber-500" />
            <h3 className="text-lg font-semibold text-[#0F172A]">Konfirmasi Refresh NDC</h3>
          </div>
          <button onClick={onCancel} className="text-gray-400 hover:text-gray-600">
            <X size={20} />
          </button>
        </div>

        {step === 1 && (
          <div className="px-6 py-5">
            <div className="bg-amber-50 border border-amber-200 rounded-lg p-4 mb-4">
              <p className="text-sm font-medium text-amber-800 mb-2">⚠️ Operasi ini akan:</p>
              <ul className="text-sm text-amber-700 space-y-1 ml-4 list-disc">
                <li>Memproses ulang seluruh knowledge base NDC</li>
                <li>Mengupdate semua alarm snapshot, symptoms, dan diagnostic tree</li>
                <li>Memakan waktu beberapa menit</li>
              </ul>
            </div>

            {lastRefresh && lastRefresh.timestamp && (
              <div className="bg-gray-50 border border-gray-200 rounded-lg p-3 mb-4">
                <p className="text-xs text-gray-500 mb-1">Refresh terakhir:</p>
                <p className="text-sm font-medium text-gray-700">
                  {new Date(lastRefresh.timestamp).toLocaleString('id-ID')}
                </p>
                {lastRefresh.entries_affected > 0 && (
                  <p className="text-xs text-gray-500 mt-1">
                    {lastRefresh.entries_affected} entries diproses
                    {lastRefresh.details?.duration_sec && ` dalam ${lastRefresh.details.duration_sec}s`}
                  </p>
                )}
              </div>
            )}

            <div className="flex justify-end gap-3">
              <button onClick={onCancel} className="px-4 py-2 text-sm text-gray-600 hover:text-gray-800 border border-gray-300 rounded-lg">
                Batal
              </button>
              <button onClick={() => setStep(2)} className="px-4 py-2 text-sm font-medium text-white bg-amber-500 hover:bg-amber-600 rounded-lg">
                Lanjutkan
              </button>
            </div>
          </div>
        )}

        {step === 2 && (
          <div className="px-6 py-5">
            <p className="text-sm text-gray-600 mb-3">
              Ketik <span className="font-mono font-bold text-red-600 bg-red-50 px-1.5 py-0.5 rounded">REFRESH</span> untuk mengkonfirmasi:
            </p>
            <input
              type="text"
              value={confirmText}
              onChange={(e) => setConfirmText(e.target.value)}
              placeholder="Ketik REFRESH di sini"
              className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-amber-400 focus:border-amber-400 font-mono"
              autoFocus
            />
            <div className="flex justify-end gap-3 mt-4">
              <button onClick={() => setStep(1)} className="px-4 py-2 text-sm text-gray-600 hover:text-gray-800 border border-gray-300 rounded-lg">
                Kembali
              </button>
              <button
                onClick={onConfirm}
                disabled={confirmText !== 'REFRESH' || refreshing}
                className="px-4 py-2 text-sm font-medium text-white bg-red-600 hover:bg-red-700 rounded-lg disabled:opacity-40 disabled:cursor-not-allowed flex items-center gap-2"
              >
                {refreshing && <RefreshCw size={14} className="animate-spin" />}
                {refreshing ? 'Memproses...' : 'Konfirmasi Refresh'}
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

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
  const [showRefreshModal, setShowRefreshModal] = useState(false);
  const [lastRefresh, setLastRefresh] = useState(null);
  const [changelog, setChangelog] = useState([]);
  const [adminOpen, setAdminOpen] = useState(false);

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

  const fetchLastRefresh = async () => {
    try {
      const res = await fetch('/api/ndc/last-refresh');
      const data = await res.json();
      setLastRefresh(data);
    } catch (e) {}
  };

  const fetchChangelog = async () => {
    try {
      const res = await fetch('/api/ndc/changelog?limit=20');
      const data = await res.json();
      setChangelog(data || []);
    } catch (e) {}
  };

  useEffect(() => { fetchEntries(); }, [fetchEntries]);
  useEffect(() => { fetchLastRefresh(); fetchChangelog(); }, []);

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
          setShowRefreshModal(false);
          fetchEntries();
          fetchLastRefresh();
          fetchChangelog();
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
        {lastRefresh && lastRefresh.timestamp && (
          <div className="flex items-center gap-1.5 text-xs text-gray-400">
            <Clock size={13} />
            <span>Refresh terakhir: {new Date(lastRefresh.timestamp).toLocaleString('id-ID')}</span>
          </div>
        )}
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

      <div className="mt-10 border border-gray-200 rounded-lg">
        <button
          onClick={() => setAdminOpen(!adminOpen)}
          className="w-full flex items-center justify-between px-5 py-3 text-sm font-medium text-gray-500 hover:text-gray-700 hover:bg-gray-50 rounded-lg transition-colors"
        >
          <div className="flex items-center gap-2">
            <Shield size={16} />
            <span>Administrasi Knowledge</span>
          </div>
          {adminOpen ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
        </button>

        {adminOpen && (
          <div className="px-5 pb-5 border-t border-gray-100">
            <div className="flex items-center justify-between mt-4 mb-4">
              <div>
                <p className="text-sm font-medium text-gray-700">Refresh NDC Knowledge Base</p>
                <p className="text-xs text-gray-400 mt-0.5">
                  Regenerasi seluruh knowledge base dari data tiket terbaru
                </p>
              </div>
              <button
                onClick={() => setShowRefreshModal(true)}
                disabled={refreshing}
                className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-white bg-[#1E40AF] rounded-lg hover:bg-[#1E3A8A] disabled:opacity-50 transition-colors"
              >
                <RefreshCw size={16} className={refreshing ? 'animate-spin' : ''} />
                {refreshing ? 'Memproses...' : 'Refresh NDC'}
              </button>
            </div>

            {changelog.length > 0 && (
              <div>
                <div className="flex items-center gap-2 mb-3">
                  <History size={15} className="text-gray-400" />
                  <h4 className="text-sm font-medium text-gray-600">Audit Log</h4>
                </div>
                <div className="border border-gray-200 rounded-lg overflow-hidden">
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="bg-gray-50 text-gray-500">
                        <th className="text-left px-3 py-2 font-medium">Waktu</th>
                        <th className="text-left px-3 py-2 font-medium">Aksi</th>
                        <th className="text-left px-3 py-2 font-medium">Oleh</th>
                        <th className="text-right px-3 py-2 font-medium">Entries</th>
                        <th className="text-left px-3 py-2 font-medium">Detail</th>
                      </tr>
                    </thead>
                    <tbody>
                      {changelog.map(log => (
                        <tr key={log.id} className="border-t border-gray-100 hover:bg-gray-50">
                          <td className="px-3 py-2 text-gray-600 whitespace-nowrap">
                            {log.timestamp ? new Date(log.timestamp).toLocaleString('id-ID') : '-'}
                          </td>
                          <td className="px-3 py-2">
                            <span className={`px-2 py-0.5 rounded-full font-medium ${
                              log.action === 'refresh' ? 'bg-blue-50 text-blue-700' :
                              log.action === 'curate' ? 'bg-green-50 text-green-700' :
                              'bg-gray-100 text-gray-600'
                            }`}>
                              {log.action}
                            </span>
                          </td>
                          <td className="px-3 py-2 text-gray-500">{log.performed_by || '-'}</td>
                          <td className="px-3 py-2 text-right text-gray-600">{log.entries_affected}</td>
                          <td className="px-3 py-2 text-gray-400 max-w-xs truncate">
                            {log.details ? (
                              <span title={JSON.stringify(log.details)}>
                                {log.details.entries_before !== undefined
                                  ? `${log.details.entries_before} → ${log.details.entries_after} entries`
                                  : '-'}
                                {log.details.duration_sec ? ` (${log.details.duration_sec}s)` : ''}
                                {log.details.errors?.length > 0 ? ` · ${log.details.errors.length} errors` : ''}
                              </span>
                            ) : '-'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        )}
      </div>

      {showRefreshModal && (
        <RefreshConfirmModal
          onConfirm={handleRefresh}
          onCancel={() => setShowRefreshModal(false)}
          lastRefresh={lastRefresh}
          refreshing={refreshing}
        />
      )}
    </div>
  );
}

export default NdcBrowserPage;
