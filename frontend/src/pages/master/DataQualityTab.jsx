import { useState, useEffect, useCallback, useMemo } from 'react';
import { CheckCircle, AlertTriangle, XCircle, Wrench, X } from 'lucide-react';
import client from '../../api/client';

function SummaryCard({ icon, label, value, sub, warn }) {
  return (
    <div className={`rounded-lg border p-4 bg-white ${warn ? 'border-yellow-300' : 'border-gray-200'}`}>
      <div className="flex items-center gap-2 mb-1">
        {icon}
        <span className="text-xs font-medium text-gray-500 uppercase tracking-wide">{label}</span>
      </div>
      <div className={`text-2xl font-bold ${warn ? 'text-yellow-600' : 'text-[#1B2A4A]'}`}>{value}</div>
      {sub && <div className="text-xs text-gray-400 mt-0.5">{sub}</div>}
    </div>
  );
}

function FixModal({ orphan, onClose, onResolve }) {
  const [mode, setMode] = useState('map');
  const [resolvedTo, setResolvedTo] = useState('');
  const [newId, setNewId] = useState('');
  const [saving, setSaving] = useState(false);

  if (!orphan) return null;

  const handleSubmit = async () => {
    let value = '';
    if (mode === 'map') value = resolvedTo;
    else if (mode === 'create') value = newId;
    else if (mode === 'ignore') value = '__IGNORED__';

    if (!value) return;
    setSaving(true);
    try {
      await onResolve(orphan.id, value);
      onClose();
    } catch (e) {}
    setSaving(false);
  };

  return (
    <div className="fixed inset-0 bg-black/40 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg shadow-xl w-full max-w-md p-6">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold text-gray-800">Perbaiki Orphan</h3>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600"><X size={20} /></button>
        </div>
        <div className="mb-3 text-sm">
          <span className="text-gray-500">Level:</span> <span className="font-medium">{orphan.level}</span>
          <span className="ml-4 text-gray-500">Nilai:</span> <span className="font-medium">{orphan.value}</span>
        </div>
        <div className="space-y-3">
          <div className="flex gap-2">
            <button
              onClick={() => setMode('map')}
              className={`flex-1 py-2 text-sm rounded-md border ${mode === 'map' ? 'bg-[#1B2A4A] text-white border-[#1B2A4A]' : 'bg-white text-gray-600 border-gray-300 hover:bg-gray-50'}`}
            >
              Map ke existing
            </button>
            <button
              onClick={() => setMode('create')}
              className={`flex-1 py-2 text-sm rounded-md border ${mode === 'create' ? 'bg-[#1B2A4A] text-white border-[#1B2A4A]' : 'bg-white text-gray-600 border-gray-300 hover:bg-gray-50'}`}
            >
              Buat baru
            </button>
            <button
              onClick={() => setMode('ignore')}
              className={`flex-1 py-2 text-sm rounded-md border ${mode === 'ignore' ? 'bg-[#1B2A4A] text-white border-[#1B2A4A]' : 'bg-white text-gray-600 border-gray-300 hover:bg-gray-50'}`}
            >
              Abaikan
            </button>
          </div>

          {mode === 'map' && (
            <div>
              <label className="block text-xs text-gray-500 mb-1">Map ke ID existing</label>
              <input
                className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1B2A4A]"
                placeholder={`Masukkan ${orphan.level}_id tujuan`}
                value={resolvedTo}
                onChange={(e) => setResolvedTo(e.target.value)}
              />
              {orphan.suggested_match && (
                <button
                  onClick={() => setResolvedTo(orphan.suggested_match)}
                  className="mt-1 text-xs text-blue-600 hover:underline"
                >
                  Saran: {orphan.suggested_match}
                </button>
              )}
            </div>
          )}

          {mode === 'create' && (
            <div>
              <label className="block text-xs text-gray-500 mb-1">ID baru</label>
              <input
                className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1B2A4A]"
                placeholder="Masukkan ID baru"
                value={newId}
                onChange={(e) => setNewId(e.target.value)}
              />
            </div>
          )}

          {mode === 'ignore' && (
            <p className="text-sm text-gray-500">Orphan ini akan ditandai sebagai diabaikan dan tidak akan muncul lagi.</p>
          )}
        </div>
        <div className="flex justify-end gap-2 mt-6">
          <button onClick={onClose} className="px-4 py-2 text-sm text-gray-600 border border-gray-300 rounded-md hover:bg-gray-50">Batal</button>
          <button
            onClick={handleSubmit}
            disabled={saving || (mode === 'map' && !resolvedTo) || (mode === 'create' && !newId)}
            className="px-4 py-2 text-sm text-white bg-[#1B2A4A] rounded-md hover:bg-[#2a3d66] flex items-center gap-1 disabled:opacity-50"
          >
            <Wrench size={14} /> {saving ? 'Menyimpan...' : 'Selesaikan'}
          </button>
        </div>
      </div>
    </div>
  );
}

function DataQualityTab() {
  const [summary, setSummary] = useState(null);
  const [orphans, setOrphans] = useState({ orphans: {}, total: 0 });
  const [loading, setLoading] = useState(true);
  const [fixingOrphan, setFixingOrphan] = useState(null);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const [sumRes, orphRes] = await Promise.all([
        client.get('/data-quality/summary'),
        client.get('/orphans'),
      ]);
      setSummary(sumRes.data);
      setOrphans(orphRes.data);
    } catch (e) {}
    setLoading(false);
  }, []);

  useEffect(() => { fetchData(); }, [fetchData]);

  const resolveOrphan = async (id, resolvedTo) => {
    await client.put(`/orphans/${id}/resolve`, { resolved_to: resolvedTo });
    await fetchData();
  };

  const hierarchyComplete = useMemo(() => {
    if (!summary?.hierarchy_completeness) return false;
    return Object.values(summary.hierarchy_completeness).every((l) => l.total > 0);
  }, [summary]);

  const totalSites = summary?.enrichment_coverage?.total_sites || 0;

  const uniqueMonths = useMemo(() => {
    if (!summary?.data_coverage) return 0;
    return new Set(summary.data_coverage.map((d) => d.period)).size;
  }, [summary]);

  const coverageMatrix = useMemo(() => {
    if (!summary?.data_coverage) return { months: [], types: [], grid: {} };
    const months = [...new Set(summary.data_coverage.map((d) => d.period))].sort().reverse();
    const types = [...new Set(summary.data_coverage.map((d) => d.file_type))].sort();
    const grid = {};
    summary.data_coverage.forEach((d) => {
      grid[`${d.period}-${d.file_type}`] = d;
    });
    return { months, types, grid };
  }, [summary]);

  const allOrphans = useMemo(() => {
    const list = [];
    Object.entries(orphans.orphans || {}).forEach(([level, items]) => {
      items.forEach((item) => list.push(item));
    });
    return list;
  }, [orphans]);

  if (loading) {
    return <div className="flex items-center justify-center h-40 text-gray-400 text-sm">Memuat data kualitas...</div>;
  }

  if (!summary) {
    return <div className="text-center text-gray-400 text-sm py-10">Gagal memuat data</div>;
  }

  const hc = summary.hierarchy_completeness || {};
  const ac = summary.alias_coverage || {};

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-4 gap-4">
        <SummaryCard
          icon={hierarchyComplete ? <CheckCircle size={18} className="text-green-500" /> : <AlertTriangle size={18} className="text-yellow-500" />}
          label="Hierarki"
          value={hierarchyComplete ? 'Lengkap' : `${Object.values(hc).filter((l) => l.total > 0).length}/${Object.keys(hc).length}`}
          sub={hierarchyComplete ? 'Semua level terisi' : 'Beberapa level kosong'}
        />
        <SummaryCard
          icon={<CheckCircle size={18} className="text-blue-500" />}
          label="Site"
          value={totalSites.toLocaleString()}
          sub="Site aktif"
        />
        <SummaryCard
          icon={orphans.total > 0 ? <AlertTriangle size={18} className="text-yellow-500" /> : <CheckCircle size={18} className="text-green-500" />}
          label="Orphans"
          value={orphans.total}
          sub={orphans.total > 0 ? 'Perlu ditangani' : 'Tidak ada orphan'}
          warn={orphans.total > 0}
        />
        <SummaryCard
          icon={<CheckCircle size={18} className="text-indigo-500" />}
          label="Coverage"
          value={`${uniqueMonths} bulan`}
          sub="Data tersedia"
        />
      </div>

      <div className="border border-gray-200 rounded-lg bg-white overflow-hidden">
        <div className="px-4 py-3 bg-gray-50 border-b border-gray-200">
          <h3 className="text-sm font-semibold text-[#1B2A4A]">Kelengkapan Hierarki</h3>
        </div>
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-50 text-xs text-gray-500 uppercase tracking-wide">
              <th className="px-4 py-2 text-left">Level</th>
              <th className="px-4 py-2 text-right">Total</th>
              <th className="px-4 py-2 text-right">Aktif</th>
              <th className="px-4 py-2 text-right">Nonaktif</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {Object.entries(hc).map(([level, data]) => (
              <tr key={level} className="hover:bg-gray-50">
                <td className="px-4 py-2 font-medium text-gray-800 capitalize">{level}</td>
                <td className="px-4 py-2 text-right text-gray-600">{data.total}</td>
                <td className="px-4 py-2 text-right text-green-600 font-medium">{data.active}</td>
                <td className="px-4 py-2 text-right text-red-500">{data.inactive}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="border border-gray-200 rounded-lg bg-white overflow-hidden">
        <div className="px-4 py-3 bg-gray-50 border-b border-gray-200">
          <h3 className="text-sm font-semibold text-[#1B2A4A]">Cakupan Alias</h3>
        </div>
        <div className="p-4">
          {Object.keys(ac).length === 0 ? (
            <p className="text-sm text-gray-400">Tidak ada data alias</p>
          ) : (
            <div className="grid grid-cols-3 gap-4">
              {Object.entries(ac).map(([level, cols]) => (
                <div key={level} className="border border-gray-100 rounded-lg p-3">
                  <h4 className="text-xs font-semibold text-gray-700 uppercase mb-2">{level}</h4>
                  <div className="space-y-1.5">
                    {Object.entries(cols).map(([col, data]) => (
                      <div key={col} className="flex items-center justify-between text-xs">
                        <span className="text-gray-500 truncate mr-2">{col.replace(`${level}_`, '')}</span>
                        <div className="flex items-center gap-2">
                          <div className="w-20 h-1.5 bg-gray-200 rounded-full overflow-hidden">
                            <div className="h-full bg-[#1B2A4A] rounded-full" style={{ width: `${data.pct}%` }} />
                          </div>
                          <span className="text-gray-600 font-medium w-16 text-right">{data.filled}/{data.total}</span>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      <div className="border border-gray-200 rounded-lg bg-white overflow-hidden">
        <div className="px-4 py-3 bg-gray-50 border-b border-gray-200 flex items-center justify-between">
          <h3 className="text-sm font-semibold text-[#1B2A4A]">Log Orphan</h3>
          {orphans.total > 0 && (
            <span className="text-xs bg-yellow-100 text-yellow-700 px-2 py-0.5 rounded-full font-medium">{orphans.total} belum diselesaikan</span>
          )}
        </div>
        {allOrphans.length === 0 ? (
          <div className="p-6 text-center text-sm text-gray-400 flex flex-col items-center gap-2">
            <CheckCircle size={24} className="text-green-400" />
            Tidak ada orphan yang perlu ditangani
          </div>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50 text-xs text-gray-500 uppercase tracking-wide">
                <th className="px-4 py-2 text-left">Level</th>
                <th className="px-4 py-2 text-left">Nilai</th>
                <th className="px-4 py-2 text-right">Tiket</th>
                <th className="px-4 py-2 text-left">Sumber</th>
                <th className="px-4 py-2 text-center">Aksi</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {allOrphans.map((item) => (
                <tr key={item.id} className="hover:bg-gray-50">
                  <td className="px-4 py-2 capitalize font-medium text-gray-700">{item.level}</td>
                  <td className="px-4 py-2 text-gray-800 font-mono text-xs">{item.value}</td>
                  <td className="px-4 py-2 text-right text-gray-600">{item.ticket_count || 0}</td>
                  <td className="px-4 py-2 text-gray-500 text-xs">{item.source || '-'}</td>
                  <td className="px-4 py-2 text-center">
                    <button
                      onClick={() => setFixingOrphan(item)}
                      className="inline-flex items-center gap-1 px-2.5 py-1 text-xs font-medium text-[#1B2A4A] bg-blue-50 hover:bg-blue-100 rounded-md transition-colors"
                    >
                      <Wrench size={12} /> Fix
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      <div className="border border-gray-200 rounded-lg bg-white overflow-hidden">
        <div className="px-4 py-3 bg-gray-50 border-b border-gray-200">
          <h3 className="text-sm font-semibold text-[#1B2A4A]">Matriks Cakupan Data</h3>
        </div>
        {coverageMatrix.months.length === 0 ? (
          <div className="p-6 text-center text-sm text-gray-400">Belum ada data import</div>
        ) : (
          <div className="overflow-auto p-4">
            <table className="w-full text-xs">
              <thead>
                <tr>
                  <th className="px-3 py-2 text-left text-gray-500 font-medium">Periode</th>
                  {coverageMatrix.types.map((t) => (
                    <th key={t} className="px-3 py-2 text-center text-gray-500 font-medium">{t}</th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {coverageMatrix.months.map((month) => (
                  <tr key={month} className="hover:bg-gray-50">
                    <td className="px-3 py-2 font-medium text-gray-700">{month}</td>
                    {coverageMatrix.types.map((t) => {
                      const entry = coverageMatrix.grid[`${month}-${t}`];
                      return (
                        <td key={t} className="px-3 py-2 text-center">
                          {entry ? (
                            <span className="inline-flex items-center justify-center" title={`${entry.total_rows} baris`}>
                              <CheckCircle size={16} className="text-green-500" />
                            </span>
                          ) : (
                            <span className="inline-flex items-center justify-center">
                              <XCircle size={16} className="text-gray-300" />
                            </span>
                          )}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <FixModal orphan={fixingOrphan} onClose={() => setFixingOrphan(null)} onResolve={resolveOrphan} />
    </div>
  );
}

export default DataQualityTab;
