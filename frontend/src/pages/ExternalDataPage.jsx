import { useState, useEffect, useCallback, useRef } from 'react';
import client from '../api/client';
import {
  Cloud, Zap, CalendarDays, MessageSquare, TrendingUp,
  Upload, Download, RefreshCw, Plus, Edit2, Trash2,
  AlertTriangle, Check, Info, X
} from 'lucide-react';

const TABS = [
  { key: 'cuaca', label: 'Cuaca', icon: Cloud },
  { key: 'pln', label: 'PLN', icon: Zap },
  { key: 'kalender', label: 'Kalender', icon: CalendarDays },
  { key: 'anotasi', label: 'Anotasi', icon: MessageSquare },
  { key: 'korelasi', label: 'Korelasi', icon: TrendingUp },
];

const SEVERITY_COLORS = {
  info: 'bg-blue-100 text-blue-800',
  warning: 'bg-yellow-100 text-yellow-800',
  critical: 'bg-red-100 text-red-800',
};

const CORRELATION_COLORS = {
  'sangat kuat': 'text-green-600',
  'kuat': 'text-blue-600',
  'moderat': 'text-yellow-600',
  'lemah': 'text-orange-600',
  'sangat lemah': 'text-red-600',
};

function getCorrelationColor(interpretation) {
  if (!interpretation) return 'text-gray-600';
  const lower = interpretation.toLowerCase();
  for (const [key, val] of Object.entries(CORRELATION_COLORS)) {
    if (lower.includes(key)) return val;
  }
  return 'text-gray-600';
}

function CuacaTab() {
  const [data, setData] = useState([]);
  const [summary, setSummary] = useState(null);
  const [loading, setLoading] = useState(true);
  const [uploadResult, setUploadResult] = useState(null);
  const [uploading, setUploading] = useState(false);
  const fileRef = useRef();

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const res = await client.get('/external/weather');
      setData(res.data.data || []);
      setSummary(res.data.summary || null);
    } catch {
      setData([]);
      setSummary(null);
    }
    setLoading(false);
  }, []);

  useEffect(() => { fetchData(); }, [fetchData]);

  const handleUpload = async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    setUploading(true);
    setUploadResult(null);
    const form = new FormData();
    form.append('file', file);
    try {
      const res = await client.post('/external/weather/upload', form, {
        headers: { 'Content-Type': 'multipart/form-data' },
      });
      setUploadResult(res.data);
      fetchData();
    } catch {
      setUploadResult({ error: 'Upload gagal' });
    }
    setUploading(false);
    if (fileRef.current) fileRef.current.value = '';
  };

  return (
    <div className="space-y-4">
      <h3 className="text-lg font-bold text-[#1B2A4A]">Data Cuaca (BMKG)</h3>

      {summary && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <div className="bg-white rounded-lg border p-3">
            <p className="text-xs text-gray-500">Total Records</p>
            <p className="text-xl font-bold text-[#1B2A4A]">{summary.total_records ?? 0}</p>
          </div>
          <div className="bg-white rounded-lg border p-3">
            <p className="text-xs text-gray-500">Provinsi</p>
            <p className="text-xl font-bold text-[#1B2A4A]">{summary.provinces ?? 0}</p>
          </div>
          <div className="bg-white rounded-lg border p-3">
            <p className="text-xs text-gray-500">Rentang Tanggal</p>
            <p className="text-sm font-semibold text-[#1B2A4A]">{summary.date_range || '-'}</p>
          </div>
          <div className="bg-white rounded-lg border p-3">
            <p className="text-xs text-gray-500">Hari Ekstrem</p>
            <p className="text-xl font-bold text-red-600">{summary.extreme_days ?? 0}</p>
          </div>
        </div>
      )}

      <div className="flex gap-3 items-center">
        <label className="inline-flex items-center gap-2 px-4 py-2 bg-[#1B2A4A] text-white rounded-lg cursor-pointer hover:bg-[#2a3d6b] text-sm font-medium">
          <Upload size={16} />
          {uploading ? 'Uploading...' : 'Upload Data Cuaca CSV'}
          <input type="file" accept=".csv" className="hidden" ref={fileRef} onChange={handleUpload} disabled={uploading} />
        </label>
        <a href="/api/external/weather/template" className="inline-flex items-center gap-2 px-4 py-2 border border-gray-300 rounded-lg text-sm font-medium text-gray-700 hover:bg-gray-50">
          <Download size={16} /> Download Template
        </a>
      </div>

      {uploadResult && !uploadResult.error && (
        <div className="bg-green-50 border border-green-200 rounded-lg p-3 flex items-start gap-2">
          <Check size={16} className="text-green-600 mt-0.5" />
          <div className="text-sm text-green-800">
            <p>Import berhasil: <strong>{uploadResult.imported}</strong> record</p>
            <p>Hari ekstrem: <strong>{uploadResult.extreme_days}</strong> | Anotasi dibuat: <strong>{uploadResult.annotations_created}</strong></p>
          </div>
        </div>
      )}
      {uploadResult?.error && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-3 text-sm text-red-800 flex items-center gap-2">
          <AlertTriangle size={16} /> {uploadResult.error}
        </div>
      )}

      {summary?.extreme_days > 0 && (
        <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-3 text-sm text-yellow-800 flex items-center gap-2">
          <AlertTriangle size={16} /> Terdapat {summary.extreme_days} hari dengan cuaca ekstrem. Anotasi otomatis telah dibuat.
        </div>
      )}

      <div className="bg-white rounded-lg border overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-50 border-b">
              <th className="text-left px-4 py-2.5 font-medium text-gray-600">Tanggal</th>
              <th className="text-left px-4 py-2.5 font-medium text-gray-600">Provinsi</th>
              <th className="text-left px-4 py-2.5 font-medium text-gray-600">Kota</th>
              <th className="text-left px-4 py-2.5 font-medium text-gray-600">Curah Hujan (mm)</th>
              <th className="text-left px-4 py-2.5 font-medium text-gray-600">Suhu Rata² (°C)</th>
              <th className="text-left px-4 py-2.5 font-medium text-gray-600">Kondisi</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr><td colSpan={6} className="px-4 py-8 text-center text-gray-400">Memuat data...</td></tr>
            ) : data.length === 0 ? (
              <tr><td colSpan={6} className="px-4 py-8 text-center text-gray-400">Belum ada data cuaca</td></tr>
            ) : data.map((r, i) => (
              <tr key={i} className="border-b hover:bg-gray-50">
                <td className="px-4 py-2">{r.date}</td>
                <td className="px-4 py-2">{r.province}</td>
                <td className="px-4 py-2">{r.city}</td>
                <td className="px-4 py-2">
                  <span className="flex items-center gap-1">
                    {r.rainfall_mm}
                    {r.rainfall_mm > 50 && <Cloud size={14} className="text-blue-500" />}
                  </span>
                </td>
                <td className="px-4 py-2">{r.temperature_avg_c}</td>
                <td className="px-4 py-2">{r.weather_condition}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function PlnTab() {
  const [data, setData] = useState([]);
  const [summary, setSummary] = useState(null);
  const [loading, setLoading] = useState(true);
  const [uploadResult, setUploadResult] = useState(null);
  const [uploading, setUploading] = useState(false);
  const fileRef = useRef();

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const res = await client.get('/external/pln');
      setData(res.data.data || []);
      setSummary(res.data.summary || null);
    } catch {
      setData([]);
      setSummary(null);
    }
    setLoading(false);
  }, []);

  useEffect(() => { fetchData(); }, [fetchData]);

  const handleUpload = async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    setUploading(true);
    setUploadResult(null);
    const form = new FormData();
    form.append('file', file);
    try {
      const res = await client.post('/external/pln/upload', form, {
        headers: { 'Content-Type': 'multipart/form-data' },
      });
      setUploadResult(res.data);
      fetchData();
    } catch {
      setUploadResult({ error: 'Upload gagal' });
    }
    setUploading(false);
    if (fileRef.current) fileRef.current.value = '';
  };

  return (
    <div className="space-y-4">
      <h3 className="text-lg font-bold text-[#1B2A4A]">Data Gangguan PLN</h3>

      {summary && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <div className="bg-white rounded-lg border p-3">
            <p className="text-xs text-gray-500">Total Records</p>
            <p className="text-xl font-bold text-[#1B2A4A]">{summary.total_records ?? 0}</p>
          </div>
          <div className="bg-white rounded-lg border p-3">
            <p className="text-xs text-gray-500">Provinsi</p>
            <p className="text-xl font-bold text-[#1B2A4A]">{summary.provinces ?? 0}</p>
          </div>
          <div className="bg-white rounded-lg border p-3">
            <p className="text-xs text-gray-500">Rentang Tanggal</p>
            <p className="text-sm font-semibold text-[#1B2A4A]">{summary.date_range || '-'}</p>
          </div>
          <div className="bg-white rounded-lg border p-3">
            <p className="text-xs text-gray-500">Total Gangguan</p>
            <p className="text-xl font-bold text-red-600">{summary.total_outages ?? 0}</p>
          </div>
        </div>
      )}

      <div className="flex gap-3 items-center">
        <label className="inline-flex items-center gap-2 px-4 py-2 bg-[#1B2A4A] text-white rounded-lg cursor-pointer hover:bg-[#2a3d6b] text-sm font-medium">
          <Upload size={16} />
          {uploading ? 'Uploading...' : 'Upload Data PLN CSV'}
          <input type="file" accept=".csv" className="hidden" ref={fileRef} onChange={handleUpload} disabled={uploading} />
        </label>
        <a href="/api/external/pln/template" className="inline-flex items-center gap-2 px-4 py-2 border border-gray-300 rounded-lg text-sm font-medium text-gray-700 hover:bg-gray-50">
          <Download size={16} /> Download Template
        </a>
      </div>

      {uploadResult && !uploadResult.error && (
        <div className="bg-green-50 border border-green-200 rounded-lg p-3 flex items-start gap-2">
          <Check size={16} className="text-green-600 mt-0.5" />
          <div className="text-sm text-green-800">
            <p>Import berhasil: <strong>{uploadResult.imported}</strong> record</p>
            <p>Anotasi dibuat: <strong>{uploadResult.annotations_created}</strong></p>
          </div>
        </div>
      )}
      {uploadResult?.error && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-3 text-sm text-red-800 flex items-center gap-2">
          <AlertTriangle size={16} /> {uploadResult.error}
        </div>
      )}

      <div className="bg-white rounded-lg border overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-50 border-b">
              <th className="text-left px-4 py-2.5 font-medium text-gray-600">Tanggal</th>
              <th className="text-left px-4 py-2.5 font-medium text-gray-600">Provinsi</th>
              <th className="text-left px-4 py-2.5 font-medium text-gray-600">Kota</th>
              <th className="text-left px-4 py-2.5 font-medium text-gray-600">Tipe Gangguan</th>
              <th className="text-left px-4 py-2.5 font-medium text-gray-600">Durasi (jam)</th>
              <th className="text-left px-4 py-2.5 font-medium text-gray-600">Area Terdampak</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr><td colSpan={6} className="px-4 py-8 text-center text-gray-400">Memuat data...</td></tr>
            ) : data.length === 0 ? (
              <tr><td colSpan={6} className="px-4 py-8 text-center text-gray-400">Belum ada data PLN</td></tr>
            ) : data.map((r, i) => (
              <tr key={i} className="border-b hover:bg-gray-50">
                <td className="px-4 py-2">{r.date}</td>
                <td className="px-4 py-2">{r.province}</td>
                <td className="px-4 py-2">{r.city}</td>
                <td className="px-4 py-2">{r.outage_type}</td>
                <td className="px-4 py-2">{r.duration_hours}</td>
                <td className="px-4 py-2">{r.affected_area}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function KalenderTab() {
  const [year, setYear] = useState(2025);
  const [holidays, setHolidays] = useState([]);
  const [loading, setLoading] = useState(true);
  const [generating, setGenerating] = useState(false);
  const [editRow, setEditRow] = useState(null);
  const [editName, setEditName] = useState('');
  const [editCuti, setEditCuti] = useState(false);
  const [showAdd, setShowAdd] = useState(false);
  const [addForm, setAddForm] = useState({ date: '', name: '', is_cuti_bersama: false });
  const [ramadan, setRamadan] = useState(null);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const [holRes, ramRes] = await Promise.all([
        client.get(`/external/calendar?year=${year}&type=holiday`),
        client.get(`/external/calendar?year=${year}&type=ramadan`),
      ]);
      const calData = Array.isArray(holRes.data) ? holRes.data : [];
      setHolidays(calData);
      const ramadanDates = Array.isArray(ramRes.data) ? ramRes.data : [];
      if (ramadanDates.length > 0) {
        setRamadan({ start: ramadanDates[0].date, end: ramadanDates[ramadanDates.length - 1].date });
      } else {
        setRamadan(null);
      }
    } catch {
      setHolidays([]);
    }
    setLoading(false);
  }, [year]);

  useEffect(() => { fetchData(); }, [fetchData]);

  const handleGenerate = async () => {
    setGenerating(true);
    try {
      await client.post('/external/calendar/generate', { year });
      fetchData();
    } catch {}
    setGenerating(false);
  };

  const handleEdit = (h) => {
    setEditRow(h.date);
    setEditName(h.holiday_name || h.name || '');
    setEditCuti(h.is_cuti_bersama || false);
  };

  const handleSaveEdit = async () => {
    try {
      await client.put('/external/calendar/holiday', {
        date: editRow,
        holiday_name: editName,
        is_cuti_bersama: editCuti,
      });
      setEditRow(null);
      fetchData();
    } catch {}
  };

  const handleDelete = async (date) => {
    if (!confirm('Hapus hari libur ini?')) return;
    try {
      await client.delete(`/external/calendar/holiday/${date}`);
      fetchData();
    } catch {}
  };

  const handleAdd = async () => {
    try {
      await client.put('/external/calendar/holiday', {
        date: addForm.date,
        holiday_name: addForm.name,
        is_cuti_bersama: addForm.is_cuti_bersama,
      });
      setShowAdd(false);
      setAddForm({ date: '', name: '', is_cuti_bersama: false });
      fetchData();
    } catch {}
  };

  return (
    <div className="space-y-4">
      <h3 className="text-lg font-bold text-[#1B2A4A]">Kalender Indonesia</h3>

      <div className="flex gap-3 items-center flex-wrap">
        <select value={year} onChange={(e) => setYear(Number(e.target.value))} className="border rounded-lg px-3 py-2 text-sm">
          <option value={2025}>2025</option>
          <option value={2026}>2026</option>
        </select>
        <button onClick={handleGenerate} disabled={generating} className="inline-flex items-center gap-2 px-4 py-2 bg-[#1B2A4A] text-white rounded-lg text-sm font-medium hover:bg-[#2a3d6b] disabled:opacity-50">
          <RefreshCw size={16} className={generating ? 'animate-spin' : ''} />
          {generating ? 'Generating...' : 'Generate Kalender'}
        </button>
        <button onClick={() => setShowAdd(true)} className="inline-flex items-center gap-2 px-4 py-2 border border-gray-300 rounded-lg text-sm font-medium text-gray-700 hover:bg-gray-50">
          <Plus size={16} /> Tambah Hari Libur
        </button>
      </div>

      {ramadan && (
        <div className="bg-emerald-50 border border-emerald-200 rounded-lg p-3 text-sm text-emerald-800 flex items-center gap-2">
          <CalendarDays size={16} /> Ramadan {ramadan.year || year}: {ramadan.start} - {ramadan.end}
        </div>
      )}

      {showAdd && (
        <div className="bg-white rounded-lg border p-4 space-y-3">
          <h4 className="font-semibold text-sm text-[#1B2A4A]">Tambah Hari Libur</h4>
          <div className="flex gap-3 flex-wrap items-end">
            <div>
              <label className="block text-xs text-gray-500 mb-1">Tanggal</label>
              <input type="date" value={addForm.date} onChange={(e) => setAddForm({ ...addForm, date: e.target.value })} className="border rounded-lg px-3 py-1.5 text-sm" />
            </div>
            <div>
              <label className="block text-xs text-gray-500 mb-1">Nama</label>
              <input type="text" value={addForm.name} onChange={(e) => setAddForm({ ...addForm, name: e.target.value })} className="border rounded-lg px-3 py-1.5 text-sm" />
            </div>
            <label className="flex items-center gap-2 text-sm">
              <input type="checkbox" checked={addForm.is_cuti_bersama} onChange={(e) => setAddForm({ ...addForm, is_cuti_bersama: e.target.checked })} />
              Cuti Bersama
            </label>
            <button onClick={handleAdd} className="px-4 py-1.5 bg-[#1B2A4A] text-white rounded-lg text-sm font-medium hover:bg-[#2a3d6b]">Simpan</button>
            <button onClick={() => setShowAdd(false)} className="px-4 py-1.5 border rounded-lg text-sm text-gray-600 hover:bg-gray-50">Batal</button>
          </div>
        </div>
      )}

      <div className="bg-white rounded-lg border overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-50 border-b">
              <th className="text-left px-4 py-2.5 font-medium text-gray-600">Tanggal</th>
              <th className="text-left px-4 py-2.5 font-medium text-gray-600">Nama</th>
              <th className="text-left px-4 py-2.5 font-medium text-gray-600">Tipe</th>
              <th className="text-left px-4 py-2.5 font-medium text-gray-600">Aksi</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr><td colSpan={4} className="px-4 py-8 text-center text-gray-400">Memuat data...</td></tr>
            ) : holidays.length === 0 ? (
              <tr><td colSpan={4} className="px-4 py-8 text-center text-gray-400">Belum ada data kalender. Klik "Generate Kalender".</td></tr>
            ) : holidays.map((h, i) => (
              <tr key={i} className="border-b hover:bg-gray-50">
                <td className="px-4 py-2">{h.date}</td>
                <td className="px-4 py-2">
                  {editRow === h.date ? (
                    <input type="text" value={editName} onChange={(e) => setEditName(e.target.value)} className="border rounded px-2 py-1 text-sm w-full" />
                  ) : (h.holiday_name || h.name)}
                </td>
                <td className="px-4 py-2">
                  {editRow === h.date ? (
                    <label className="flex items-center gap-1 text-sm">
                      <input type="checkbox" checked={editCuti} onChange={(e) => setEditCuti(e.target.checked)} /> Cuti
                    </label>
                  ) : (
                    <span className={`px-2 py-0.5 rounded text-xs font-medium ${h.is_cuti_bersama ? 'bg-orange-100 text-orange-700' : 'bg-red-100 text-red-700'}`}>
                      {h.is_cuti_bersama ? 'Cuti' : 'Libur'}
                    </span>
                  )}
                </td>
                <td className="px-4 py-2">
                  {editRow === h.date ? (
                    <div className="flex gap-1">
                      <button onClick={handleSaveEdit} className="p-1 text-green-600 hover:bg-green-50 rounded"><Check size={16} /></button>
                      <button onClick={() => setEditRow(null)} className="p-1 text-gray-400 hover:bg-gray-100 rounded"><X size={16} /></button>
                    </div>
                  ) : (
                    <div className="flex gap-1">
                      <button onClick={() => handleEdit(h)} className="p-1 text-blue-600 hover:bg-blue-50 rounded"><Edit2 size={16} /></button>
                      <button onClick={() => handleDelete(h.date)} className="p-1 text-red-600 hover:bg-red-50 rounded"><Trash2 size={16} /></button>
                    </div>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function AnotasiTab() {
  const [annotations, setAnnotations] = useState([]);
  const [loading, setLoading] = useState(true);
  const [filterType, setFilterType] = useState('');
  const [filterFrom, setFilterFrom] = useState('');
  const [filterTo, setFilterTo] = useState('');
  const [showForm, setShowForm] = useState(false);
  const [editItem, setEditItem] = useState(null);
  const [form, setForm] = useState({
    date: '', date_end: '', area_id: '', annotation_type: 'custom',
    title: '', description: '', severity: 'info', icon: '',
  });

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const params = {};
      if (filterType) params.type = filterType;
      if (filterFrom) params.from = filterFrom;
      if (filterTo) params.to = filterTo;
      const res = await client.get('/external/annotations', { params });
      setAnnotations(res.data.data || res.data || []);
    } catch {
      setAnnotations([]);
    }
    setLoading(false);
  }, [filterType, filterFrom, filterTo]);

  useEffect(() => { fetchData(); }, [fetchData]);

  const typeIcons = {
    weather: <Cloud size={14} className="text-blue-500" />,
    pln: <Zap size={14} className="text-yellow-500" />,
    holiday: <CalendarDays size={14} className="text-green-500" />,
    custom: <MessageSquare size={14} className="text-purple-500" />,
    incident: <AlertTriangle size={14} className="text-red-500" />,
  };

  const resetForm = () => {
    setForm({ date: '', date_end: '', area_id: '', annotation_type: 'custom', title: '', description: '', severity: 'info', icon: '' });
    setEditItem(null);
    setShowForm(false);
  };

  const handleSubmit = async () => {
    try {
      const payload = { ...form };
      if (!payload.date_end) delete payload.date_end;
      if (!payload.area_id) delete payload.area_id;
      if (!payload.icon) delete payload.icon;

      if (editItem) {
        await client.put(`/external/annotation/${editItem.id}`, payload);
      } else {
        await client.post('/external/annotation', payload);
      }
      resetForm();
      fetchData();
    } catch {}
  };

  const handleEdit = (a) => {
    setForm({
      date: a.date || '',
      date_end: a.date_end || '',
      area_id: a.area_id || '',
      annotation_type: a.annotation_type || a.type || 'custom',
      title: a.title || '',
      description: a.description || '',
      severity: a.severity || 'info',
      icon: a.icon || '',
    });
    setEditItem(a);
    setShowForm(true);
  };

  const handleDelete = async (id) => {
    if (!confirm('Hapus anotasi ini?')) return;
    try {
      await client.delete(`/external/annotation/${id}`);
      fetchData();
    } catch {}
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-lg font-bold text-[#1B2A4A]">Anotasi Trend</h3>
        <button onClick={() => { resetForm(); setShowForm(true); }} className="inline-flex items-center gap-2 px-4 py-2 bg-[#1B2A4A] text-white rounded-lg text-sm font-medium hover:bg-[#2a3d6b]">
          <Plus size={16} /> Tambah Manual
        </button>
      </div>

      {showForm && (
        <div className="bg-white rounded-lg border p-4 space-y-3">
          <h4 className="font-semibold text-sm text-[#1B2A4A]">{editItem ? 'Edit Anotasi' : 'Tambah Anotasi'}</h4>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
            <div>
              <label className="block text-xs text-gray-500 mb-1">Tanggal</label>
              <input type="date" value={form.date} onChange={(e) => setForm({ ...form, date: e.target.value })} className="border rounded-lg px-3 py-1.5 text-sm w-full" />
            </div>
            <div>
              <label className="block text-xs text-gray-500 mb-1">Tanggal Akhir (opsional)</label>
              <input type="date" value={form.date_end} onChange={(e) => setForm({ ...form, date_end: e.target.value })} className="border rounded-lg px-3 py-1.5 text-sm w-full" />
            </div>
            <div>
              <label className="block text-xs text-gray-500 mb-1">Area ID (opsional)</label>
              <input type="text" value={form.area_id} onChange={(e) => setForm({ ...form, area_id: e.target.value })} className="border rounded-lg px-3 py-1.5 text-sm w-full" />
            </div>
            <div>
              <label className="block text-xs text-gray-500 mb-1">Tipe</label>
              <select value={form.annotation_type} onChange={(e) => setForm({ ...form, annotation_type: e.target.value })} className="border rounded-lg px-3 py-1.5 text-sm w-full">
                <option value="custom">Custom</option>
                <option value="weather">Weather</option>
                <option value="pln">PLN</option>
                <option value="holiday">Holiday</option>
                <option value="incident">Incident</option>
              </select>
            </div>
            <div>
              <label className="block text-xs text-gray-500 mb-1">Severity</label>
              <select value={form.severity} onChange={(e) => setForm({ ...form, severity: e.target.value })} className="border rounded-lg px-3 py-1.5 text-sm w-full">
                <option value="info">Info</option>
                <option value="warning">Warning</option>
                <option value="critical">Critical</option>
              </select>
            </div>
            <div>
              <label className="block text-xs text-gray-500 mb-1">Icon (opsional)</label>
              <input type="text" value={form.icon} onChange={(e) => setForm({ ...form, icon: e.target.value })} className="border rounded-lg px-3 py-1.5 text-sm w-full" placeholder="emoji atau nama icon" />
            </div>
            <div className="md:col-span-2">
              <label className="block text-xs text-gray-500 mb-1">Judul</label>
              <input type="text" value={form.title} onChange={(e) => setForm({ ...form, title: e.target.value })} className="border rounded-lg px-3 py-1.5 text-sm w-full" />
            </div>
            <div className="md:col-span-3">
              <label className="block text-xs text-gray-500 mb-1">Deskripsi</label>
              <textarea value={form.description} onChange={(e) => setForm({ ...form, description: e.target.value })} className="border rounded-lg px-3 py-1.5 text-sm w-full" rows={2} />
            </div>
          </div>
          <div className="flex gap-2">
            <button onClick={handleSubmit} className="px-4 py-1.5 bg-[#1B2A4A] text-white rounded-lg text-sm font-medium hover:bg-[#2a3d6b]">
              {editItem ? 'Simpan Perubahan' : 'Simpan'}
            </button>
            <button onClick={resetForm} className="px-4 py-1.5 border rounded-lg text-sm text-gray-600 hover:bg-gray-50">Batal</button>
          </div>
        </div>
      )}

      <div className="flex gap-3 flex-wrap items-end">
        <div>
          <label className="block text-xs text-gray-500 mb-1">Tipe</label>
          <select value={filterType} onChange={(e) => setFilterType(e.target.value)} className="border rounded-lg px-3 py-1.5 text-sm">
            <option value="">Semua</option>
            <option value="weather">Weather</option>
            <option value="pln">PLN</option>
            <option value="holiday">Holiday</option>
            <option value="custom">Custom</option>
          </select>
        </div>
        <div>
          <label className="block text-xs text-gray-500 mb-1">Dari</label>
          <input type="date" value={filterFrom} onChange={(e) => setFilterFrom(e.target.value)} className="border rounded-lg px-3 py-1.5 text-sm" />
        </div>
        <div>
          <label className="block text-xs text-gray-500 mb-1">Sampai</label>
          <input type="date" value={filterTo} onChange={(e) => setFilterTo(e.target.value)} className="border rounded-lg px-3 py-1.5 text-sm" />
        </div>
      </div>

      <div className="bg-white rounded-lg border overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-50 border-b">
              <th className="text-left px-4 py-2.5 font-medium text-gray-600">Tanggal</th>
              <th className="text-left px-4 py-2.5 font-medium text-gray-600">Tipe</th>
              <th className="text-left px-4 py-2.5 font-medium text-gray-600">Title</th>
              <th className="text-left px-4 py-2.5 font-medium text-gray-600">Severity</th>
              <th className="text-left px-4 py-2.5 font-medium text-gray-600">Source</th>
              <th className="text-left px-4 py-2.5 font-medium text-gray-600">Aksi</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr><td colSpan={6} className="px-4 py-8 text-center text-gray-400">Memuat data...</td></tr>
            ) : annotations.length === 0 ? (
              <tr><td colSpan={6} className="px-4 py-8 text-center text-gray-400">Belum ada anotasi</td></tr>
            ) : annotations.map((a, i) => (
              <tr key={i} className="border-b hover:bg-gray-50">
                <td className="px-4 py-2">{a.date}</td>
                <td className="px-4 py-2">
                  <span className="flex items-center gap-1">
                    {typeIcons[a.annotation_type || a.type] || typeIcons.custom}
                    {a.annotation_type || a.type}
                  </span>
                </td>
                <td className="px-4 py-2">{a.title}</td>
                <td className="px-4 py-2">
                  <span className={`px-2 py-0.5 rounded text-xs font-medium ${SEVERITY_COLORS[a.severity] || SEVERITY_COLORS.info}`}>
                    {a.severity}
                  </span>
                </td>
                <td className="px-4 py-2 text-gray-500">{a.source || (a.is_auto ? 'auto' : 'manual')}</td>
                <td className="px-4 py-2">
                  {(a.source === 'manual' || !a.is_auto) && (
                    <div className="flex gap-1">
                      <button onClick={() => handleEdit(a)} className="p-1 text-blue-600 hover:bg-blue-50 rounded"><Edit2 size={16} /></button>
                      <button onClick={() => handleDelete(a.id)} className="p-1 text-red-600 hover:bg-red-50 rounded"><Trash2 size={16} /></button>
                    </div>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="bg-blue-50 border border-blue-200 rounded-lg p-3 text-sm text-blue-800 flex items-center gap-2">
        <Info size={16} /> Anotasi ditampilkan sebagai marker pada chart trend di halaman Profiler
      </div>
    </div>
  );
}

function KorelasiTab() {
  const [from, setFrom] = useState('');
  const [to, setTo] = useState('');
  const [province, setProvince] = useState('');
  const [weatherCorr, setWeatherCorr] = useState(null);
  const [plnCorr, setPlnCorr] = useState(null);
  const [calCorr, setCalCorr] = useState(null);
  const [loading, setLoading] = useState(false);

  const handleCalculate = async () => {
    setLoading(true);
    const params = {};
    if (from) params.from = from;
    if (to) params.to = to;
    if (province) params.province = province;

    const [w, p, c] = await Promise.allSettled([
      client.get('/external/correlation/weather', { params }),
      client.get('/external/correlation/pln', { params }),
      client.get('/external/correlation/calendar', { params }),
    ]);

    setWeatherCorr(w.status === 'fulfilled' ? w.value.data : { error: 'no_data' });
    setPlnCorr(p.status === 'fulfilled' ? p.value.data : { error: 'no_data' });
    setCalCorr(c.status === 'fulfilled' ? c.value.data : { error: 'no_data' });
    setLoading(false);
  };

  const renderCorrelationValue = (value, interpretation) => {
    if (value === null || value === undefined) return <span className="text-gray-400">-</span>;
    const color = getCorrelationColor(interpretation);
    return (
      <div>
        <span className="text-lg font-bold">{typeof value === 'number' ? value.toFixed(3) : value}</span>
        {interpretation && <span className={`ml-2 text-sm font-medium ${color}`}>{interpretation}</span>}
      </div>
    );
  };

  return (
    <div className="space-y-4">
      <h3 className="text-lg font-bold text-[#1B2A4A]">Analisis Korelasi</h3>

      <div className="flex gap-3 flex-wrap items-end">
        <div>
          <label className="block text-xs text-gray-500 mb-1">Periode Dari</label>
          <input type="date" value={from} onChange={(e) => setFrom(e.target.value)} className="border rounded-lg px-3 py-1.5 text-sm" />
        </div>
        <div>
          <label className="block text-xs text-gray-500 mb-1">Periode Sampai</label>
          <input type="date" value={to} onChange={(e) => setTo(e.target.value)} className="border rounded-lg px-3 py-1.5 text-sm" />
        </div>
        <div>
          <label className="block text-xs text-gray-500 mb-1">Provinsi (opsional)</label>
          <input type="text" value={province} onChange={(e) => setProvince(e.target.value)} className="border rounded-lg px-3 py-1.5 text-sm" placeholder="Semua" />
        </div>
        <button onClick={handleCalculate} disabled={loading} className="inline-flex items-center gap-2 px-4 py-2 bg-[#1B2A4A] text-white rounded-lg text-sm font-medium hover:bg-[#2a3d6b] disabled:opacity-50">
          <TrendingUp size={16} className={loading ? 'animate-pulse' : ''} />
          {loading ? 'Menghitung...' : 'Hitung'}
        </button>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="bg-white rounded-lg border p-4 space-y-3">
          <div className="flex items-center gap-2 text-[#1B2A4A] font-semibold">
            <Cloud size={18} /> Cuaca x Gangguan
          </div>
          {!weatherCorr ? (
            <p className="text-sm text-gray-400">Klik "Hitung" untuk melihat hasil</p>
          ) : weatherCorr.error ? (
            <div className="bg-yellow-50 border border-yellow-200 rounded p-2 text-sm text-yellow-800 flex items-center gap-2">
              <AlertTriangle size={14} /> Upload data cuaca terlebih dahulu di tab Cuaca
            </div>
          ) : (
            <div className="space-y-2 text-sm">
              <div>
                <p className="text-gray-500">Curah Hujan vs Tiket</p>
                {renderCorrelationValue(weatherCorr.correlation_rainfall_vs_tickets, weatherCorr.interpretation)}
              </div>
              <div>
                <p className="text-gray-500">Cuaca Ekstrem vs Tiket</p>
                {renderCorrelationValue(weatherCorr.correlation_extreme_vs_tickets, null)}
              </div>
              {weatherCorr.extreme_impact && (
                <div className="bg-gray-50 rounded p-2 text-xs">
                  <p>Rata-rata tiket normal: <strong>{weatherCorr.extreme_impact.avg_tickets_normal}</strong></p>
                  <p>Rata-rata tiket ekstrem: <strong>{weatherCorr.extreme_impact.avg_tickets_extreme}</strong></p>
                  <p>Kenaikan: <strong className="text-red-600">{weatherCorr.extreme_impact.increase_pct}%</strong></p>
                </div>
              )}
            </div>
          )}
        </div>

        <div className="bg-white rounded-lg border p-4 space-y-3">
          <div className="flex items-center gap-2 text-[#1B2A4A] font-semibold">
            <Zap size={18} /> PLN x Gangguan
          </div>
          {!plnCorr ? (
            <p className="text-sm text-gray-400">Klik "Hitung" untuk melihat hasil</p>
          ) : plnCorr.error ? (
            <div className="bg-yellow-50 border border-yellow-200 rounded p-2 text-sm text-yellow-800 flex items-center gap-2">
              <AlertTriangle size={14} /> Upload data PLN terlebih dahulu di tab PLN
            </div>
          ) : (
            <div className="space-y-2 text-sm">
              <div>
                <p className="text-gray-500">Korelasi PLN vs Tiket</p>
                {renderCorrelationValue(plnCorr.correlation_pln_vs_tickets, plnCorr.interpretation)}
              </div>
              {plnCorr.pln_impact && (
                <div className="bg-gray-50 rounded p-2 text-xs">
                  <p>Rata-rata tiket tanpa PLN: <strong>{plnCorr.pln_impact.avg_tickets_no_pln}</strong></p>
                  <p>Rata-rata tiket saat PLN: <strong>{plnCorr.pln_impact.avg_tickets_with_pln}</strong></p>
                  <p>Kenaikan: <strong className="text-red-600">{plnCorr.pln_impact.increase_pct}%</strong></p>
                </div>
              )}
            </div>
          )}
        </div>

        <div className="bg-white rounded-lg border p-4 space-y-3">
          <div className="flex items-center gap-2 text-[#1B2A4A] font-semibold">
            <CalendarDays size={18} /> Hari Kerja vs Libur
          </div>
          {!calCorr ? (
            <p className="text-sm text-gray-400">Klik "Hitung" untuk melihat hasil</p>
          ) : calCorr.error ? (
            <div className="bg-yellow-50 border border-yellow-200 rounded p-2 text-sm text-yellow-800 flex items-center gap-2">
              <AlertTriangle size={14} /> Generate kalender terlebih dahulu di tab Kalender
            </div>
          ) : (
            <div className="space-y-2 text-sm">
              <div className="grid grid-cols-2 gap-2">
                <div className="bg-gray-50 rounded p-2 text-center">
                  <p className="text-xs text-gray-500">Hari Kerja</p>
                  <p className="text-lg font-bold">{calCorr.avg_tickets_weekday ?? '-'}</p>
                </div>
                <div className="bg-gray-50 rounded p-2 text-center">
                  <p className="text-xs text-gray-500">Weekend</p>
                  <p className="text-lg font-bold">{calCorr.avg_tickets_weekend ?? '-'}</p>
                </div>
                <div className="bg-gray-50 rounded p-2 text-center">
                  <p className="text-xs text-gray-500">Libur</p>
                  <p className="text-lg font-bold">{calCorr.avg_tickets_holiday ?? '-'}</p>
                </div>
                <div className="bg-gray-50 rounded p-2 text-center">
                  <p className="text-xs text-gray-500">Ramadan</p>
                  <p className="text-lg font-bold">{calCorr.avg_tickets_ramadan ?? '-'}</p>
                </div>
              </div>
              <div className="text-xs space-y-1">
                <p>Weekend: <strong className={calCorr.weekend_reduction_pct > 0 ? 'text-red-600' : 'text-green-600'}>{calCorr.weekend_reduction_pct > 0 ? '+' : ''}{calCorr.weekend_reduction_pct}%</strong></p>
                <p>Libur: <strong className={calCorr.holiday_reduction_pct > 0 ? 'text-red-600' : 'text-green-600'}>{calCorr.holiday_reduction_pct > 0 ? '+' : ''}{calCorr.holiday_reduction_pct}%</strong></p>
                <p>Ramadan: <strong className={calCorr.ramadan_increase_pct > 0 ? 'text-red-600' : 'text-green-600'}>{calCorr.ramadan_increase_pct > 0 ? '+' : ''}{calCorr.ramadan_increase_pct}%</strong></p>
              </div>
              {calCorr.interpretation && (
                <p className="text-xs text-gray-600 italic">{calCorr.interpretation}</p>
              )}
            </div>
          )}
        </div>
      </div>

      <div className="bg-blue-50 border border-blue-200 rounded-lg p-3 text-sm text-blue-800 flex items-center gap-2">
        <Info size={16} /> Data cuaca/PLN belum tersedia? Upload di tab Cuaca/PLN.
      </div>
    </div>
  );
}

function ExternalDataPage() {
  const [activeTab, setActiveTab] = useState('cuaca');

  return (
    <div>
      <h2 className="text-xl font-bold text-gray-800 mb-4">External Data</h2>

      <div className="border-b border-gray-200 mb-6">
        <nav className="flex gap-0 -mb-px">
          {TABS.map(({ key, label, icon: Icon }) => (
            <button
              key={key}
              onClick={() => setActiveTab(key)}
              className={`inline-flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
                activeTab === key
                  ? 'border-[#1B2A4A] text-[#1B2A4A]'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              <Icon size={16} />
              {label}
            </button>
          ))}
        </nav>
      </div>

      {activeTab === 'cuaca' && <CuacaTab />}
      {activeTab === 'pln' && <PlnTab />}
      {activeTab === 'kalender' && <KalenderTab />}
      {activeTab === 'anotasi' && <AnotasiTab />}
      {activeTab === 'korelasi' && <KorelasiTab />}
    </div>
  );
}

export default ExternalDataPage;