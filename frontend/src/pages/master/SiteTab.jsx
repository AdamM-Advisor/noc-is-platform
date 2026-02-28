import { useState, useEffect, useCallback } from 'react';
import client from '../../api/client';
import { Search, Download, Edit, ChevronLeft, ChevronRight, X, ArrowUp, ArrowDown } from 'lucide-react';

const CLASS_OPTIONS = ['Diamond', 'Platinum', 'Gold', 'Silver', 'Bronze'];
const FLAG_OPTIONS = ['Site Reguler', '3T', 'USO/MP', 'Femto', 'No BTS'];
const STATUS_OPTIONS = ['ACTIVE', 'INACTIVE'];

function enrichPreview(siteClass, siteFlag) {
  const cls = siteClass;
  const flag = siteFlag;

  let site_category = 'Non-Komersial';
  if (flag === 'Site Reguler') site_category = 'Komersial';

  let site_sub_class = cls;
  if (flag === 'No BTS') site_sub_class = 'No BTS';
  else if (flag === 'Site Reguler') site_sub_class = cls;
  else if (flag === '3T') site_sub_class = `${cls}-3T`;
  else if (flag === 'USO/MP') site_sub_class = `${cls}-USO`;
  else if (flag === 'Femto') site_sub_class = `${cls}-Femto`;

  let est_technology = 'N/A';
  if (flag === 'No BTS') est_technology = 'Tidak Ada BTS';
  else if (['Diamond', 'Platinum'].includes(cls)) est_technology = 'Multi (2G/3G/4G/5G)';
  else if (['Gold', 'Silver'].includes(cls)) est_technology = 'Multi (2G/3G/4G)';
  else if (cls === 'Bronze' && flag === 'Site Reguler') est_technology = 'Limited (4G / 2G+4G)';
  else if (cls === 'Bronze' && ['3T', 'USO/MP'].includes(flag)) est_technology = 'Single (4G LTE)';
  else if (cls === 'Bronze' && flag === 'Femto') est_technology = 'Single (4G Femto)';

  let est_power = 'N/A';
  if (flag === 'No BTS') est_power = 'Tidak Ada';
  else if (['Diamond', 'Platinum'].includes(cls)) est_power = 'PLN + Genset + Baterai (redundan)';
  else if (['Gold', 'Silver'].includes(cls)) est_power = 'PLN + Baterai';
  else if (cls === 'Bronze' && flag === 'Site Reguler') est_power = 'PLN + Baterai (minimal)';
  else if (cls === 'Bronze' && ['3T', 'USO/MP'].includes(flag)) est_power = 'Solar Panel + Baterai';
  else if (cls === 'Bronze' && flag === 'Femto') est_power = 'PLN (rumah/gedung)';

  let complexity_level = 0;
  if (flag === 'No BTS') complexity_level = 0;
  else if (['Diamond', 'Platinum'].includes(cls)) complexity_level = 5;
  else if (['Gold', 'Silver'].includes(cls)) complexity_level = 4;
  else if (cls === 'Bronze' && flag === 'Site Reguler') complexity_level = 3;
  else if (cls === 'Bronze' && ['3T', 'USO/MP'].includes(flag)) complexity_level = 2;
  else if (cls === 'Bronze' && flag === 'Femto') complexity_level = 1;

  let strategy_focus = 'N/A';
  if (flag === 'No BTS') strategy_focus = 'Non-Applicable';
  else if (['Diamond', 'Platinum'].includes(cls)) strategy_focus = 'Capacity & Quality Management';
  else if (['Gold', 'Silver'].includes(cls)) strategy_focus = 'Reliability & Optimization';
  else if (cls === 'Bronze' && flag === 'Site Reguler') strategy_focus = 'OPEX Efficiency';
  else if (cls === 'Bronze' && ['3T', 'USO/MP'].includes(flag)) strategy_focus = 'Availability & Access';
  else if (cls === 'Bronze' && flag === 'Femto') strategy_focus = 'Monitoring Minimal';

  return { site_category, site_sub_class, est_technology, est_power, complexity_level, strategy_focus };
}

function SiteTab() {
  const [items, setItems] = useState([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [loading, setLoading] = useState(false);

  const [areaId, setAreaId] = useState('');
  const [regionalId, setRegionalId] = useState('');
  const [nopId, setNopId] = useState('');
  const [classFilter, setClassFilter] = useState('');
  const [flagFilter, setFlagFilter] = useState('');
  const [statusFilter, setStatusFilter] = useState('ACTIVE');
  const [search, setSearch] = useState('');
  const [searchInput, setSearchInput] = useState('');
  const [sortBy, setSortBy] = useState('site_id');
  const [sortDir, setSortDir] = useState('asc');

  const [areas, setAreas] = useState([]);
  const [regionals, setRegionals] = useState([]);
  const [nops, setNops] = useState([]);
  const [tos, setTos] = useState([]);

  const [editSite, setEditSite] = useState(null);
  const [editForm, setEditForm] = useState({});
  const [editEnrichment, setEditEnrichment] = useState(null);
  const [saving, setSaving] = useState(false);

  const perPage = 50;

  useEffect(() => {
    client.get('/master/area').then(r => setAreas(r.data)).catch(() => {});
  }, []);

  useEffect(() => {
    if (areaId) {
      client.get(`/master/regional?area_id=${areaId}`).then(r => setRegionals(r.data)).catch(() => {});
    } else {
      setRegionals([]);
    }
    setRegionalId('');
    setNopId('');
    setNops([]);
  }, [areaId]);

  useEffect(() => {
    if (regionalId) {
      client.get(`/master/nop?regional_id=${regionalId}`).then(r => setNops(r.data)).catch(() => {});
    } else {
      setNops([]);
    }
    setNopId('');
  }, [regionalId]);

  useEffect(() => {
    if (editSite) {
      client.get('/master/to').then(r => setTos(r.data)).catch(() => {});
    }
  }, [editSite]);

  const fetchSites = useCallback(async () => {
    setLoading(true);
    try {
      const params = new URLSearchParams();
      params.set('page', page);
      params.set('per_page', perPage);
      if (areaId) params.set('area_id', areaId);
      if (regionalId) params.set('regional_id', regionalId);
      if (nopId) params.set('nop_id', nopId);
      if (classFilter) params.set('class', classFilter);
      if (flagFilter) params.set('flag', flagFilter);
      if (statusFilter) params.set('status', statusFilter);
      if (search) params.set('search', search);
      params.set('sort_by', sortBy);
      params.set('sort_dir', sortDir);

      const res = await client.get(`/master/site?${params.toString()}`);
      setItems(res.data.items);
      setTotal(res.data.total);
      setTotalPages(res.data.total_pages);
    } catch {
    } finally {
      setLoading(false);
    }
  }, [page, areaId, regionalId, nopId, classFilter, flagFilter, statusFilter, search, sortBy, sortDir]);

  useEffect(() => {
    fetchSites();
  }, [fetchSites]);

  useEffect(() => {
    setPage(1);
  }, [areaId, regionalId, nopId, classFilter, flagFilter, statusFilter, search, sortBy, sortDir]);

  const handleSort = (col) => {
    if (sortBy === col) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc');
    } else {
      setSortBy(col);
      setSortDir('asc');
    }
  };

  const handleSearch = (e) => {
    e.preventDefault();
    setSearch(searchInput);
  };

  const handleExport = async () => {
    try {
      const body = {};
      if (areaId) body.area_id = areaId;
      if (regionalId) body.regional_id = regionalId;
      if (nopId) body.nop_id = nopId;
      if (classFilter) body.site_class = classFilter;
      if (flagFilter) body.site_flag = flagFilter;
      if (statusFilter) body.status = statusFilter;
      if (search) body.search = search;

      const res = await client.post('/master/site/export', body, { responseType: 'blob' });
      const url = window.URL.createObjectURL(new Blob([res.data]));
      const a = document.createElement('a');
      a.href = url;
      a.download = 'sites_export.csv';
      a.click();
      window.URL.revokeObjectURL(url);
    } catch {
    }
  };

  const openEdit = (site) => {
    setEditSite(site);
    setEditForm({
      to_id: site.to_id || '',
      site_class: site.site_class || '',
      site_flag: site.site_flag || '',
      latitude: site.latitude ?? '',
      longitude: site.longitude ?? '',
      status: site.status || 'ACTIVE',
    });
    setEditEnrichment(null);
  };

  const handleEditChange = (field, value) => {
    const updated = { ...editForm, [field]: value };
    setEditForm(updated);

    if (field === 'site_class' || field === 'site_flag') {
      const cls = field === 'site_class' ? value : updated.site_class;
      const flg = field === 'site_flag' ? value : updated.site_flag;
      if (cls && flg && (cls !== editSite.site_class || flg !== editSite.site_flag)) {
        setEditEnrichment(enrichPreview(cls, flg));
      } else {
        setEditEnrichment(null);
      }
    }
  };

  const handleSave = async () => {
    if (!editSite) return;
    setSaving(true);
    try {
      const body = {};
      if (editForm.to_id !== (editSite.to_id || '')) body.to_id = editForm.to_id || null;
      if (editForm.site_class !== editSite.site_class) body.site_class = editForm.site_class;
      if (editForm.site_flag !== editSite.site_flag) body.site_flag = editForm.site_flag;
      if (String(editForm.latitude) !== String(editSite.latitude ?? '')) body.latitude = editForm.latitude === '' ? null : Number(editForm.latitude);
      if (String(editForm.longitude) !== String(editSite.longitude ?? '')) body.longitude = editForm.longitude === '' ? null : Number(editForm.longitude);
      if (editForm.status !== editSite.status) body.site_name = editSite.site_name;

      if (editForm.status !== editSite.status) {
        body.site_class = editForm.site_class;
      }

      if (Object.keys(body).length === 0) {
        setEditSite(null);
        return;
      }

      await client.put(`/master/site/${editSite.site_id}`, body);
      setEditSite(null);
      fetchSites();
    } catch {
    } finally {
      setSaving(false);
    }
  };

  const startItem = (page - 1) * perPage + 1;
  const endItem = Math.min(page * perPage, total);

  const renderPageNumbers = () => {
    const pages = [];
    let start = Math.max(1, page - 2);
    let end = Math.min(totalPages, page + 2);
    if (start > 1) pages.push(
      <button key={1} onClick={() => setPage(1)} className="px-3 py-1 text-sm rounded border border-gray-300 hover:bg-gray-50">1</button>
    );
    if (start > 2) pages.push(<span key="ds" className="px-1 text-gray-400">...</span>);
    for (let i = start; i <= end; i++) {
      pages.push(
        <button
          key={i}
          onClick={() => setPage(i)}
          className={`px-3 py-1 text-sm rounded border ${page === i ? 'bg-[#1B2A4A] text-white border-[#1B2A4A]' : 'border-gray-300 hover:bg-gray-50'}`}
        >
          {i}
        </button>
      );
    }
    if (end < totalPages - 1) pages.push(<span key="de" className="px-1 text-gray-400">...</span>);
    if (end < totalPages) pages.push(
      <button key={totalPages} onClick={() => setPage(totalPages)} className="px-3 py-1 text-sm rounded border border-gray-300 hover:bg-gray-50">{totalPages}</button>
    );
    return pages;
  };

  const SortHeader = ({ col, label }) => (
    <th
      className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase cursor-pointer select-none hover:text-[#1B2A4A]"
      onClick={() => handleSort(col)}
    >
      <div className="flex items-center gap-1">
        {label}
        {sortBy === col && (sortDir === 'asc' ? <ArrowUp className="w-3 h-3" /> : <ArrowDown className="w-3 h-3" />)}
      </div>
    </th>
  );

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap gap-3 items-end">
        <div>
          <label className="block text-xs font-medium text-gray-600 mb-1">Area</label>
          <select value={areaId} onChange={e => setAreaId(e.target.value)} className="border border-gray-300 rounded-lg px-3 py-2 text-sm min-w-[140px]">
            <option value="">Semua Area</option>
            {areas.map(a => <option key={a.area_id} value={a.area_id}>{a.area_name}</option>)}
          </select>
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-600 mb-1">Regional</label>
          <select value={regionalId} onChange={e => setRegionalId(e.target.value)} disabled={!areaId} className="border border-gray-300 rounded-lg px-3 py-2 text-sm min-w-[140px] disabled:bg-gray-100">
            <option value="">Semua Regional</option>
            {regionals.map(r => <option key={r.regional_id} value={r.regional_id}>{r.regional_name}</option>)}
          </select>
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-600 mb-1">NOP</label>
          <select value={nopId} onChange={e => setNopId(e.target.value)} disabled={!regionalId} className="border border-gray-300 rounded-lg px-3 py-2 text-sm min-w-[140px] disabled:bg-gray-100">
            <option value="">Semua NOP</option>
            {nops.map(n => <option key={n.nop_id} value={n.nop_id}>{n.nop_name}</option>)}
          </select>
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-600 mb-1">Class</label>
          <select value={classFilter} onChange={e => setClassFilter(e.target.value)} className="border border-gray-300 rounded-lg px-3 py-2 text-sm min-w-[120px]">
            <option value="">Semua Class</option>
            {CLASS_OPTIONS.map(c => <option key={c} value={c}>{c}</option>)}
          </select>
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-600 mb-1">Flag</label>
          <select value={flagFilter} onChange={e => setFlagFilter(e.target.value)} className="border border-gray-300 rounded-lg px-3 py-2 text-sm min-w-[120px]">
            <option value="">Semua Flag</option>
            {FLAG_OPTIONS.map(f => <option key={f} value={f}>{f}</option>)}
          </select>
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-600 mb-1">Status</label>
          <select value={statusFilter} onChange={e => setStatusFilter(e.target.value)} className="border border-gray-300 rounded-lg px-3 py-2 text-sm min-w-[120px]">
            <option value="">Semua Status</option>
            {STATUS_OPTIONS.map(s => <option key={s} value={s}>{s}</option>)}
          </select>
        </div>
      </div>

      <div className="flex items-center justify-between gap-4">
        <form onSubmit={handleSearch} className="flex items-center gap-2">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
            <input
              type="text"
              value={searchInput}
              onChange={e => setSearchInput(e.target.value)}
              placeholder="Cari Site ID atau Nama..."
              className="border border-gray-300 rounded-lg pl-9 pr-3 py-2 text-sm w-64"
            />
          </div>
          <button type="submit" className="bg-[#1B2A4A] text-white px-4 py-2 rounded-lg text-sm hover:bg-[#2a3d66] transition-colors">
            Cari
          </button>
        </form>

        <div className="flex items-center gap-3">
          <span className="text-sm font-medium text-gray-700">Total: {total.toLocaleString()} site</span>
          <button onClick={handleExport} className="flex items-center gap-2 bg-[#1B2A4A] text-white px-4 py-2 rounded-lg text-sm hover:bg-[#2a3d66] transition-colors">
            <Download className="w-4 h-4" />
            Export CSV
          </button>
        </div>
      </div>

      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-gray-50 border-b border-gray-200">
              <tr>
                <SortHeader col="site_id" label="Site ID" />
                <SortHeader col="site_name" label="Nama" />
                <SortHeader col="to_name" label="TO" />
                <SortHeader col="site_class" label="Class" />
                <SortHeader col="site_flag" label="Flag" />
                <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Kategori</th>
                <SortHeader col="status" label="Status" />
                <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Aksi</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {loading ? (
                <tr><td colSpan={8} className="px-4 py-12 text-center text-gray-400">Memuat data...</td></tr>
              ) : items.length === 0 ? (
                <tr><td colSpan={8} className="px-4 py-12 text-center text-gray-400">Tidak ada data</td></tr>
              ) : items.map(item => (
                <tr key={item.site_id} className="hover:bg-gray-50">
                  <td className="px-4 py-3 text-sm font-mono text-gray-800">{item.site_id}</td>
                  <td className="px-4 py-3 text-sm text-gray-700 max-w-[200px] truncate">{item.site_name}</td>
                  <td className="px-4 py-3 text-sm text-gray-600">{item.to_name || item.to_id || '-'}</td>
                  <td className="px-4 py-3 text-sm">
                    <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${
                      item.site_class === 'Diamond' ? 'bg-blue-100 text-blue-700' :
                      item.site_class === 'Platinum' ? 'bg-purple-100 text-purple-700' :
                      item.site_class === 'Gold' ? 'bg-yellow-100 text-yellow-700' :
                      item.site_class === 'Silver' ? 'bg-gray-200 text-gray-700' :
                      'bg-orange-100 text-orange-700'
                    }`}>{item.site_class}</span>
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-600">{item.site_flag}</td>
                  <td className="px-4 py-3 text-sm text-gray-600">{item.site_category || '-'}</td>
                  <td className="px-4 py-3 text-sm">
                    <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${item.status === 'ACTIVE' ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'}`}>
                      {item.status}
                    </span>
                  </td>
                  <td className="px-4 py-3">
                    <button onClick={() => openEdit(item)} className="p-1.5 text-gray-500 hover:text-[#1B2A4A] hover:bg-gray-100 rounded-lg transition-colors">
                      <Edit className="w-4 h-4" />
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {totalPages > 1 && (
        <div className="flex items-center justify-between">
          <span className="text-sm text-gray-600">
            Menampilkan {startItem}-{endItem} dari {total.toLocaleString()}
          </span>
          <div className="flex items-center gap-1">
            <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1} className="p-2 rounded-lg border border-gray-300 hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed">
              <ChevronLeft className="w-4 h-4" />
            </button>
            {renderPageNumbers()}
            <button onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={page === totalPages} className="p-2 rounded-lg border border-gray-300 hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed">
              <ChevronRight className="w-4 h-4" />
            </button>
          </div>
        </div>
      )}

      {editSite && (
        <div className="fixed inset-0 bg-black/50 z-50 flex justify-end">
          <div className="w-full max-w-lg bg-white h-full overflow-y-auto shadow-2xl">
            <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200 bg-[#1B2A4A]">
              <h3 className="text-lg font-semibold text-white">Edit Site</h3>
              <button onClick={() => setEditSite(null)} className="text-white/80 hover:text-white">
                <X className="w-5 h-5" />
              </button>
            </div>

            <div className="p-6 space-y-5">
              <div>
                <label className="block text-xs font-medium text-gray-500 mb-1">Site ID</label>
                <div className="text-sm font-mono text-gray-800 bg-gray-50 rounded-lg px-3 py-2">{editSite.site_id}</div>
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-500 mb-1">Nama Site</label>
                <div className="text-sm text-gray-800 bg-gray-50 rounded-lg px-3 py-2">{editSite.site_name}</div>
              </div>

              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">TO</label>
                <select value={editForm.to_id} onChange={e => handleEditChange('to_id', e.target.value)} className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm">
                  <option value="">- Pilih TO -</option>
                  {tos.map(t => <option key={t.to_id} value={t.to_id}>{t.to_name} ({t.to_id})</option>)}
                </select>
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-xs font-medium text-gray-600 mb-1">Site Class</label>
                  <select value={editForm.site_class} onChange={e => handleEditChange('site_class', e.target.value)} className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm">
                    {CLASS_OPTIONS.map(c => <option key={c} value={c}>{c}</option>)}
                  </select>
                </div>
                <div>
                  <label className="block text-xs font-medium text-gray-600 mb-1">Site Flag</label>
                  <select value={editForm.site_flag} onChange={e => handleEditChange('site_flag', e.target.value)} className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm">
                    {FLAG_OPTIONS.map(f => <option key={f} value={f}>{f}</option>)}
                  </select>
                </div>
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-xs font-medium text-gray-600 mb-1">Latitude</label>
                  <input type="number" step="any" value={editForm.latitude} onChange={e => handleEditChange('latitude', e.target.value)} className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm" />
                </div>
                <div>
                  <label className="block text-xs font-medium text-gray-600 mb-1">Longitude</label>
                  <input type="number" step="any" value={editForm.longitude} onChange={e => handleEditChange('longitude', e.target.value)} className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm" />
                </div>
              </div>

              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">Status</label>
                <select value={editForm.status} onChange={e => handleEditChange('status', e.target.value)} className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm">
                  {STATUS_OPTIONS.map(s => <option key={s} value={s}>{s}</option>)}
                </select>
              </div>

              {editEnrichment && (
                <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 space-y-2">
                  <h4 className="text-sm font-semibold text-blue-800">Preview Perubahan Enrichment</h4>
                  <div className="grid grid-cols-2 gap-2 text-xs">
                    <div>
                      <span className="text-gray-500">Kategori:</span>
                      <span className="ml-1 font-medium text-gray-800">{editEnrichment.site_category}</span>
                    </div>
                    <div>
                      <span className="text-gray-500">Sub Class:</span>
                      <span className="ml-1 font-medium text-gray-800">{editEnrichment.site_sub_class}</span>
                    </div>
                    <div>
                      <span className="text-gray-500">Teknologi:</span>
                      <span className="ml-1 font-medium text-gray-800">{editEnrichment.est_technology}</span>
                    </div>
                    <div>
                      <span className="text-gray-500">Power:</span>
                      <span className="ml-1 font-medium text-gray-800">{editEnrichment.est_power}</span>
                    </div>
                    <div>
                      <span className="text-gray-500">Complexity:</span>
                      <span className="ml-1 font-medium text-gray-800">{editEnrichment.complexity_level}</span>
                    </div>
                    <div>
                      <span className="text-gray-500">Strategi:</span>
                      <span className="ml-1 font-medium text-gray-800">{editEnrichment.strategy_focus}</span>
                    </div>
                  </div>
                </div>
              )}

              <div className="bg-gray-50 rounded-lg p-4 space-y-2">
                <h4 className="text-sm font-semibold text-gray-700">Informasi Enrichment Saat Ini</h4>
                <div className="grid grid-cols-2 gap-2 text-xs">
                  <div><span className="text-gray-500">Kategori:</span> <span className="font-medium">{editSite.site_category || '-'}</span></div>
                  <div><span className="text-gray-500">Sub Class:</span> <span className="font-medium">{editSite.site_sub_class || '-'}</span></div>
                  <div><span className="text-gray-500">Teknologi:</span> <span className="font-medium">{editSite.est_technology || '-'}</span></div>
                  <div><span className="text-gray-500">Power:</span> <span className="font-medium">{editSite.est_power || '-'}</span></div>
                  <div><span className="text-gray-500">Complexity:</span> <span className="font-medium">{editSite.complexity_level ?? '-'}</span></div>
                  <div><span className="text-gray-500">Strategi:</span> <span className="font-medium">{editSite.strategy_focus || '-'}</span></div>
                </div>
              </div>

              <div className="flex gap-3 pt-2">
                <button
                  onClick={handleSave}
                  disabled={saving}
                  className="flex-1 bg-[#1B2A4A] text-white py-2.5 rounded-lg text-sm font-medium hover:bg-[#2a3d66] transition-colors disabled:opacity-50"
                >
                  {saving ? 'Menyimpan...' : 'Simpan'}
                </button>
                <button
                  onClick={() => setEditSite(null)}
                  className="flex-1 border border-gray-300 text-gray-700 py-2.5 rounded-lg text-sm font-medium hover:bg-gray-50 transition-colors"
                >
                  Batal
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default SiteTab;
