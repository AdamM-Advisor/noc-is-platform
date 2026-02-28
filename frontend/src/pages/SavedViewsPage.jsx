import { useState, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { Bookmark, RefreshCw, Pin, Clock, Search, Pencil, X, Check } from 'lucide-react';
import axios from 'axios';
import SavedViewCard from '../components/saved-views/SavedViewCard';

export default function SavedViewsPage() {
  const navigate = useNavigate();
  const [views, setViews] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [searchQuery, setSearchQuery] = useState('');
  const [editingView, setEditingView] = useState(null);
  const [editName, setEditName] = useState('');
  const [editDesc, setEditDesc] = useState('');

  const fetchViews = useCallback(async () => {
    setLoading(true);
    setError('');
    try {
      const res = await axios.get('/api/saved-views');
      const list = res.data?.views || [];
      const withDeltas = await Promise.all(
        list.map(async (v) => {
          try {
            const detail = await axios.get(`/api/saved-views/${v.id}`);
            return detail.data;
          } catch {
            return v;
          }
        })
      );
      setViews(withDeltas);
    } catch (err) {
      setError(err.response?.data?.detail || 'Gagal memuat saved views');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchViews();
  }, [fetchViews]);

  const handleOpen = async (view) => {
    try {
      await axios.post(`/api/saved-views/${view.id}/access`);
    } catch {}
    const params = new URLSearchParams();
    if (view.entity_level) params.set('level', view.entity_level);
    if (view.entity_id) params.set('id', view.entity_id);
    if (view.granularity) params.set('gran', view.granularity);
    if (view.date_from) params.set('from', view.date_from);
    if (view.date_to) params.set('to', view.date_to);
    navigate(`/profiler?${params.toString()}`);
  };

  const handleCompare = (view) => {
    navigate(`/comparison?view_id=${view.id}`);
  };

  const handleEdit = (view) => {
    setEditingView(view);
    setEditName(view.name);
    setEditDesc(view.description || '');
  };

  const handleEditSave = async () => {
    if (!editingView || !editName.trim()) return;
    try {
      await axios.put(`/api/saved-views/${editingView.id}`, {
        name: editName.trim(),
        description: editDesc.trim(),
      });
      setEditingView(null);
      fetchViews();
    } catch {}
  };

  const handleDelete = async (id) => {
    if (!window.confirm('Hapus saved view ini?')) return;
    try {
      await axios.delete(`/api/saved-views/${id}`);
      setViews((prev) => prev.filter((v) => v.id !== id));
    } catch {}
  };

  const handlePin = async (id) => {
    try {
      await axios.put(`/api/saved-views/${id}/pin`);
      fetchViews();
    } catch {}
  };

  const filtered = views.filter((v) => {
    if (!searchQuery) return true;
    const q = searchQuery.toLowerCase();
    return (
      v.name?.toLowerCase().includes(q) ||
      v.entity_name?.toLowerCase().includes(q) ||
      v.entity_id?.toLowerCase().includes(q) ||
      v.description?.toLowerCase().includes(q)
    );
  });

  const pinnedViews = filtered.filter((v) => v.is_pinned);
  const recentViews = filtered.filter((v) => !v.is_pinned);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <div className="flex items-center gap-2">
            <Bookmark size={22} className="text-blue-600" />
            <h2 className="text-xl font-bold text-gray-800">Saved Views</h2>
          </div>
          <p className="text-xs text-gray-400 mt-0.5">
            Simpan dan bandingkan konfigurasi Profiler dengan delta tracking
          </p>
        </div>
        <button
          onClick={fetchViews}
          disabled={loading}
          className="p-2 hover:bg-gray-100 rounded-lg text-gray-500"
          title="Refresh"
        >
          <RefreshCw size={18} className={loading ? 'animate-spin' : ''} />
        </button>
      </div>

      <div className="relative">
        <Search size={16} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
        <input
          type="text"
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          placeholder="Cari saved views..."
          className="w-full pl-9 pr-4 py-2.5 border rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
        />
      </div>

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-sm text-red-700">{error}</div>
      )}

      {loading && (
        <div className="flex items-center justify-center py-12 text-gray-400">
          <RefreshCw size={24} className="animate-spin mr-2" /> Memuat saved views...
        </div>
      )}

      {!loading && views.length === 0 && (
        <div className="bg-white rounded-lg border p-12 text-center text-gray-500">
          <Bookmark size={48} className="mx-auto mb-4 text-gray-300" />
          <h3 className="text-lg font-medium text-gray-700 mb-2">Belum ada Saved Views</h3>
          <p className="text-sm">
            Buka Profiler, generate profil, lalu klik "Simpan View" untuk menyimpan konfigurasi.
          </p>
        </div>
      )}

      {!loading && pinnedViews.length > 0 && (
        <div>
          <div className="flex items-center gap-2 mb-3">
            <Pin size={14} className="text-blue-500" />
            <h3 className="text-sm font-semibold text-gray-600 uppercase tracking-wide">
              Pinned ({pinnedViews.length})
            </h3>
          </div>
          <div className="grid sm:grid-cols-2 lg:grid-cols-3 gap-4">
            {pinnedViews.map((v) => (
              <SavedViewCard
                key={v.id}
                view={v}
                onOpen={handleOpen}
                onCompare={handleCompare}
                onEdit={handleEdit}
                onDelete={handleDelete}
                onPin={handlePin}
              />
            ))}
          </div>
        </div>
      )}

      {!loading && recentViews.length > 0 && (
        <div>
          <div className="flex items-center gap-2 mb-3">
            <Clock size={14} className="text-gray-500" />
            <h3 className="text-sm font-semibold text-gray-600 uppercase tracking-wide">
              Terbaru ({recentViews.length})
            </h3>
          </div>
          <div className="grid sm:grid-cols-2 lg:grid-cols-3 gap-4">
            {recentViews.map((v) => (
              <SavedViewCard
                key={v.id}
                view={v}
                onOpen={handleOpen}
                onCompare={handleCompare}
                onEdit={handleEdit}
                onDelete={handleDelete}
                onPin={handlePin}
              />
            ))}
          </div>
        </div>
      )}

      {editingView && (
        <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
          <div className="bg-white rounded-xl shadow-xl max-w-md w-full">
            <div className="flex items-center justify-between p-4 border-b">
              <div className="flex items-center gap-2 font-semibold text-gray-800">
                <Pencil size={18} className="text-blue-600" />
                Edit Saved View
              </div>
              <button onClick={() => setEditingView(null)} className="p-1 hover:bg-gray-100 rounded">
                <X size={18} />
              </button>
            </div>
            <div className="p-4 space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Nama</label>
                <input
                  type="text"
                  value={editName}
                  onChange={(e) => setEditName(e.target.value)}
                  className="w-full border rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500"
                  autoFocus
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Deskripsi</label>
                <textarea
                  value={editDesc}
                  onChange={(e) => setEditDesc(e.target.value)}
                  rows={2}
                  className="w-full border rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500"
                />
              </div>
            </div>
            <div className="flex items-center justify-end gap-2 p-4 border-t bg-gray-50 rounded-b-xl">
              <button onClick={() => setEditingView(null)} className="px-4 py-2 text-sm text-gray-600 hover:bg-gray-200 rounded-lg">
                Batal
              </button>
              <button
                onClick={handleEditSave}
                disabled={!editName.trim()}
                className="px-4 py-2 text-sm bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 flex items-center gap-2"
              >
                <Check size={14} />
                Simpan
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
