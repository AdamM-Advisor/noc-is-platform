import { useState } from 'react';
import { X, Save } from 'lucide-react';
import axios from 'axios';

export default function SaveDialog({ open, onClose, filters, profileData }) {
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [pinned, setPinned] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');

  if (!open) return null;

  const kpis = profileData?.kpis || {};
  const identity = profileData?.identity || {};
  const behavior = profileData?.behavior;

  const handleSave = async () => {
    if (!name.trim()) {
      setError('Nama view wajib diisi');
      return;
    }
    setSaving(true);
    setError('');

    const urlParams = new URLSearchParams();
    if (filters.entityLevel) urlParams.set('level', filters.entityLevel);
    if (filters.entityId) urlParams.set('id', filters.entityId);
    if (filters.granularity) urlParams.set('gran', filters.granularity);
    if (filters.dateFrom) urlParams.set('from', filters.dateFrom);
    if (filters.dateTo) urlParams.set('to', filters.dateTo);

    const payload = {
      name: name.trim(),
      description: description.trim(),
      entity_level: filters.entityLevel || '',
      entity_id: filters.entityId || '',
      entity_name: identity.name || identity.entity_id || filters.entityId || '',
      granularity: filters.granularity || 'monthly',
      date_from: filters.dateFrom || '',
      date_to: filters.dateTo || '',
      type_ticket: filters.typeTicket || '',
      severities: filters.severities || [],
      fault_level: filters.faultLevel || '',
      rc_category: filters.rcCategory || '',
      snapshot_sla: kpis.sla_pct?.value ?? null,
      snapshot_mttr: kpis.avg_mttr_min?.value ?? null,
      snapshot_volume: kpis.total_tickets?.value ?? null,
      snapshot_escalation: kpis.escalation_pct?.value ?? null,
      snapshot_auto_resolve: kpis.auto_resolve_pct?.value ?? null,
      snapshot_repeat: kpis.repeat_pct?.value ?? null,
      snapshot_behavior: behavior?.label || '',
      snapshot_status: profileData?.overall_status?.status || '',
      snapshot_risk_score: null,
      is_pinned: pinned,
      url_params: urlParams.toString(),
    };

    try {
      await axios.post('/api/saved-views', payload);
      setName('');
      setDescription('');
      setPinned(false);
      onClose(true);
    } catch (err) {
      setError(err.response?.data?.detail || 'Gagal menyimpan view');
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
      <div className="bg-white rounded-xl shadow-xl max-w-md w-full">
        <div className="flex items-center justify-between p-4 border-b">
          <div className="flex items-center gap-2 font-semibold text-gray-800">
            <Save size={18} className="text-blue-600" />
            Simpan Profiler View
          </div>
          <button onClick={() => onClose(false)} className="p-1 hover:bg-gray-100 rounded">
            <X size={18} />
          </button>
        </div>

        <div className="p-4 space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Nama View *</label>
            <input
              type="text"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="contoh: Area SUMBAGUT Jan-Mar 2025"
              className="w-full border rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              autoFocus
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Deskripsi</label>
            <textarea
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="Catatan opsional..."
              rows={2}
              className="w-full border rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            />
          </div>

          <div className="bg-gray-50 rounded-lg p-3 space-y-2">
            <div className="text-xs font-semibold text-gray-500 uppercase">Snapshot Info</div>
            <div className="grid grid-cols-2 gap-2 text-sm">
              <div><span className="text-gray-500">Level:</span> {filters.entityLevel?.toUpperCase()}</div>
              <div><span className="text-gray-500">Entity:</span> {identity.name || filters.entityId}</div>
              {filters.dateFrom && <div><span className="text-gray-500">Dari:</span> {filters.dateFrom}</div>}
              {filters.dateTo && <div><span className="text-gray-500">Sampai:</span> {filters.dateTo}</div>}
            </div>
            {kpis.sla_pct && (
              <div className="flex flex-wrap gap-3 text-xs text-gray-600 pt-1 border-t border-gray-200">
                <span>SLA: {kpis.sla_pct.value?.toFixed(1)}%</span>
                <span>MTTR: {Math.round(kpis.avg_mttr_min?.value || 0)} min</span>
                <span>Vol: {kpis.total_tickets?.value || 0}</span>
              </div>
            )}
          </div>

          <label className="flex items-center gap-2 cursor-pointer">
            <input
              type="checkbox"
              checked={pinned}
              onChange={(e) => setPinned(e.target.checked)}
              className="rounded text-blue-600"
            />
            <span className="text-sm text-gray-700">Pin ke atas daftar</span>
          </label>

          {error && (
            <div className="text-sm text-red-600 bg-red-50 rounded-lg p-2">{error}</div>
          )}
        </div>

        <div className="flex items-center justify-end gap-2 p-4 border-t bg-gray-50 rounded-b-xl">
          <button
            onClick={() => onClose(false)}
            className="px-4 py-2 text-sm text-gray-600 hover:bg-gray-200 rounded-lg"
          >
            Batal
          </button>
          <button
            onClick={handleSave}
            disabled={saving || !name.trim()}
            className="px-4 py-2 text-sm bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 flex items-center gap-2"
          >
            <Save size={14} />
            {saving ? 'Menyimpan...' : 'Simpan'}
          </button>
        </div>
      </div>
    </div>
  );
}
