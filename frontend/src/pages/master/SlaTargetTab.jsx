import { useState, useEffect, useCallback } from 'react';
import { Plus, Edit, Trash2, FlaskConical, Info } from 'lucide-react';
import client from '../../api/client';

const CLASS_OPTIONS = ['*', 'Diamond', 'Platinum', 'Gold', 'Silver', 'Bronze'];
const FLAG_OPTIONS = ['*', 'Site Reguler', '3T', 'USO', 'MP', 'Femto', 'No BTS'];
const SEVERITY_OPTIONS = ['*', 'Critical', 'Major', 'Minor', 'Low'];

const EMPTY_FORM = {
  area_id: '*',
  regional_id: '*',
  site_class: '*',
  site_flag: '*',
  severity: '*',
  sla_target_pct: '',
  mttr_target_min: '',
  priority: '',
  description: '',
};

function scopeLabel(rule) {
  const parts = [];
  if (rule.area_id && rule.area_id !== '*') parts.push(`Area: ${rule.area_id}`);
  if (rule.regional_id && rule.regional_id !== '*') parts.push(`Regional: ${rule.regional_id}`);
  if (rule.site_class && rule.site_class !== '*') parts.push(`Class: ${rule.site_class}`);
  if (rule.site_flag && rule.site_flag !== '*') parts.push(`Flag: ${rule.site_flag}`);
  if (rule.severity && rule.severity !== '*') parts.push(`Severity: ${rule.severity}`);
  return parts.length > 0 ? parts.join(', ') : 'Default (semua)';
}

function SlaTargetTab() {
  const [rules, setRules] = useState([]);
  const [loading, setLoading] = useState(true);
  const [areas, setAreas] = useState([]);
  const [modalOpen, setModalOpen] = useState(false);
  const [editingRule, setEditingRule] = useState(null);
  const [form, setForm] = useState(EMPTY_FORM);
  const [saving, setSaving] = useState(false);
  const [impacts, setImpacts] = useState({});

  const [testerClass, setTesterClass] = useState('*');
  const [testerFlag, setTesterFlag] = useState('*');
  const [testerArea, setTesterArea] = useState('');
  const [resolveResult, setResolveResult] = useState(null);
  const [resolving, setResolving] = useState(false);

  const fetchRules = useCallback(async () => {
    setLoading(true);
    try {
      const res = await client.get('/master/sla-target');
      setRules(res.data.items || []);
    } catch {
    } finally {
      setLoading(false);
    }
  }, []);

  const fetchAreas = useCallback(async () => {
    try {
      const res = await client.get('/master/area');
      setAreas(Array.isArray(res.data) ? res.data : res.data.items || []);
    } catch {
    }
  }, []);

  useEffect(() => {
    fetchRules();
    fetchAreas();
  }, [fetchRules, fetchAreas]);

  const fetchImpact = useCallback(async (id) => {
    try {
      const res = await client.get(`/master/sla-target/${id}/impact`);
      setImpacts((prev) => ({ ...prev, [id]: res.data.affected_sites }));
    } catch {
    }
  }, []);

  useEffect(() => {
    rules.forEach((r) => {
      if (impacts[r.id] === undefined) fetchImpact(r.id);
    });
  }, [rules, fetchImpact, impacts]);

  const openCreate = () => {
    setEditingRule(null);
    setForm(EMPTY_FORM);
    setModalOpen(true);
  };

  const openEdit = (rule) => {
    setEditingRule(rule);
    setForm({
      area_id: rule.area_id || '*',
      regional_id: rule.regional_id || '*',
      site_class: rule.site_class || '*',
      site_flag: rule.site_flag || '*',
      severity: rule.severity || '*',
      sla_target_pct: rule.sla_target_pct ?? '',
      mttr_target_min: rule.mttr_target_min ?? '',
      priority: rule.priority ?? '',
      description: rule.description || '',
    });
    setModalOpen(true);
  };

  const handleSave = async () => {
    setSaving(true);
    try {
      const payload = {
        ...form,
        sla_target_pct: form.sla_target_pct !== '' ? Number(form.sla_target_pct) : null,
        mttr_target_min: form.mttr_target_min !== '' ? Number(form.mttr_target_min) : null,
        priority: form.priority !== '' ? Number(form.priority) : 0,
      };
      if (editingRule) {
        await client.put(`/master/sla-target/${editingRule.id}`, payload);
      } else {
        await client.post('/master/sla-target', payload);
      }
      setModalOpen(false);
      setImpacts({});
      fetchRules();
    } catch {
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async (rule) => {
    if (rule.priority === 0) return;
    if (!window.confirm(`Hapus rule #${rule.id}?`)) return;
    try {
      await client.delete(`/master/sla-target/${rule.id}`);
      setImpacts({});
      fetchRules();
    } catch {
    }
  };

  const handleResolve = useCallback(async () => {
    setResolving(true);
    try {
      const params = {};
      if (testerClass !== '*') params.site_class = testerClass;
      if (testerFlag !== '*') params.site_flag = testerFlag;
      if (testerArea) params.area_id = testerArea;
      const res = await client.get('/master/sla-target/resolve', { params });
      setResolveResult(res.data);
    } catch {
      setResolveResult(null);
    } finally {
      setResolving(false);
    }
  }, [testerClass, testerFlag, testerArea]);

  useEffect(() => {
    handleResolve();
  }, [handleResolve]);

  const updateField = (field, value) => {
    setForm((prev) => ({ ...prev, [field]: value }));
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h3 className="text-lg font-semibold text-[#1B2A4A]">SLA Target Rules</h3>
        <button
          onClick={openCreate}
          className="flex items-center gap-2 px-4 py-2 bg-[#1B2A4A] text-white rounded-lg text-sm font-medium hover:bg-[#2a3d66] transition-colors"
        >
          <Plus size={16} />
          Tambah Rule
        </button>
      </div>

      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50 border-b border-gray-200">
                <th className="text-left px-4 py-3 font-medium text-gray-600">#</th>
                <th className="text-left px-4 py-3 font-medium text-gray-600">Scope</th>
                <th className="text-left px-4 py-3 font-medium text-gray-600">SLA %</th>
                <th className="text-left px-4 py-3 font-medium text-gray-600">MTTR (min)</th>
                <th className="text-left px-4 py-3 font-medium text-gray-600">Priority</th>
                <th className="text-left px-4 py-3 font-medium text-gray-600">Impact</th>
                <th className="text-right px-4 py-3 font-medium text-gray-600">Aksi</th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan={7} className="text-center py-8 text-gray-400">Memuat...</td>
                </tr>
              ) : rules.length === 0 ? (
                <tr>
                  <td colSpan={7} className="text-center py-8 text-gray-400">Belum ada rule</td>
                </tr>
              ) : (
                rules.map((rule) => (
                  <tr
                    key={rule.id}
                    className="border-b border-gray-100 hover:bg-gray-50 cursor-pointer transition-colors"
                    onClick={() => openEdit(rule)}
                  >
                    <td className="px-4 py-3 text-gray-500">{rule.id}</td>
                    <td className="px-4 py-3 font-medium text-gray-800">{scopeLabel(rule)}</td>
                    <td className="px-4 py-3 text-gray-700">{rule.sla_target_pct != null ? `${rule.sla_target_pct}%` : '-'}</td>
                    <td className="px-4 py-3 text-gray-700">{rule.mttr_target_min ?? '-'}</td>
                    <td className="px-4 py-3">
                      <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${rule.priority === 0 ? 'bg-gray-100 text-gray-600' : 'bg-blue-50 text-blue-700'}`}>
                        {rule.priority}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-gray-500 text-xs">
                      {impacts[rule.id] !== undefined ? `${impacts[rule.id]} site` : '...'}
                    </td>
                    <td className="px-4 py-3 text-right">
                      <div className="flex items-center justify-end gap-1" onClick={(e) => e.stopPropagation()}>
                        <button
                          onClick={() => openEdit(rule)}
                          className="p-1.5 text-gray-400 hover:text-[#1B2A4A] hover:bg-gray-100 rounded transition-colors"
                        >
                          <Edit size={14} />
                        </button>
                        {rule.priority !== 0 && (
                          <button
                            onClick={() => handleDelete(rule)}
                            className="p-1.5 text-gray-400 hover:text-red-600 hover:bg-red-50 rounded transition-colors"
                          >
                            <Trash2 size={14} />
                          </button>
                        )}
                      </div>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      <div className="bg-white rounded-xl border border-gray-200 p-5">
        <div className="flex items-center gap-2 mb-4">
          <FlaskConical size={18} className="text-[#1B2A4A]" />
          <h4 className="text-sm font-semibold text-[#1B2A4A]">SLA Target Tester</h4>
        </div>
        <div className="flex flex-wrap items-end gap-4 mb-4">
          <div>
            <label className="block text-xs text-gray-500 mb-1">Class</label>
            <select
              value={testerClass}
              onChange={(e) => setTesterClass(e.target.value)}
              className="border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1B2A4A]/20"
            >
              {CLASS_OPTIONS.map((c) => (
                <option key={c} value={c}>{c === '*' ? 'Semua' : c}</option>
              ))}
            </select>
          </div>
          <div>
            <label className="block text-xs text-gray-500 mb-1">Flag</label>
            <select
              value={testerFlag}
              onChange={(e) => setTesterFlag(e.target.value)}
              className="border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1B2A4A]/20"
            >
              {FLAG_OPTIONS.map((f) => (
                <option key={f} value={f}>{f === '*' ? 'Semua' : f}</option>
              ))}
            </select>
          </div>
          <div>
            <label className="block text-xs text-gray-500 mb-1">Area</label>
            <select
              value={testerArea}
              onChange={(e) => setTesterArea(e.target.value)}
              className="border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1B2A4A]/20"
            >
              <option value="">Semua</option>
              {areas.map((a) => (
                <option key={a.area_id} value={a.area_id}>{a.area_name || a.area_id}</option>
              ))}
            </select>
          </div>
        </div>

        {resolving ? (
          <p className="text-sm text-gray-400">Resolving...</p>
        ) : resolveResult ? (
          <div className="space-y-2">
            {resolveResult.resolved ? (
              <>
                <div className="flex items-start gap-2 bg-green-50 border border-green-200 rounded-lg px-4 py-3">
                  <Info size={16} className="text-green-600 mt-0.5 shrink-0" />
                  <p className="text-sm text-green-800">
                    Resolved: <span className="font-bold">{resolveResult.resolved.sla_target_pct}%</span>
                    {' '}(rule #{resolveResult.resolved.id}: {resolveResult.resolved.description || scopeLabel(resolveResult.resolved)}, priority {resolveResult.resolved.priority})
                  </p>
                </div>
                {resolveResult.priority_chain && resolveResult.priority_chain.length > 0 && (
                  <p className="text-xs text-gray-500 pl-1">
                    Chain: {resolveResult.priority_chain.map((c) => `#${c.id}(${c.sla_target_pct}%,p${c.priority})`).join(' > ')}
                  </p>
                )}
              </>
            ) : (
              <div className="bg-yellow-50 border border-yellow-200 rounded-lg px-4 py-3">
                <p className="text-sm text-yellow-800">Tidak ada rule yang cocok</p>
              </div>
            )}
          </div>
        ) : null}
      </div>

      {modalOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
          <div className="bg-white rounded-xl shadow-xl w-full max-w-lg mx-4 max-h-[90vh] overflow-y-auto">
            <div className="px-6 py-4 border-b border-gray-200">
              <h3 className="text-lg font-semibold text-[#1B2A4A]">
                {editingRule ? `Edit Rule #${editingRule.id}` : 'Tambah Rule Baru'}
              </h3>
            </div>
            <div className="px-6 py-4 space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-xs font-medium text-gray-600 mb-1">Area</label>
                  <select
                    value={form.area_id}
                    onChange={(e) => updateField('area_id', e.target.value)}
                    className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1B2A4A]/20"
                  >
                    <option value="*">* (Semua)</option>
                    {areas.map((a) => (
                      <option key={a.area_id} value={a.area_id}>{a.area_name || a.area_id}</option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="block text-xs font-medium text-gray-600 mb-1">Regional</label>
                  <input
                    type="text"
                    value={form.regional_id}
                    onChange={(e) => updateField('regional_id', e.target.value)}
                    placeholder="* (Semua)"
                    className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1B2A4A]/20"
                  />
                </div>
              </div>
              <div className="grid grid-cols-3 gap-4">
                <div>
                  <label className="block text-xs font-medium text-gray-600 mb-1">Site Class</label>
                  <select
                    value={form.site_class}
                    onChange={(e) => updateField('site_class', e.target.value)}
                    className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1B2A4A]/20"
                  >
                    {CLASS_OPTIONS.map((c) => (
                      <option key={c} value={c}>{c === '*' ? '* (Semua)' : c}</option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="block text-xs font-medium text-gray-600 mb-1">Site Flag</label>
                  <select
                    value={form.site_flag}
                    onChange={(e) => updateField('site_flag', e.target.value)}
                    className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1B2A4A]/20"
                  >
                    {FLAG_OPTIONS.map((f) => (
                      <option key={f} value={f}>{f === '*' ? '* (Semua)' : f}</option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="block text-xs font-medium text-gray-600 mb-1">Severity</label>
                  <select
                    value={form.severity}
                    onChange={(e) => updateField('severity', e.target.value)}
                    className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1B2A4A]/20"
                  >
                    {SEVERITY_OPTIONS.map((s) => (
                      <option key={s} value={s}>{s === '*' ? '* (Semua)' : s}</option>
                    ))}
                  </select>
                </div>
              </div>
              <div className="grid grid-cols-3 gap-4">
                <div>
                  <label className="block text-xs font-medium text-gray-600 mb-1">SLA Target (%)</label>
                  <input
                    type="number"
                    step="0.1"
                    value={form.sla_target_pct}
                    onChange={(e) => updateField('sla_target_pct', e.target.value)}
                    className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1B2A4A]/20"
                  />
                </div>
                <div>
                  <label className="block text-xs font-medium text-gray-600 mb-1">MTTR (min)</label>
                  <input
                    type="number"
                    value={form.mttr_target_min}
                    onChange={(e) => updateField('mttr_target_min', e.target.value)}
                    className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1B2A4A]/20"
                  />
                </div>
                <div>
                  <label className="block text-xs font-medium text-gray-600 mb-1">Priority</label>
                  <input
                    type="number"
                    value={form.priority}
                    onChange={(e) => updateField('priority', e.target.value)}
                    className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1B2A4A]/20"
                  />
                </div>
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">Deskripsi</label>
                <input
                  type="text"
                  value={form.description}
                  onChange={(e) => updateField('description', e.target.value)}
                  className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#1B2A4A]/20"
                />
              </div>
            </div>
            <div className="px-6 py-4 border-t border-gray-200 flex justify-end gap-3">
              <button
                onClick={() => setModalOpen(false)}
                className="px-4 py-2 text-sm text-gray-600 hover:text-gray-800 transition-colors"
              >
                Batal
              </button>
              <button
                onClick={handleSave}
                disabled={saving}
                className="px-4 py-2 bg-[#1B2A4A] text-white rounded-lg text-sm font-medium hover:bg-[#2a3d66] transition-colors disabled:opacity-50"
              >
                {saving ? 'Menyimpan...' : 'Simpan'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default SlaTargetTab;
