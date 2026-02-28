import { useState, useEffect } from 'react';
import axios from 'axios';
import { Save, RotateCcw, RefreshCw } from 'lucide-react';

const GROUPS = [
  { key: 'MTTR', label: 'MTTR', fields: [
    { param: 'mttr_good', label: 'MTTR Good', unit: 'menit' },
    { param: 'mttr_attention', label: 'MTTR Attention', unit: 'menit' },
    { param: 'mttr_slow', label: 'MTTR Slow', unit: 'menit' },
  ]},
  { key: 'ESCALATION', label: 'Escalation', fields: [
    { param: 'esc_normal', label: 'Normal', unit: '%' },
    { param: 'esc_warning', label: 'Warning', unit: '%' },
  ]},
  { key: 'AUTO_RESOLVE', label: 'Auto-resolve', fields: [
    { param: 'auto_resolve_good', label: 'Good', unit: '%' },
    { param: 'auto_resolve_moderate', label: 'Moderate', unit: '%' },
  ]},
  { key: 'REPEAT', label: 'Repeat Incident', fields: [
    { param: 'repeat_normal', label: 'Normal', unit: '%' },
    { param: 'repeat_warning', label: 'Warning', unit: '%' },
  ]},
  { key: 'TREND', label: 'Trend', fields: [
    { param: 'trend_stable_sla', label: 'SLA Stable', unit: 'pp/bulan' },
    { param: 'trend_stable_mttr', label: 'MTTR Stable', unit: '%/bulan' },
    { param: 'trend_stable_vol', label: 'Volume Stable', unit: '%/bulan' },
    { param: 'trend_decline_months', label: 'Decline Duration', unit: 'bulan' },
  ]},
  { key: 'ANOMALY', label: 'Anomaly', fields: [
    { param: 'anomaly_threshold', label: 'Threshold', unit: 'z-score' },
    { param: 'anomaly_significant', label: 'Significant', unit: 'z-score' },
  ]},
  { key: 'RISK', label: 'Risk Score', fields: [
    { param: 'risk_high', label: 'HIGH ≥', unit: 'score' },
    { param: 'risk_medium', label: 'MEDIUM ≥', unit: 'score' },
  ]},
  { key: 'BEHAVIOR', label: 'Behavior', fields: [
    { param: 'chronic_monthly_min', label: 'Chronic monthly min', unit: 'tiket' },
    { param: 'chronic_duration', label: 'Chronic duration', unit: 'bulan' },
    { param: 'device_age_economic', label: 'Device economic life', unit: 'tahun' },
  ]},
  { key: 'SEASONAL', label: 'Seasonal', fields: [
    { param: 'seasonal_peak', label: 'Peak factor', unit: '×' },
    { param: 'seasonal_low', label: 'Low factor', unit: '×' },
  ]},
  { key: 'PATTERN', label: 'Pattern', fields: [
    { param: 'pattern_cv_consistent', label: 'CV threshold', unit: 'cv' },
  ]},
  { key: 'CAPACITY', label: 'Capacity', fields: [
    { param: 'capacity_buffer', label: 'Alert threshold', unit: 'factor' },
  ]},
  { key: 'DISPLAY', label: 'Display', fields: [
    { param: 'max_recommendations', label: 'Max recommendations', unit: 'count' },
  ]},
];

export default function ThresholdForm() {
  const [values, setValues] = useState({});
  const [original, setOriginal] = useState({});
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);

  const fetchThresholds = async () => {
    setLoading(true);
    try {
      const res = await axios.get('/api/threshold');
      const flat = {};
      const data = res.data;
      if (Array.isArray(data)) {
        data.forEach(t => { flat[t.param_key] = t.param_value; });
      } else if (typeof data === 'object') {
        Object.values(data).forEach(group => {
          if (Array.isArray(group)) {
            group.forEach(t => { flat[t.param_key] = t.param_value; });
          }
        });
      }
      setValues(flat);
      setOriginal(flat);
    } catch {}
    setLoading(false);
  };

  useEffect(() => { fetchThresholds(); }, []);

  const handleChange = (param, val) => {
    setValues(prev => ({ ...prev, [param]: parseFloat(val) || 0 }));
    setSaved(false);
  };

  const handleSave = async () => {
    setSaving(true);
    try {
      const changed = Object.entries(values).filter(([k, v]) => v !== original[k]);
      for (const [key, value] of changed) {
        await axios.put(`/api/threshold/${key}`, { param_value: value });
      }
      setOriginal({ ...values });
      setSaved(true);
      setTimeout(() => setSaved(false), 3000);
    } catch {}
    setSaving(false);
  };

  const handleReset = async () => {
    if (!confirm('Reset semua threshold ke default?')) return;
    setSaving(true);
    try {
      await axios.post('/api/schema/seed-reset');
      await fetchThresholds();
    } catch {}
    setSaving(false);
  };

  if (loading) {
    return <div className="flex items-center justify-center py-8 text-gray-400"><RefreshCw size={20} className="animate-spin mr-2" /> Loading...</div>;
  }

  return (
    <div className="space-y-4">
      {GROUPS.map(group => (
        <div key={group.key} className="border border-gray-200 rounded-lg p-4">
          <p className="text-xs font-bold text-gray-500 uppercase mb-3">{group.label}</p>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
            {group.fields.map(f => (
              <div key={f.param} className="flex items-center gap-2">
                <label className="text-xs text-gray-600 flex-1 min-w-[120px]">{f.label}</label>
                <input
                  type="number"
                  step="any"
                  value={values[f.param] ?? ''}
                  onChange={(e) => handleChange(f.param, e.target.value)}
                  className="w-20 border border-gray-300 rounded px-2 py-1 text-sm text-right focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                />
                <span className="text-[10px] text-gray-400 w-14">{f.unit}</span>
              </div>
            ))}
          </div>
        </div>
      ))}

      <div className="flex items-center gap-3 pt-2">
        <button
          onClick={handleReset}
          disabled={saving}
          className="flex items-center gap-1.5 px-4 py-2 bg-gray-100 text-gray-600 rounded-lg text-sm font-medium hover:bg-gray-200 disabled:opacity-50"
        >
          <RotateCcw size={14} /> Reset Default
        </button>
        <button
          onClick={handleSave}
          disabled={saving}
          className="flex items-center gap-1.5 px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 disabled:opacity-50"
        >
          <Save size={14} /> {saving ? 'Saving...' : 'Simpan'}
        </button>
        {saved && <span className="text-xs text-green-600 font-medium">✓ Tersimpan</span>}
      </div>
    </div>
  );
}
