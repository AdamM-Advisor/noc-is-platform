import { useState, useEffect, useMemo } from 'react';
import { Search, ArrowRightLeft } from 'lucide-react';
import axios from 'axios';

const LEVEL_OPTIONS = [
  { value: 'area', label: 'Area' },
  { value: 'regional', label: 'Regional' },
  { value: 'nop', label: 'NOP' },
  { value: 'to', label: 'TO' },
  { value: 'site', label: 'Site' },
];

const TYPE_LABELS = {
  temporal: 'Temporal (Waktu Berbeda)',
  entity: 'Entity (Entitas Berbeda)',
  fault: 'Fault (Filter Berbeda)',
};

function ProfilePanel({ label, profile, onChange, options, color }) {
  const areas = options?.areas || [];
  const regionals = options?.regionals || [];
  const nops = options?.nops || [];
  const tos = options?.tos || [];
  const periods = options?.periods || [];

  const filteredEntities = useMemo(() => {
    switch (profile.entity_level) {
      case 'area': return areas;
      case 'regional': return regionals;
      case 'nop': return nops;
      case 'to': return tos;
      default: return [];
    }
  }, [profile.entity_level, areas, regionals, nops, tos]);

  return (
    <div className={`border-2 rounded-lg p-4 space-y-3 ${color}`}>
      <div className="text-sm font-semibold text-gray-700">{label}</div>

      <div>
        <label className="text-xs text-gray-500">Level</label>
        <select
          className="w-full border rounded-lg px-3 py-2 text-sm mt-1"
          value={profile.entity_level}
          onChange={(e) => onChange({ ...profile, entity_level: e.target.value, entity_id: '', entity_name: '' })}
        >
          {LEVEL_OPTIONS.map(o => (
            <option key={o.value} value={o.value}>{o.label}</option>
          ))}
        </select>
      </div>

      <div>
        <label className="text-xs text-gray-500">Entitas</label>
        {profile.entity_level === 'site' ? (
          <input
            type="text"
            className="w-full border rounded-lg px-3 py-2 text-sm mt-1"
            placeholder="Ketik Site ID..."
            value={profile.entity_id}
            onChange={(e) => onChange({ ...profile, entity_id: e.target.value, entity_name: e.target.value })}
          />
        ) : (
          <select
            className="w-full border rounded-lg px-3 py-2 text-sm mt-1"
            value={profile.entity_id}
            onChange={(e) => {
              const selected = filteredEntities.find(f => f.id === e.target.value);
              onChange({ ...profile, entity_id: e.target.value, entity_name: selected?.name || e.target.value });
            }}
          >
            <option value="">-- Pilih --</option>
            {filteredEntities.map(e => (
              <option key={e.id} value={e.id}>{e.name}</option>
            ))}
          </select>
        )}
      </div>

      <div className="grid grid-cols-2 gap-2">
        <div>
          <label className="text-xs text-gray-500">Dari</label>
          <select
            className="w-full border rounded-lg px-3 py-2 text-sm mt-1"
            value={profile.date_from}
            onChange={(e) => onChange({ ...profile, date_from: e.target.value })}
          >
            <option value="">Semua</option>
            {periods.map(p => <option key={p} value={p}>{p}</option>)}
          </select>
        </div>
        <div>
          <label className="text-xs text-gray-500">Sampai</label>
          <select
            className="w-full border rounded-lg px-3 py-2 text-sm mt-1"
            value={profile.date_to}
            onChange={(e) => onChange({ ...profile, date_to: e.target.value })}
          >
            <option value="">Semua</option>
            {periods.map(p => <option key={p} value={p}>{p}</option>)}
          </select>
        </div>
      </div>
    </div>
  );
}

export default function ComparisonSelector({ profileA, profileB, onChangeA, onChangeB, onCompare, loading, comparisonType }) {
  const [options, setOptions] = useState(null);

  useEffect(() => {
    axios.get('/api/profiler/filter-options').then(r => setOptions(r.data)).catch(() => {});
  }, []);

  const detectedType = useMemo(() => {
    if (!profileA.entity_id || !profileB.entity_id) return null;
    const sameEntity = profileA.entity_id === profileB.entity_id;
    const samePeriod = profileA.date_from === profileB.date_from && profileA.date_to === profileB.date_to;
    if (sameEntity && !samePeriod) return 'temporal';
    if (!sameEntity && samePeriod) return 'entity';
    return 'fault';
  }, [profileA, profileB]);

  const canCompare = profileA.entity_id && profileB.entity_id && profileA.entity_level === profileB.entity_level;

  return (
    <div className="bg-white rounded-lg border shadow-sm p-5 space-y-4">
      <div className="flex items-center gap-2 text-lg font-semibold text-gray-800">
        <ArrowRightLeft size={20} className="text-blue-600" />
        Comparison Mode
      </div>

      <div className="grid md:grid-cols-2 gap-4 items-start">
        <ProfilePanel
          label="Profil A"
          profile={profileA}
          onChange={onChangeA}
          options={options}
          color="border-blue-200 bg-blue-50/30"
        />
        <ProfilePanel
          label="Profil B"
          profile={profileB}
          onChange={onChangeB}
          options={options}
          color="border-emerald-200 bg-emerald-50/30"
        />
      </div>

      {detectedType && (
        <div className="flex items-center gap-2 text-sm">
          <span className="text-gray-500">Tipe Perbandingan:</span>
          <span className="px-2 py-0.5 bg-gray-100 rounded-full text-xs font-medium text-gray-700">
            {TYPE_LABELS[detectedType] || detectedType}
          </span>
        </div>
      )}

      {profileA.entity_level !== profileB.entity_level && profileA.entity_id && profileB.entity_id && (
        <div className="text-sm text-amber-600 bg-amber-50 border border-amber-200 rounded-lg px-3 py-2">
          ⚠️ Level entitas harus sama untuk membandingkan.
        </div>
      )}

      <button
        onClick={onCompare}
        disabled={!canCompare || loading}
        className="bg-blue-600 text-white px-5 py-2.5 rounded-lg text-sm font-medium hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
      >
        {loading ? (
          <>
            <Search size={16} className="animate-spin" />
            Membandingkan...
          </>
        ) : (
          <>
            <ArrowRightLeft size={16} />
            Bandingkan
          </>
        )}
      </button>
    </div>
  );
}
