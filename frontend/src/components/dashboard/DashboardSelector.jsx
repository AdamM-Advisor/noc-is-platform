import { useEffect } from 'react';

const LEVEL_OPTIONS = [
  { value: 'area', label: 'Area' },
  { value: 'regional', label: 'Regional' },
  { value: 'nop', label: 'NOP' },
  { value: 'to', label: 'TO' },
];

export default function DashboardSelector({
  period, periods, viewLevel, parentFilter, parentOptions,
  onPeriodChange, onLevelChange, onParentChange, onRefresh,
}) {
  return (
    <div className="flex flex-wrap items-center gap-3">
      <div>
        <label className="text-xs text-gray-500 block mb-0.5">Periode</label>
        <select
          value={period}
          onChange={(e) => onPeriodChange(e.target.value)}
          className="border border-gray-300 rounded-lg px-3 py-1.5 text-sm bg-white focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
        >
          {periods.map((p) => (
            <option key={p} value={p}>{p}</option>
          ))}
        </select>
      </div>

      <div>
        <label className="text-xs text-gray-500 block mb-0.5">Level</label>
        <select
          value={viewLevel}
          onChange={(e) => onLevelChange(e.target.value)}
          className="border border-gray-300 rounded-lg px-3 py-1.5 text-sm bg-white focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
        >
          {LEVEL_OPTIONS.map((o) => (
            <option key={o.value} value={o.value}>{o.label}</option>
          ))}
        </select>
      </div>

      <div>
        <label className="text-xs text-gray-500 block mb-0.5">Filter</label>
        <select
          value={parentFilter || ''}
          onChange={(e) => onParentChange(e.target.value || null)}
          className="border border-gray-300 rounded-lg px-3 py-1.5 text-sm bg-white focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
        >
          <option value="">Semua</option>
          {parentOptions.map((p) => (
            <option key={p.id} value={p.id}>{p.name}</option>
          ))}
        </select>
      </div>

      <div className="pt-4">
        <button
          onClick={onRefresh}
          className="px-4 py-1.5 bg-[#1B2A4A] text-white rounded-lg text-sm font-medium hover:bg-[#243660] transition-colors"
        >
          Refresh
        </button>
      </div>
    </div>
  );
}
