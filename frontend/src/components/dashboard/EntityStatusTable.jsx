import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import StatusDot from '../ui/StatusDot';
import { getStatusLevel } from '../ui/StatusDot';

export default function EntityStatusTable({ entities, viewLevel }) {
  const navigate = useNavigate();
  const [sortKey, setSortKey] = useState('sla_pct');
  const [sortDir, setSortDir] = useState('asc');

  if (!entities?.length) return <p className="text-sm text-gray-400">Tidak ada data entitas.</p>;

  const handleSort = (key) => {
    if (sortKey === key) {
      setSortDir(sortDir === 'asc' ? 'desc' : 'asc');
    } else {
      setSortKey(key);
      setSortDir(key === 'sla_pct' ? 'asc' : 'desc');
    }
  };

  const sorted = [...entities].sort((a, b) => {
    const av = a[sortKey] ?? 0;
    const bv = b[sortKey] ?? 0;
    return sortDir === 'asc' ? av - bv : bv - av;
  });

  const handleViewProfile = (entity) => {
    navigate(`/profiler?level=${viewLevel}&id=${entity.id}`);
  };

  const levelLabel = { area: 'Area', regional: 'Regional', nop: 'NOP', to: 'TO' }[viewLevel] || viewLevel;

  const SortHeader = ({ label, field }) => (
    <th
      className="text-left px-3 py-2 text-xs font-semibold text-gray-500 cursor-pointer hover:text-gray-700 select-none"
      onClick={() => handleSort(field)}
    >
      {label} {sortKey === field ? (sortDir === 'asc' ? '↑' : '↓') : ''}
    </th>
  );

  const isBelowTarget = (slaPct) => slaPct != null && slaPct < 95;

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-gray-200">
            <SortHeader label={levelLabel} field="name" />
            <SortHeader label="SLA%" field="sla_pct" />
            <SortHeader label="MTTR" field="avg_mttr_min" />
            <SortHeader label="Volume" field="total_volume" />
            <th className="text-center px-3 py-2 text-xs font-semibold text-gray-500">Trend</th>
            <th className="text-center px-3 py-2 text-xs font-semibold text-gray-500">Status</th>
            <th className="text-right px-3 py-2 text-xs font-semibold text-gray-500">Action</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map((e) => {
            const statusLevel = getStatusLevel(e.status_level);
            return (
              <tr key={e.id} className="border-b border-gray-100 hover:bg-gray-50">
                <td className="px-3 py-2.5 font-medium" style={{ color: 'var(--text-primary)' }}>{e.name || e.id}</td>
                <td
                  className="px-3 py-2.5"
                  style={{ color: isBelowTarget(e.sla_pct) ? 'var(--status-critical-text)' : 'var(--text-secondary)' }}
                >
                  {e.sla_pct}%
                </td>
                <td className="px-3 py-2.5" style={{ color: 'var(--text-secondary)' }}>{Math.round(e.avg_mttr_min)}m</td>
                <td className="px-3 py-2.5" style={{ color: 'var(--text-secondary)' }}>
                  {e.total_volume >= 1000 ? `${(e.total_volume / 1000).toFixed(1)}K` : e.total_volume}
                </td>
                <td className="px-3 py-2.5 text-center" style={{ color: 'var(--text-muted)' }}>
                  {e.trend_label || '—'}
                </td>
                <td className="px-3 py-2.5 text-center">
                  <span className="inline-flex items-center gap-1.5 text-[11px] font-medium" style={{ color: 'var(--text-secondary)' }}>
                    <StatusDot status={statusLevel} size={7} />
                    {e.status_level}
                  </span>
                </td>
                <td className="px-3 py-2.5 text-right">
                  <button
                    onClick={() => handleViewProfile(e)}
                    className="text-xs font-medium"
                    style={{ color: 'var(--accent-brand)' }}
                  >
                    Lihat Profil
                  </button>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
      <p className="text-[10px] mt-1.5 px-1" style={{ color: 'var(--text-muted)' }}>
        Sorted by: {sortKey === 'sla_pct' ? 'SLA' : sortKey} {sortDir === 'asc' ? 'ASC' : 'DESC'} (klik header untuk sort)
      </p>
    </div>
  );
}
