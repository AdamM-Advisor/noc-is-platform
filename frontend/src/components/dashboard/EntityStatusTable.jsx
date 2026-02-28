import { useState } from 'react';
import { useNavigate } from 'react-router-dom';

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
          {sorted.map((e) => (
            <tr key={e.id} className="border-b border-gray-100 hover:bg-gray-50">
              <td className="px-3 py-2.5 font-medium text-gray-700">{e.name || e.id}</td>
              <td className="px-3 py-2.5 text-gray-600">{e.sla_pct}%</td>
              <td className="px-3 py-2.5 text-gray-600">{Math.round(e.avg_mttr_min)}m</td>
              <td className="px-3 py-2.5 text-gray-600">
                {e.total_volume >= 1000 ? `${(e.total_volume / 1000).toFixed(1)}K` : e.total_volume}
              </td>
              <td className="px-3 py-2.5 text-center">{e.trend_icon}</td>
              <td className="px-3 py-2.5 text-center">
                <span
                  className="inline-block px-2 py-0.5 rounded text-[10px] font-medium"
                  style={{ backgroundColor: e.status_color + '15', color: e.status_color }}
                >
                  {e.status_icon} {e.status_level}
                </span>
              </td>
              <td className="px-3 py-2.5 text-right">
                <button
                  onClick={() => handleViewProfile(e)}
                  className="text-xs text-blue-600 hover:text-blue-800 font-medium"
                >
                  Lihat Profil →
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="text-[10px] text-gray-400 mt-1.5 px-1">
        Sorted by: {sortKey === 'sla_pct' ? 'SLA' : sortKey} {sortDir === 'asc' ? 'ASC' : 'DESC'} (klik header untuk sort)
      </p>
    </div>
  );
}
