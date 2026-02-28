const PRIORITY_BADGE = {
  high: 'border border-gray-200 bg-gray-50 text-gray-700',
  medium: 'border border-gray-200 bg-gray-50 text-gray-700',
  low: 'border border-gray-200 bg-gray-50 text-gray-600',
};

const PRIORITY_DOT = {
  high: 'bg-red-500',
  medium: 'bg-amber-500',
  low: 'bg-gray-400',
};

export default function MaintenanceTable({ schedule }) {
  if (!schedule?.length) return null;

  return (
    <div className="space-y-2">
      <p className="text-xs font-semibold text-gray-500 uppercase">Detail Maintenance Schedule</p>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b">
              <th className="text-left px-2 py-1.5 text-gray-500">#</th>
              <th className="text-left px-2 py-1.5 text-gray-500">Tanggal</th>
              <th className="text-left px-2 py-1.5 text-gray-500">Site</th>
              <th className="text-center px-2 py-1.5 text-gray-500">Priority</th>
              <th className="text-left px-2 py-1.5 text-gray-500">Reason</th>
              <th className="text-right px-2 py-1.5 text-gray-500">Est (jam)</th>
            </tr>
          </thead>
          <tbody>
            {schedule.map((s, i) => {
              const p = s.priority || 'low';
              return (
                <tr key={i} className="border-b hover:bg-gray-50">
                  <td className="px-2 py-1.5 text-gray-400">{i + 1}</td>
                  <td className="px-2 py-1.5 text-gray-700 font-medium">
                    {s.date ? new Date(s.date).toLocaleDateString('id-ID', { day: 'numeric', month: 'short' }) : '—'}
                  </td>
                  <td className="px-2 py-1.5 text-gray-700 max-w-[10rem] truncate" title={s.site_name}>
                    {s.site_name || s.site_id}
                  </td>
                  <td className="px-2 py-1.5 text-center">
                    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-[10px] font-medium ${PRIORITY_BADGE[p]}`}>
                      <span className={`inline-block w-1.5 h-1.5 rounded-full ${PRIORITY_DOT[p]}`} />
                      {p.charAt(0).toUpperCase() + p.slice(1)}
                    </span>
                  </td>
                  <td className="px-2 py-1.5 text-gray-600 max-w-[14rem] truncate" title={s.reason}>
                    {s.reason || '—'}
                  </td>
                  <td className="px-2 py-1.5 text-right text-gray-600">
                    {s.estimated_hours ?? '—'}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
