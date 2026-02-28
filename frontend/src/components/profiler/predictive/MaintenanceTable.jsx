const PRIORITY_BADGE = {
  high: 'bg-red-100 text-red-700',
  medium: 'bg-yellow-100 text-yellow-700',
  low: 'bg-green-100 text-green-700',
};

const PRIORITY_ICON = {
  high: '🔴',
  medium: '🟡',
  low: '🟢',
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
                    <span className={`inline-block px-2 py-0.5 rounded text-[10px] font-medium ${PRIORITY_BADGE[p]}`}>
                      {PRIORITY_ICON[p]} {p.charAt(0).toUpperCase() + p.slice(1)}
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
