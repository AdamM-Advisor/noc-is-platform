export default function BreachCountdown({ children: items, narrative }) {
  if (!items?.length) return null;

  const getStatusStyle = (item) => {
    if (item.status === 'breached' || item.status === 'already_breached') {
      return { badge: 'bg-red-100 text-red-700', icon: '🔴', label: 'Breached' };
    }
    if (item.breach_in != null && item.breach_in <= 4) {
      return { badge: 'bg-yellow-100 text-yellow-700', icon: '🟡', label: `~${item.breach_in}w` };
    }
    return { badge: 'bg-green-100 text-green-700', icon: '🟢', label: 'Aman' };
  };

  return (
    <div className="space-y-2">
      <p className="text-xs font-semibold text-gray-500 uppercase">Breach Countdown</p>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b">
              <th className="text-left px-2 py-1.5 text-gray-500">Entity</th>
              <th className="text-right px-2 py-1.5 text-gray-500">SLA Saat Ini</th>
              <th className="text-right px-2 py-1.5 text-gray-500">Projected 4w</th>
              <th className="text-center px-2 py-1.5 text-gray-500">Breach In</th>
              <th className="text-center px-2 py-1.5 text-gray-500">Status</th>
            </tr>
          </thead>
          <tbody>
            {items.map((item, i) => {
              const st = getStatusStyle(item);
              return (
                <tr key={item.entity_id || i} className="border-b hover:bg-gray-50">
                  <td className="px-2 py-1.5 font-medium text-gray-700 max-w-[10rem] truncate" title={item.entity_name}>
                    {item.entity_name || item.entity_id}
                  </td>
                  <td className="px-2 py-1.5 text-right text-gray-600">
                    {item.current_sla != null ? `${item.current_sla.toFixed(1)}%` : '—'}
                  </td>
                  <td className="px-2 py-1.5 text-right text-gray-600">
                    {item.projected_4w != null ? `${item.projected_4w.toFixed(1)}%` : '—'}
                  </td>
                  <td className="px-2 py-1.5 text-center">
                    <span className="text-sm">{st.icon}</span>
                    <span className="ml-1">{item.breach_in != null && item.breach_in > 0 ? `~${item.breach_in} minggu` : st.label}</span>
                  </td>
                  <td className="px-2 py-1.5 text-center">
                    <span className={`inline-block px-2 py-0.5 rounded text-[10px] font-medium ${st.badge}`}>
                      {st.label}
                    </span>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {narrative && (
        <div className="text-sm px-3 py-2 rounded border bg-gray-50 border-gray-200 text-gray-600">
          {narrative}
        </div>
      )}
    </div>
  );
}
