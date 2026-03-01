function NdcDistribution({ data, entityLevel, entityId, compact }) {
  if (!data || data.length === 0) {
    return <p className="text-sm text-[#475569]">Tidak ada data distribusi NDC.</p>;
  }

  const isSiteView = data[0] && data[0].period !== undefined;

  if (isSiteView) {
    const grouped = {};
    data.forEach(r => {
      if (!grouped[r.ndc_code]) grouped[r.ndc_code] = [];
      grouped[r.ndc_code].push(r);
    });

    return (
      <div className="space-y-3">
        {Object.entries(grouped).map(([code, rows]) => {
          const totalTickets = rows.reduce((s, r) => s + (r.ticket_count || 0), 0);
          return (
            <div key={code} className="bg-white rounded-lg border border-gray-200 p-3">
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm font-mono font-medium text-[#1E40AF]">{code}</span>
                <span className="text-xs text-[#475569]">{totalTickets} tiket total</span>
              </div>
              <div className="space-y-1">
                {rows.slice(0, 6).map((r, i) => (
                  <div key={i} className="flex items-center gap-2 text-xs">
                    <span className="w-16 text-[#475569] shrink-0">{r.period}</span>
                    <div className="flex-1 h-2 bg-gray-100 rounded-full overflow-hidden">
                      <div
                        className="h-full bg-[#1E40AF] rounded-full"
                        style={{ width: `${Math.min(100, r.pct_of_site_total || 0)}%` }}
                      />
                    </div>
                    <span className="w-12 text-right font-mono text-[#0F172A]">{r.ticket_count}</span>
                    <span className="w-12 text-right font-mono text-[#475569]">{(r.pct_of_site_total || 0).toFixed(1)}%</span>
                  </div>
                ))}
              </div>
            </div>
          );
        })}
      </div>
    );
  }

  const maxPct = Math.max(...data.map(d => d.pct || 0), 1);

  return (
    <div className="space-y-2">
      {data.map((item, i) => (
        <div key={i} className={`flex items-center gap-3 ${compact ? '' : 'bg-white rounded-lg border border-gray-200 p-3'}`}>
          <div className={compact ? 'min-w-[80px]' : 'min-w-[120px]'}>
            <span className={`font-mono font-medium text-[#1E40AF] ${compact ? 'text-xs' : 'text-sm'}`}>{item.ndc_code}</span>
          </div>
          {!compact && (
            <div className="flex-1 min-w-0">
              <div className="text-sm text-[#0F172A] truncate">{item.title}</div>
              {item.priority && (
                <span className="text-xs text-[#475569]">{item.priority}</span>
              )}
            </div>
          )}
          <div className={compact ? 'flex-1' : 'w-32'}>
            <div className="h-2 bg-gray-100 rounded-full overflow-hidden">
              <div
                className="h-full bg-[#1E40AF] rounded-full"
                style={{ width: `${(item.pct || 0) / maxPct * 100}%` }}
              />
            </div>
          </div>
          <div className="text-right shrink-0">
            <div className={`font-mono font-semibold text-[#0F172A] ${compact ? 'text-xs' : 'text-sm'}`}>{(item.pct || 0).toFixed(1)}%</div>
            <div className={`font-mono text-[#475569] ${compact ? 'text-[10px]' : 'text-xs'}`}>{(item.ticket_count || 0).toLocaleString()}</div>
          </div>
        </div>
      ))}
    </div>
  );
}

export default NdcDistribution;
