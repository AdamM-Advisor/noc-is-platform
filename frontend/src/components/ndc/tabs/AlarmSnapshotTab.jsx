function AlarmSnapshotTab({ data, coAlarms }) {
  if (!data) {
    return <p className="text-sm text-[#475569]">Data alarm snapshot belum tersedia.</p>;
  }

  return (
    <div className="space-y-5">
      <Section title="Profil Tipikal">
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-x-8 gap-y-2">
          <KV label="Severity" value={data.typical_severity} />
          <KV label="NE Class" value={data.typical_ne_class} />
          <KV label="Fault Level" value={data.typical_fault_level} />
          <KV label="Impact" value={data.typical_impact} />
          <KV label="Type Tiket" value={data.typical_type_ticket} />
          <KV label="RAT" value={data.typical_rat} />
        </div>
        <div className="mt-2 text-xs text-[#475569]">Sample: {(data.sample_size || 0).toLocaleString()} tiket</div>
      </Section>

      <Section title="Temporal Signature">
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-x-8 gap-y-2">
          <KV label="Peak Hours" value={data.peak_hours_range} />
          <KV label="Peak Days" value={data.peak_days} />
          <KV label="Musiman" value={data.seasonal_pattern || 'Tidak ada pola signifikan'} />
        </div>
      </Section>

      <Section title="Site Signature">
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-x-8 gap-y-2">
          <KV label="Site Class" value={data.site_class_distribution} />
          <KV label="3T %" value={data.pct_3t != null ? `${data.pct_3t}%` : '-'} />
          <KV label="Top Regions" value={data.top_regions} />
        </div>
      </Section>

      {coAlarms && coAlarms.length > 0 && (
        <Section title="Co-occurring Alarms">
          <div className="space-y-2">
            {coAlarms.map((ca, i) => (
              <div key={i} className="flex items-center gap-3 bg-white rounded-lg border border-gray-200 p-3">
                <div className="flex-1 min-w-0">
                  <div className="text-sm font-medium text-[#0F172A] truncate">{ca.co_alarm_rc_1 || ca.co_alarm_description}</div>
                  <div className="text-xs text-[#475569] mt-0.5">
                    {ca.co_alarm_rc_category} | {ca.typical_lag_description || `Lag: ${ca.typical_lag_min || 0}m`}
                  </div>
                </div>
                <div className="text-right shrink-0">
                  <div className="text-sm font-semibold text-[#0F172A]">{(ca.co_occurrence_pct || 0).toFixed(1)}%</div>
                  <div className="text-xs text-[#475569]">{ca.sample_size} tiket</div>
                </div>
                <div className="w-24 shrink-0">
                  <div className="h-2 bg-gray-100 rounded-full overflow-hidden">
                    <div
                      className="h-full bg-[#1E40AF] rounded-full"
                      style={{ width: `${Math.min(100, ca.co_occurrence_pct || 0)}%` }}
                    />
                  </div>
                </div>
              </div>
            ))}
          </div>
        </Section>
      )}
    </div>
  );
}

function Section({ title, children }) {
  return (
    <div>
      <h4 className="text-xs font-semibold text-[#475569] uppercase tracking-wider mb-2">{title}</h4>
      {children}
    </div>
  );
}

function KV({ label, value }) {
  return (
    <div className="flex items-baseline gap-2 py-1">
      <span className="text-xs text-[#475569] min-w-[100px] shrink-0">{label}</span>
      <span className="text-sm text-[#0F172A]">{value || '-'}</span>
    </div>
  );
}

export default AlarmSnapshotTab;
