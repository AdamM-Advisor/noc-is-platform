const CONFIDENCE_STYLES = {
  high: { bg: '#DCFCE7', text: '#166534' },
  medium: { bg: '#FEF9C3', text: '#854D0E' },
  low: { bg: '#F1F5F9', text: '#475569' },
};

function SymptomsTab({ symptoms, code }) {
  if (!symptoms || symptoms.length === 0) {
    return <p className="text-sm text-[#475569]">Belum ada symptoms yang terdeteksi.</p>;
  }

  const primary = symptoms.filter(s => s.symptom_type === 'primary');
  const secondary = symptoms.filter(s => s.symptom_type === 'secondary');
  const negative = symptoms.filter(s => s.symptom_type === 'negative');

  return (
    <div className="space-y-5">
      {primary.length > 0 && (
        <SymptomGroup title="Primary Symptoms" items={primary} />
      )}
      {secondary.length > 0 && (
        <SymptomGroup title="Secondary Symptoms" items={secondary} />
      )}
      {negative.length > 0 && (
        <div>
          <h4 className="text-xs font-semibold text-[#475569] uppercase tracking-wider mb-2">Negative Symptoms</h4>
          <div className="space-y-2">
            {negative.map((s, i) => (
              <div key={i} className="bg-red-50 border border-red-100 rounded-lg p-3">
                <div className="text-sm text-red-800 font-medium">{s.symptom_text}</div>
                {s.negative_note && <div className="text-xs text-red-600 mt-1">{s.negative_note}</div>}
                {s.redirect_ndc && <div className="text-xs text-[#1E40AF] mt-1">Lihat: {s.redirect_ndc}</div>}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function SymptomGroup({ title, items }) {
  return (
    <div>
      <h4 className="text-xs font-semibold text-[#475569] uppercase tracking-wider mb-2">{title}</h4>
      <div className="space-y-2">
        {items.map((s, i) => {
          const conf = CONFIDENCE_STYLES[s.confidence] || CONFIDENCE_STYLES.low;
          return (
            <div key={i} className="flex items-center gap-3 bg-white rounded-lg border border-gray-200 p-3">
              <div className="flex-1">
                <div className="text-sm text-[#0F172A] font-medium">{s.symptom_text}</div>
                <div className="text-xs text-[#475569] mt-0.5">Sumber: {s.source || '-'}</div>
              </div>
              <div className="flex items-center gap-2 shrink-0">
                <span className="text-sm font-mono font-semibold text-[#0F172A]">{(s.frequency_pct || 0).toFixed(1)}%</span>
                <span className="px-2 py-0.5 rounded-full text-xs font-medium" style={{ backgroundColor: conf.bg, color: conf.text }}>
                  {s.confidence}
                </span>
              </div>
              <div className="w-20 shrink-0">
                <div className="h-2 bg-gray-100 rounded-full overflow-hidden">
                  <div
                    className="h-full rounded-full"
                    style={{
                      width: `${Math.min(100, s.frequency_pct || 0)}%`,
                      backgroundColor: '#1E40AF'
                    }}
                  />
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

export default SymptomsTab;
