import { AlertTriangle } from 'lucide-react';

export default function CompositionWarning({ compositionCheck }) {
  if (!compositionCheck) return null;
  if (compositionCheck.similar) return null;

  const compA = compositionCheck.composition_a || {};
  const compB = compositionCheck.composition_b || {};
  const allClasses = [...new Set([...Object.keys(compA), ...Object.keys(compB)])].sort();

  return (
    <div
      className="rounded-lg p-5 space-y-3"
      style={{
        backgroundColor: 'var(--status-warning-bg)',
        border: '1px solid var(--status-warning-border)',
        borderLeftWidth: '4px',
        borderLeftColor: 'var(--status-warning-dot)',
      }}
    >
      <div className="flex items-center gap-2" style={{ color: 'var(--status-warning-text)' }}>
        <AlertTriangle size={18} />
        <h3 className="text-sm font-semibold uppercase tracking-wide">Peringatan Komposisi</h3>
      </div>
      <p className="text-sm" style={{ color: 'var(--status-warning-text)' }}>{compositionCheck.warning}</p>
      <div className="grid md:grid-cols-2 gap-4">
        <div>
          <div className="text-xs font-medium mb-2" style={{ color: 'var(--text-muted)' }}>Komposisi A</div>
          <div className="space-y-1">
            {allClasses.map(cls => (
              <div key={cls} className="flex items-center gap-2">
                <div className="flex-1">
                  <div className="flex justify-between text-xs mb-0.5" style={{ color: 'var(--text-secondary)' }}>
                    <span>{cls || 'Unknown'}</span>
                    <span>{compA[cls] || 0}%</span>
                  </div>
                  <div className="h-2 rounded-full overflow-hidden" style={{ backgroundColor: 'var(--bg-hover)' }}>
                    <div className="h-full rounded-full" style={{ width: `${compA[cls] || 0}%`, backgroundColor: 'var(--chart-secondary)' }} />
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
        <div>
          <div className="text-xs font-medium mb-2" style={{ color: 'var(--text-muted)' }}>Komposisi B</div>
          <div className="space-y-1">
            {allClasses.map(cls => (
              <div key={cls} className="flex items-center gap-2">
                <div className="flex-1">
                  <div className="flex justify-between text-xs mb-0.5" style={{ color: 'var(--text-secondary)' }}>
                    <span>{cls || 'Unknown'}</span>
                    <span>{compB[cls] || 0}%</span>
                  </div>
                  <div className="h-2 rounded-full overflow-hidden" style={{ backgroundColor: 'var(--bg-hover)' }}>
                    <div className="h-full rounded-full" style={{ width: `${compB[cls] || 0}%`, backgroundColor: 'var(--chart-secondary)' }} />
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
      <div className="text-xs" style={{ color: 'var(--status-warning-text)' }}>
        Total perbedaan komposisi: <span className="font-semibold">{compositionCheck.total_diff_pp}pp</span>
      </div>
    </div>
  );
}
