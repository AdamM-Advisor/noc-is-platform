import { AlertTriangle } from 'lucide-react';

export default function CompositionWarning({ compositionCheck }) {
  if (!compositionCheck) return null;
  if (compositionCheck.similar) return null;

  const compA = compositionCheck.composition_a || {};
  const compB = compositionCheck.composition_b || {};
  const allClasses = [...new Set([...Object.keys(compA), ...Object.keys(compB)])].sort();

  return (
    <div className="bg-amber-50 border-2 border-amber-300 rounded-lg p-5 space-y-3">
      <div className="flex items-center gap-2 text-amber-800">
        <AlertTriangle size={20} />
        <h3 className="text-sm font-semibold uppercase tracking-wide">Peringatan Komposisi</h3>
      </div>
      <p className="text-sm text-amber-700">{compositionCheck.warning}</p>
      <div className="grid md:grid-cols-2 gap-4">
        <div>
          <div className="text-xs font-medium text-gray-500 mb-2">Komposisi A</div>
          <div className="space-y-1">
            {allClasses.map(cls => (
              <div key={cls} className="flex items-center gap-2">
                <div className="flex-1">
                  <div className="flex justify-between text-xs text-gray-600 mb-0.5">
                    <span>{cls || 'Unknown'}</span>
                    <span>{compA[cls] || 0}%</span>
                  </div>
                  <div className="h-2 bg-gray-200 rounded-full overflow-hidden">
                    <div className="h-full bg-blue-400 rounded-full" style={{ width: `${compA[cls] || 0}%` }} />
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
        <div>
          <div className="text-xs font-medium text-gray-500 mb-2">Komposisi B</div>
          <div className="space-y-1">
            {allClasses.map(cls => (
              <div key={cls} className="flex items-center gap-2">
                <div className="flex-1">
                  <div className="flex justify-between text-xs text-gray-600 mb-0.5">
                    <span>{cls || 'Unknown'}</span>
                    <span>{compB[cls] || 0}%</span>
                  </div>
                  <div className="h-2 bg-gray-200 rounded-full overflow-hidden">
                    <div className="h-full bg-emerald-400 rounded-full" style={{ width: `${compB[cls] || 0}%` }} />
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
      <div className="text-xs text-amber-600">
        Total perbedaan komposisi: <span className="font-semibold">{compositionCheck.total_diff_pp}pp</span>
      </div>
    </div>
  );
}
