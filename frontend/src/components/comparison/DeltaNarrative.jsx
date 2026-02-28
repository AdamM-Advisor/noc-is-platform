import { MessageSquare } from 'lucide-react';

export default function DeltaNarrative({ narrative, comparisonType, profileA, profileB }) {
  if (!narrative) return null;

  const typeLabels = {
    temporal: 'Perbandingan Temporal',
    entity: 'Perbandingan Entitas',
    fault: 'Perbandingan Filter',
  };

  return (
    <div className="bg-white rounded-lg border p-5 space-y-3">
      <div className="flex items-center gap-2">
        <MessageSquare size={18} className="text-blue-600" />
        <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide">Narasi Perbandingan</h3>
        <span className="ml-auto px-2 py-0.5 bg-gray-100 rounded-full text-xs font-medium text-gray-600">
          {typeLabels[comparisonType] || comparisonType}
        </span>
      </div>
      <div className="flex items-center gap-4 text-xs text-gray-500">
        <span className="px-2 py-1 bg-blue-50 border border-blue-200 rounded">
          A: {profileA?.entity_name || profileA?.entity_id}
          {profileA?.date_from && ` (${profileA.date_from}–${profileA.date_to})`}
        </span>
        <span>vs</span>
        <span className="px-2 py-1 bg-emerald-50 border border-emerald-200 rounded">
          B: {profileB?.entity_name || profileB?.entity_id}
          {profileB?.date_from && ` (${profileB.date_from}–${profileB.date_to})`}
        </span>
      </div>
      <p className="text-sm text-gray-700 leading-relaxed bg-gray-50 rounded-lg p-4">
        {narrative}
      </p>
    </div>
  );
}
