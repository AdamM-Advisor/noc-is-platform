export default function OverallStatusBanner({ status }) {
  if (!status) return null;

  const bgMap = {
    'KRITIS': 'bg-red-50 border-red-200',
    'PERLU PERHATIAN': 'bg-amber-50 border-amber-200',
    'BAIK': 'bg-green-50 border-green-200',
    'SANGAT BAIK': 'bg-emerald-50 border-emerald-200',
  };

  const textMap = {
    'KRITIS': 'text-red-800',
    'PERLU PERHATIAN': 'text-amber-800',
    'BAIK': 'text-green-800',
    'SANGAT BAIK': 'text-emerald-800',
  };

  return (
    <div className={`rounded-xl border-2 p-5 ${bgMap[status.status] || 'bg-gray-50 border-gray-200'}`}>
      <div className="flex items-center gap-3 mb-2">
        <span className="text-2xl">{status.icon}</span>
        <h3 className={`text-lg font-bold ${textMap[status.status] || 'text-gray-800'}`}>
          {status.status}
        </h3>
      </div>
      {status.narrative && (
        <p className={`text-sm leading-relaxed ${textMap[status.status] || 'text-gray-600'}`}>
          {status.narrative}
        </p>
      )}
    </div>
  );
}
