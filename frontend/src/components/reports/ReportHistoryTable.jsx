import DownloadButtons from './DownloadButtons';

const TYPE_LABELS = {
  daily: 'Harian',
  weekly: 'Mingguan',
  monthly: 'Bulanan',
  quarterly: 'Triwulan',
  annual: 'Tahunan',
};

const STATUS_STYLES = {
  completed: 'bg-green-100 text-green-700',
  generating: 'bg-blue-100 text-blue-700',
  pending: 'bg-yellow-100 text-yellow-700',
  failed: 'bg-red-100 text-red-700',
};

function ReportHistoryTable({ reports, total, page, perPage, onPageChange, onDelete, onPreview }) {
  const totalPages = Math.ceil(total / perPage);

  if (!reports || reports.length === 0) {
    return (
      <div className="bg-white rounded-xl border border-gray-200 p-8 text-center text-gray-500">
        Belum ada riwayat laporan.
      </div>
    );
  }

  return (
    <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-50 border-b border-gray-200">
              <th className="text-left px-4 py-3 font-medium text-gray-600">#</th>
              <th className="text-left px-4 py-3 font-medium text-gray-600">Jenis</th>
              <th className="text-left px-4 py-3 font-medium text-gray-600">Entitas</th>
              <th className="text-left px-4 py-3 font-medium text-gray-600">Periode</th>
              <th className="text-left px-4 py-3 font-medium text-gray-600">Status</th>
              <th className="text-left px-4 py-3 font-medium text-gray-600">Waktu</th>
              <th className="text-left px-4 py-3 font-medium text-gray-600">Ukuran</th>
              <th className="text-left px-4 py-3 font-medium text-gray-600">Aksi</th>
            </tr>
          </thead>
          <tbody>
            {reports.map((r, i) => (
              <tr key={r.id} className="border-b border-gray-100 hover:bg-gray-50">
                <td className="px-4 py-3 text-gray-500">{(page - 1) * perPage + i + 1}</td>
                <td className="px-4 py-3">
                  <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-50 text-blue-700">
                    {TYPE_LABELS[r.report_type] || r.report_type}
                  </span>
                </td>
                <td className="px-4 py-3">
                  <div className="font-medium text-gray-800">{r.entity_name || r.entity_id}</div>
                  <div className="text-xs text-gray-400">{r.entity_level}</div>
                </td>
                <td className="px-4 py-3 text-gray-600">{r.period_label || `${r.period_start} — ${r.period_end}`}</td>
                <td className="px-4 py-3">
                  <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${STATUS_STYLES[r.status] || 'bg-gray-100 text-gray-600'}`}>
                    {r.status}
                  </span>
                </td>
                <td className="px-4 py-3 text-gray-500 text-xs">
                  {r.generation_time_ms ? `${(r.generation_time_ms / 1000).toFixed(1)}s` : '-'}
                </td>
                <td className="px-4 py-3 text-gray-500 text-xs">
                  {r.pdf_size_kb ? `PDF: ${r.pdf_size_kb}KB` : ''}
                  {r.excel_size_kb ? ` XLS: ${r.excel_size_kb}KB` : ''}
                </td>
                <td className="px-4 py-3">
                  <div className="flex items-center gap-1">
                    {r.status === 'completed' && (
                      <DownloadButtons report={r} onPreview={onPreview} />
                    )}
                    <button
                      onClick={() => onDelete(r.id)}
                      className="text-red-500 hover:text-red-700 text-xs px-2 py-1 rounded hover:bg-red-50"
                      title="Hapus"
                    >
                      Hapus
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {totalPages > 1 && (
        <div className="flex items-center justify-between px-4 py-3 border-t border-gray-200">
          <span className="text-sm text-gray-500">
            {total} laporan — Halaman {page}/{totalPages}
          </span>
          <div className="flex gap-1">
            <button
              onClick={() => onPageChange(page - 1)}
              disabled={page <= 1}
              className="px-3 py-1 rounded text-sm border border-gray-300 disabled:opacity-50 hover:bg-gray-50"
            >
              Prev
            </button>
            <button
              onClick={() => onPageChange(page + 1)}
              disabled={page >= totalPages}
              className="px-3 py-1 rounded text-sm border border-gray-300 disabled:opacity-50 hover:bg-gray-50"
            >
              Next
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

export default ReportHistoryTable;
