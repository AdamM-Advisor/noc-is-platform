import { useState, useEffect, useCallback } from 'react';
import ReportForm from '../components/reports/ReportForm';
import ReportHistoryTable from '../components/reports/ReportHistoryTable';

function ReportGeneratorPage() {
  const [loading, setLoading] = useState(false);
  const [reports, setReports] = useState([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [filterType, setFilterType] = useState('');
  const [result, setResult] = useState(null);
  const [previewHtml, setPreviewHtml] = useState('');
  const [showPreview, setShowPreview] = useState(false);
  const perPage = 10;

  const fetchHistory = useCallback(async () => {
    try {
      const params = new URLSearchParams({ page: String(page), per_page: String(perPage) });
      if (filterType) params.append('report_type', filterType);
      const res = await fetch(`/api/reports?${params}`);
      const data = await res.json();
      setReports(data.reports || []);
      setTotal(data.total || 0);
    } catch {
      setReports([]);
    }
  }, [page, filterType]);

  useEffect(() => { fetchHistory(); }, [fetchHistory]);

  const handleGenerate = async (formData) => {
    setLoading(true);
    setResult(null);
    try {
      const res = await fetch('/api/reports/generate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(formData),
      });
      const data = await res.json();
      setResult(data);
      fetchHistory();
    } catch (err) {
      setResult({ status: 'failed', error: err.message });
    } finally {
      setLoading(false);
    }
  };

  const handleDelete = async (id) => {
    if (!confirm('Hapus laporan ini?')) return;
    try {
      await fetch(`/api/reports/${id}`, { method: 'DELETE' });
      fetchHistory();
    } catch {}
  };

  const handlePreview = async (id) => {
    try {
      const res = await fetch(`/api/reports/${id}/preview`);
      const html = await res.text();
      setPreviewHtml(html);
      setShowPreview(true);
    } catch {}
  };

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-gray-800">Report Generator</h2>
        <p className="text-sm text-gray-500 mt-1">Generate laporan NOC-IS dalam format PDF dan Excel</p>
      </div>

      <ReportForm onGenerate={handleGenerate} loading={loading} />

      {result && (
        <div className={`rounded-lg p-4 ${result.status === 'completed' ? 'bg-green-50 border border-green-200' : 'bg-red-50 border border-red-200'}`}>
          {result.status === 'completed' ? (
            <div className="space-y-2">
              <p className="text-green-700 font-medium">Laporan berhasil di-generate ({(result.generation_time_ms / 1000).toFixed(1)}s)</p>
              <div className="flex gap-3">
                {result.pdf_url && (
                  <a href={result.pdf_url} target="_blank" rel="noopener noreferrer"
                     className="inline-flex items-center gap-2 px-4 py-2 bg-red-600 text-white rounded-lg text-sm hover:bg-red-700">
                    Download PDF
                  </a>
                )}
                {result.excel_url && (
                  <a href={result.excel_url} target="_blank" rel="noopener noreferrer"
                     className="inline-flex items-center gap-2 px-4 py-2 bg-green-600 text-white rounded-lg text-sm hover:bg-green-700">
                    Download Excel
                  </a>
                )}
              </div>
            </div>
          ) : (
            <p className="text-red-700">Gagal: {result.error || 'Unknown error'}</p>
          )}
        </div>
      )}

      <div>
        <div className="flex items-center justify-between mb-3">
          <h3 className="text-lg font-semibold text-gray-800">Riwayat Laporan</h3>
          <select
            value={filterType}
            onChange={e => { setFilterType(e.target.value); setPage(1); }}
            className="border border-gray-300 rounded-lg px-3 py-1.5 text-sm"
          >
            <option value="">Semua Jenis</option>
            <option value="daily">Harian</option>
            <option value="weekly">Mingguan</option>
            <option value="monthly">Bulanan</option>
            <option value="quarterly">Triwulan</option>
            <option value="annual">Tahunan</option>
          </select>
        </div>
        <ReportHistoryTable
          reports={reports}
          total={total}
          page={page}
          perPage={perPage}
          onPageChange={setPage}
          onDelete={handleDelete}
          onPreview={handlePreview}
        />
      </div>

      {showPreview && (
        <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
          <div className="bg-white rounded-xl w-full max-w-5xl max-h-[90vh] flex flex-col">
            <div className="flex items-center justify-between p-4 border-b border-gray-200">
              <h3 className="font-semibold text-gray-800">Preview Laporan</h3>
              <button
                onClick={() => setShowPreview(false)}
                className="text-gray-500 hover:text-gray-700 text-lg font-bold px-2"
              >
                &times;
              </button>
            </div>
            <div className="flex-1 overflow-auto p-4">
              <iframe
                srcDoc={previewHtml}
                className="w-full h-full min-h-[70vh] border-0"
                title="Report Preview"
              />
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default ReportGeneratorPage;
