import { FileDown, FileSpreadsheet, Eye } from 'lucide-react';

function DownloadButtons({ report, onPreview }) {
  return (
    <div className="flex items-center gap-1">
      {report.pdf_url && (
        <a
          href={report.pdf_url}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-1 text-xs px-2 py-1 rounded bg-red-50 text-red-600 hover:bg-red-100 transition-colors"
          title="Download PDF"
        >
          <FileDown size={14} />
          PDF
        </a>
      )}
      {report.excel_url && (
        <a
          href={report.excel_url}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-1 text-xs px-2 py-1 rounded bg-green-50 text-green-600 hover:bg-green-100 transition-colors"
          title="Download Excel"
        >
          <FileSpreadsheet size={14} />
          XLS
        </a>
      )}
      {onPreview && (
        <button
          onClick={() => onPreview(report.id)}
          className="inline-flex items-center gap-1 text-xs px-2 py-1 rounded bg-blue-50 text-blue-600 hover:bg-blue-100 transition-colors"
          title="Preview"
        >
          <Eye size={14} />
        </button>
      )}
    </div>
  );
}

export default DownloadButtons;
