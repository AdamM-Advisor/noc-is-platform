import { AlertCircle, RefreshCw, Inbox } from 'lucide-react';

function LoadingWrapper({ loading, error, empty, children, onRetry, emptyMessage }) {
  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center py-16 text-gray-500">
        <div className="w-8 h-8 border-3 border-blue-500 border-t-transparent rounded-full animate-spin mb-4" />
        <p className="text-sm">Memuat data...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex flex-col items-center justify-center py-16">
        <AlertCircle size={40} className="text-red-400 mb-3" />
        <p className="text-sm text-red-600 mb-4">
          {typeof error === 'string' ? error : 'Gagal memuat data.'}
        </p>
        {onRetry && (
          <button
            onClick={onRetry}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg text-sm hover:bg-blue-700 transition-colors"
          >
            <RefreshCw size={14} />
            Coba Lagi
          </button>
        )}
      </div>
    );
  }

  if (empty) {
    return (
      <div className="flex flex-col items-center justify-center py-16 text-gray-400">
        <Inbox size={40} className="mb-3" />
        <p className="text-sm">{emptyMessage || 'Belum ada data. Upload data terlebih dahulu.'}</p>
      </div>
    );
  }

  return children;
}

export default LoadingWrapper;
