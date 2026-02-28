import { useState } from 'react';
import { AlertTriangle } from 'lucide-react';

function DangerAction({ title, description, buttonLabel, confirmText, onExecute, loading }) {
  const [showConfirm, setShowConfirm] = useState(false);
  const [inputValue, setInputValue] = useState('');

  const handleClick = () => {
    if (!showConfirm) {
      setShowConfirm(true);
      return;
    }
  };

  const handleConfirm = async () => {
    await onExecute();
    setShowConfirm(false);
    setInputValue('');
  };

  const handleCancel = () => {
    setShowConfirm(false);
    setInputValue('');
  };

  return (
    <div className="border border-red-200 rounded-lg p-4 bg-red-50/50">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h4 className="font-medium text-gray-900 text-sm">{title}</h4>
          <p className="text-xs text-gray-500 mt-1">{description}</p>
        </div>
        {!showConfirm && (
          <button
            onClick={handleClick}
            disabled={loading}
            className="px-4 py-1.5 bg-red-600 text-white text-xs font-medium rounded-lg hover:bg-red-700 transition-colors shrink-0 disabled:opacity-50"
          >
            {buttonLabel}
          </button>
        )}
      </div>

      {showConfirm && (
        <div className="mt-3 border-t border-red-200 pt-3">
          <p className="text-xs text-red-600 mb-2">
            Ketik <strong>{confirmText}</strong> untuk konfirmasi:
          </p>
          <div className="flex items-center gap-2">
            <input
              type="text"
              value={inputValue}
              onChange={(e) => setInputValue(e.target.value)}
              className="flex-1 px-3 py-1.5 text-xs border border-red-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-red-500 bg-white"
              placeholder={`Ketik "${confirmText}"`}
            />
            <button
              onClick={handleConfirm}
              disabled={inputValue !== confirmText || loading}
              className="px-4 py-1.5 bg-red-600 text-white text-xs font-medium rounded-lg hover:bg-red-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {loading ? 'Memproses...' : 'Konfirmasi'}
            </button>
            <button
              onClick={handleCancel}
              className="px-4 py-1.5 bg-gray-200 text-gray-700 text-xs font-medium rounded-lg hover:bg-gray-300 transition-colors"
            >
              Batal
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

function DangerZone({ onDeleteData, onResetDatabase, loading }) {
  return (
    <div className="border border-red-300 rounded-xl bg-white">
      <div className="px-4 py-3 border-b border-red-200 flex items-center gap-2">
        <AlertTriangle size={18} className="text-red-500" />
        <h3 className="font-semibold text-red-700 text-sm">Zona Berbahaya</h3>
      </div>
      <div className="p-4 space-y-3">
        <DangerAction
          title="Hapus Semua Data"
          description="Menghapus seluruh data tiket dan summary. Master data TIDAK terhapus."
          buttonLabel="Hapus Data"
          confirmText="HAPUS"
          onExecute={onDeleteData}
          loading={loading}
        />
        <DangerAction
          title="Reset Database"
          description="Menghapus SELURUH database termasuk master data. Backup otomatis dibuat sebelum reset."
          buttonLabel="Reset Database"
          confirmText="HAPUS"
          onExecute={onResetDatabase}
          loading={loading}
        />
      </div>
    </div>
  );
}

export default DangerZone;
