import { useState, useRef, useCallback } from 'react';
import { Upload, FileUp, CheckCircle, XCircle, RefreshCw } from 'lucide-react';
import { uploadClient } from '../api/client';

const SINGLE_LIMIT_MB = 10;
const CHUNK_SIZE = 5 * 1024 * 1024;
const ALLOWED_TYPES = ['.xlsx', '.csv', '.parquet'];

function UploadPage() {
  const [uploads, setUploads] = useState([]);
  const [dragOver, setDragOver] = useState(false);
  const fileInputRef = useRef(null);

  const getExtension = (name) => {
    const dot = name.lastIndexOf('.');
    return dot >= 0 ? name.substring(dot).toLowerCase() : '';
  };

  const addUpload = (file) => {
    const ext = getExtension(file.name);
    if (!ALLOWED_TYPES.includes(ext)) {
      alert(`Tipe file tidak didukung: ${ext}. Gunakan .xlsx, .csv, atau .parquet`);
      return;
    }
    const id = Date.now().toString() + Math.random().toString(36).slice(2);
    const entry = {
      id,
      file,
      filename: file.name,
      size: file.size,
      status: 'pending',
      progress: 0,
      chunkInfo: null,
      error: null,
    };
    setUploads((prev) => [entry, ...prev]);
    startUpload(entry);
  };

  const updateUpload = (id, updates) => {
    setUploads((prev) =>
      prev.map((u) => (u.id === id ? { ...u, ...updates } : u))
    );
  };

  const startUpload = async (entry) => {
    const sizeMB = entry.size / (1024 * 1024);
    updateUpload(entry.id, { status: 'uploading', progress: 0 });

    try {
      if (sizeMB < SINGLE_LIMIT_MB) {
        await singleUpload(entry);
      } else {
        await chunkedUpload(entry);
      }
    } catch (err) {
      updateUpload(entry.id, {
        status: 'error',
        error: err.response?.data?.detail || err.message || 'Upload gagal',
      });
    }
  };

  const singleUpload = async (entry) => {
    const formData = new FormData();
    formData.append('file', entry.file);

    await uploadClient.post('/upload/single', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
      onUploadProgress: (e) => {
        if (e.total) {
          const pct = Math.round((e.loaded / e.total) * 100);
          updateUpload(entry.id, { progress: pct });
        }
      },
    });

    updateUpload(entry.id, { status: 'success', progress: 100 });
  };

  const chunkedUpload = async (entry) => {
    const uploadId = crypto.randomUUID();
    const totalChunks = Math.ceil(entry.size / CHUNK_SIZE);

    for (let i = 0; i < totalChunks; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, entry.size);
      const chunk = entry.file.slice(start, end);

      updateUpload(entry.id, {
        progress: Math.round((i / totalChunks) * 100),
        chunkInfo: `Chunk ${i + 1}/${totalChunks}`,
      });

      await uploadClient.post('/upload/chunk', chunk, {
        headers: {
          'Content-Type': 'application/octet-stream',
          'X-Upload-Id': uploadId,
          'X-Chunk-Index': i.toString(),
          'X-Total-Chunks': totalChunks.toString(),
          'X-Filename': entry.filename,
        },
      });
    }

    updateUpload(entry.id, {
      progress: 95,
      chunkInfo: 'Memproses...',
    });

    await uploadClient.post('/upload/chunk/complete', {
      upload_id: uploadId,
      filename: entry.filename,
      total_chunks: totalChunks,
    });

    updateUpload(entry.id, {
      status: 'success',
      progress: 100,
      chunkInfo: null,
    });
  };

  const retryUpload = (entry) => {
    startUpload(entry);
  };

  const handleDrop = useCallback((e) => {
    e.preventDefault();
    setDragOver(false);
    const files = Array.from(e.dataTransfer.files);
    files.forEach(addUpload);
  }, []);

  const handleFileSelect = (e) => {
    const files = Array.from(e.target.files);
    files.forEach(addUpload);
    e.target.value = '';
  };

  const formatSize = (bytes) => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  };

  return (
    <div>
      <h2 className="text-xl font-bold text-gray-800 mb-6">Upload Data</h2>

      <div
        className={`border-2 border-dashed rounded-xl p-10 text-center transition-colors cursor-pointer ${
          dragOver
            ? 'border-blue-500 bg-blue-50'
            : 'border-gray-300 bg-white hover:border-blue-400 hover:bg-gray-50'
        }`}
        onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
        onDragLeave={() => setDragOver(false)}
        onDrop={handleDrop}
        onClick={() => fileInputRef.current?.click()}
      >
        <FileUp size={40} className="mx-auto text-gray-400 mb-3" />
        <p className="text-sm text-gray-600 font-medium mb-1">
          Drag & drop file di sini atau klik untuk memilih
        </p>
        <p className="text-xs text-gray-400">
          Format: .xlsx, .csv, .parquet — File &lt; 10 MB: upload langsung, &ge; 10 MB: chunked upload
        </p>
        <input
          ref={fileInputRef}
          type="file"
          accept=".xlsx,.csv,.parquet"
          className="hidden"
          onChange={handleFileSelect}
          multiple
        />
      </div>

      {uploads.length > 0 && (
        <div className="mt-6 space-y-3">
          <h3 className="text-sm font-semibold text-gray-700">Riwayat Upload</h3>
          {uploads.map((entry) => (
            <div key={entry.id} className="bg-white border border-gray-200 rounded-lg p-4">
              <div className="flex items-center justify-between mb-2">
                <div className="flex items-center gap-2 min-w-0">
                  {entry.status === 'success' && <CheckCircle size={16} className="text-green-500 shrink-0" />}
                  {entry.status === 'error' && <XCircle size={16} className="text-red-500 shrink-0" />}
                  {(entry.status === 'uploading' || entry.status === 'pending') && (
                    <Upload size={16} className="text-blue-500 shrink-0 animate-pulse" />
                  )}
                  <span className="text-sm font-medium text-gray-800 truncate">{entry.filename}</span>
                  <span className="text-xs text-gray-400 shrink-0">{formatSize(entry.size)}</span>
                </div>
                <div className="flex items-center gap-2 shrink-0">
                  {entry.status === 'uploading' && entry.chunkInfo && (
                    <span className="text-xs text-blue-600">{entry.chunkInfo}</span>
                  )}
                  {entry.status === 'success' && (
                    <span className="text-xs text-green-600 font-medium">Selesai</span>
                  )}
                  {entry.status === 'error' && (
                    <button
                      onClick={() => retryUpload(entry)}
                      className="flex items-center gap-1 text-xs text-red-600 hover:text-red-700"
                    >
                      <RefreshCw size={12} />
                      Retry
                    </button>
                  )}
                </div>
              </div>

              {(entry.status === 'uploading' || entry.status === 'pending') && (
                <div className="w-full bg-gray-100 rounded-full h-2">
                  <div
                    className="bg-blue-500 h-2 rounded-full transition-all duration-300"
                    style={{ width: `${entry.progress}%` }}
                  />
                </div>
              )}

              {entry.status === 'uploading' && (
                <p className="text-xs text-gray-500 mt-1">
                  {entry.progress < 100 ? 'Mengunggah...' : 'Memproses...'}
                  {' '}{entry.progress}%
                </p>
              )}

              {entry.status === 'error' && entry.error && (
                <p className="text-xs text-red-500 mt-1">{entry.error}</p>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export default UploadPage;
