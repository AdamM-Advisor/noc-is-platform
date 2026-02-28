import { useState, useRef, useCallback, useEffect } from 'react';
import { Upload, FileUp, CheckCircle, XCircle, RefreshCw, AlertTriangle, Clock, ChevronDown, Trash2 } from 'lucide-react';
import { uploadClient } from '../api/client';
import client from '../api/client';

const SINGLE_LIMIT_MB = 10;
const CHUNK_SIZE = 5 * 1024 * 1024;
const ALLOWED_TYPES = ['.xlsx', '.csv', '.parquet'];

const FILE_TYPE_LABELS = {
  site_master: 'Site Master',
  swfm_event: 'SWFM Event',
  swfm_incident: 'SWFM Incident',
  swfm_realtime: 'SWFM Realtime',
  fault_center: 'Fault Center',
};

const PHASE_LABELS = {
  reading: 'Membaca file...',
  normalizing: 'Menormalisasi header...',
  deduplicating: 'Mendeteksi duplikat...',
  calculating: 'Menghitung kolom kalkulasi...',
  resolving: 'Meresolusi hierarki...',
  inserting: 'Menyimpan ke database...',
  summarizing: 'Memperbarui summary...',
  completed: 'Selesai',
  failed: 'Gagal',
};

function UploadPage() {
  const [dragOver, setDragOver] = useState(false);
  const [selectedFile, setSelectedFile] = useState(null);
  const [uploadStatus, setUploadStatus] = useState('idle');
  const [uploadProgress, setUploadProgress] = useState(0);
  const [detection, setDetection] = useState(null);
  const [fileTypeOverride, setFileTypeOverride] = useState('auto');
  const [processingStatus, setProcessingStatus] = useState(null);
  const [processingResult, setProcessingResult] = useState(null);
  const [importHistory, setImportHistory] = useState([]);
  const [error, setError] = useState(null);
  const [isProcessing, setIsProcessing] = useState(false);
  const fileInputRef = useRef(null);
  const pollRef = useRef(null);

  useEffect(() => {
    loadImportHistory();
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, []);

  const loadImportHistory = async () => {
    try {
      const res = await client.get('/imports');
      setImportHistory(res.data);
    } catch (e) {}
  };

  const getExtension = (name) => {
    const dot = name.lastIndexOf('.');
    return dot >= 0 ? name.substring(dot).toLowerCase() : '';
  };

  const formatSize = (bytes) => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  };

  const formatDuration = (sec) => {
    if (!sec) return '-';
    if (sec < 60) return `${sec.toFixed(1)} detik`;
    const min = Math.floor(sec / 60);
    const s = Math.round(sec % 60);
    return `${min} menit ${s} detik`;
  };

  const resetState = () => {
    setSelectedFile(null);
    setUploadStatus('idle');
    setUploadProgress(0);
    setDetection(null);
    setFileTypeOverride('auto');
    setProcessingStatus(null);
    setProcessingResult(null);
    setError(null);
    setIsProcessing(false);
    if (pollRef.current) clearInterval(pollRef.current);
  };

  const handleFileSelect = (file) => {
    const ext = getExtension(file.name);
    if (!ALLOWED_TYPES.includes(ext)) {
      setError(`Tipe file tidak didukung: ${ext}. Gunakan .xlsx, .csv, atau .parquet`);
      return;
    }
    resetState();
    setSelectedFile(file);
    uploadFile(file);
  };

  const uploadFile = async (file) => {
    setUploadStatus('uploading');
    setUploadProgress(0);
    setError(null);

    try {
      const sizeMB = file.size / (1024 * 1024);
      if (sizeMB < SINGLE_LIMIT_MB) {
        const formData = new FormData();
        formData.append('file', file);
        await uploadClient.post('/upload/single', formData, {
          headers: { 'Content-Type': 'multipart/form-data' },
          onUploadProgress: (e) => {
            if (e.total) setUploadProgress(Math.round((e.loaded / e.total) * 100));
          },
        });
      } else {
        const uploadId = crypto.randomUUID();
        const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
        for (let i = 0; i < totalChunks; i++) {
          const start = i * CHUNK_SIZE;
          const end = Math.min(start + CHUNK_SIZE, file.size);
          const chunk = file.slice(start, end);
          setUploadProgress(Math.round((i / totalChunks) * 100));
          await uploadClient.post('/upload/chunk', chunk, {
            headers: {
              'Content-Type': 'application/octet-stream',
              'X-Upload-Id': uploadId,
              'X-Chunk-Index': i.toString(),
              'X-Total-Chunks': totalChunks.toString(),
              'X-Filename': file.name,
            },
          });
        }
        setUploadProgress(95);
        await uploadClient.post('/upload/chunk/complete', {
          upload_id: uploadId,
          filename: file.name,
          total_chunks: totalChunks,
        });
      }

      setUploadProgress(100);
      setUploadStatus('uploaded');
      detectFileType(file.name);
    } catch (err) {
      setUploadStatus('error');
      setError(err.response?.data?.detail || err.message || 'Upload gagal');
    }
  };

  const detectFileType = async (filename) => {
    try {
      const res = await client.post('/upload/detect', { filename });
      setDetection(res.data);
    } catch (e) {
      setDetection({ file_type: 'unknown', confidence: 'low', reason: 'Detection failed' });
    }
  };

  const startProcessing = async () => {
    if (!selectedFile) return;

    setIsProcessing(true);
    setProcessingResult(null);
    setError(null);
    setProcessingStatus({ phase: 'reading', detail: 'Memulai...', row: 0, total: 0 });

    try {
      const fileType = fileTypeOverride !== 'auto' ? fileTypeOverride : 'auto';
      const res = await client.post('/upload/process', {
        filename: selectedFile.name,
        file_type: fileType,
      });

      const jobId = res.data.job_id;
      pollRef.current = setInterval(async () => {
        try {
          const statusRes = await client.get(`/upload/process/status/${jobId}`);
          const job = statusRes.data;
          setProcessingStatus(job.progress);

          if (job.status === 'completed') {
            clearInterval(pollRef.current);
            setProcessingResult(job.result);
            setIsProcessing(false);
            loadImportHistory();
          } else if (job.status === 'failed') {
            clearInterval(pollRef.current);
            setError(job.error || 'Processing gagal');
            setIsProcessing(false);
          }
        } catch (e) {
          clearInterval(pollRef.current);
          setError('Gagal memeriksa status processing');
          setIsProcessing(false);
        }
      }, 1000);
    } catch (err) {
      setError(err.response?.data?.detail || err.message || 'Processing gagal');
      setIsProcessing(false);
    }
  };

  const deleteImport = async (id) => {
    if (!confirm('Hapus data import ini? Data tiket terkait juga akan dihapus.')) return;
    try {
      await client.delete(`/imports/${id}`);
      loadImportHistory();
    } catch (e) {
      setError('Gagal menghapus import');
    }
  };

  const handleDrop = useCallback((e) => {
    e.preventDefault();
    setDragOver(false);
    const files = Array.from(e.dataTransfer.files);
    if (files.length > 0) handleFileSelect(files[0]);
  }, []);

  const handleInputChange = (e) => {
    const files = Array.from(e.target.files);
    if (files.length > 0) handleFileSelect(files[0]);
    e.target.value = '';
  };

  const getPhaseProgress = () => {
    const phases = ['reading', 'normalizing', 'deduplicating', 'calculating', 'resolving', 'inserting', 'summarizing', 'completed'];
    const currentPhase = processingStatus?.phase || 'reading';
    const idx = phases.indexOf(currentPhase);
    if (idx < 0) return 0;
    return Math.round(((idx + 1) / phases.length) * 100);
  };

  const confidenceColor = (c) => {
    if (c === 'high') return 'text-green-600';
    if (c === 'medium') return 'text-yellow-600';
    return 'text-red-600';
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
        onClick={() => !isProcessing && fileInputRef.current?.click()}
      >
        <FileUp size={40} className="mx-auto text-gray-400 mb-3" />
        <p className="text-sm text-gray-600 font-medium mb-1">
          Drag & drop file di sini atau klik untuk memilih
        </p>
        <p className="text-xs text-gray-400">
          Format: .xlsx, .csv, .parquet
        </p>
        <input
          ref={fileInputRef}
          type="file"
          accept=".xlsx,.csv,.parquet"
          className="hidden"
          onChange={handleInputChange}
        />
      </div>

      {error && (
        <div className="mt-4 bg-red-50 border border-red-200 rounded-lg p-3 flex items-start gap-2">
          <XCircle size={16} className="text-red-500 mt-0.5 shrink-0" />
          <p className="text-sm text-red-700">{error}</p>
        </div>
      )}

      {selectedFile && (
        <div className="mt-6 bg-white border border-gray-200 rounded-lg p-5">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-3">
              <Upload size={20} className="text-blue-500" />
              <div>
                <p className="text-sm font-semibold text-gray-800">{selectedFile.name}</p>
                <p className="text-xs text-gray-400">{formatSize(selectedFile.size)}</p>
              </div>
            </div>
            {!isProcessing && uploadStatus !== 'uploading' && (
              <button
                onClick={resetState}
                className="text-xs text-gray-400 hover:text-gray-600"
              >
                Hapus
              </button>
            )}
          </div>

          {uploadStatus === 'uploading' && (
            <div className="mb-4">
              <div className="flex justify-between text-xs text-gray-500 mb-1">
                <span>Mengunggah...</span>
                <span>{uploadProgress}%</span>
              </div>
              <div className="w-full bg-gray-100 rounded-full h-2">
                <div className="bg-blue-500 h-2 rounded-full transition-all" style={{ width: `${uploadProgress}%` }} />
              </div>
            </div>
          )}

          {detection && uploadStatus === 'uploaded' && (
            <div className="mb-4 space-y-3">
              <div className="bg-gray-50 rounded-lg p-3">
                <div className="grid grid-cols-2 gap-2 text-sm">
                  <div>
                    <span className="text-gray-500">Tipe Terdeteksi:</span>{' '}
                    <span className="font-semibold">{FILE_TYPE_LABELS[detection.file_type] || detection.file_type}</span>
                    {detection.confidence === 'high' && <CheckCircle size={14} className="inline ml-1 text-green-500" />}
                    {detection.confidence === 'medium' && <AlertTriangle size={14} className="inline ml-1 text-yellow-500" />}
                  </div>
                  <div>
                    <span className="text-gray-500">Confidence:</span>{' '}
                    <span className={`font-medium ${confidenceColor(detection.confidence)}`}>{detection.confidence}</span>
                  </div>
                  <div>
                    <span className="text-gray-500">Header Format:</span>{' '}
                    <span className="font-medium">{detection.header_format}</span>
                  </div>
                  <div>
                    <span className="text-gray-500">Kolom:</span>{' '}
                    <span className="font-medium">{detection.total_columns}</span>
                  </div>
                </div>
              </div>

              <div className="flex items-center gap-3">
                <label className="text-sm text-gray-600">Ubah Tipe:</label>
                <div className="relative">
                  <select
                    value={fileTypeOverride}
                    onChange={(e) => setFileTypeOverride(e.target.value)}
                    disabled={isProcessing}
                    className="appearance-none bg-white border border-gray-300 rounded-lg px-3 py-1.5 text-sm pr-8 focus:outline-none focus:ring-2 focus:ring-blue-300"
                  >
                    <option value="auto">Auto ({FILE_TYPE_LABELS[detection.file_type] || detection.file_type})</option>
                    {Object.entries(FILE_TYPE_LABELS).map(([k, v]) => (
                      <option key={k} value={k}>{v}</option>
                    ))}
                  </select>
                  <ChevronDown size={14} className="absolute right-2 top-1/2 -translate-y-1/2 text-gray-400 pointer-events-none" />
                </div>
              </div>
            </div>
          )}

          {isProcessing && processingStatus && (
            <div className="mb-4">
              <div className="flex justify-between text-xs text-gray-500 mb-1">
                <span>{PHASE_LABELS[processingStatus.phase] || processingStatus.phase}</span>
                <span>{getPhaseProgress()}%</span>
              </div>
              <div className="w-full bg-gray-100 rounded-full h-2 mb-1">
                <div className="bg-indigo-500 h-2 rounded-full transition-all" style={{ width: `${getPhaseProgress()}%` }} />
              </div>
              {processingStatus.total > 0 && (
                <p className="text-xs text-gray-400">
                  {processingStatus.row?.toLocaleString()} / {processingStatus.total?.toLocaleString()} baris
                </p>
              )}
            </div>
          )}

          {uploadStatus === 'uploaded' && !isProcessing && !processingResult && (
            <button
              onClick={startProcessing}
              className="w-full bg-[#1B2A4A] text-white py-2.5 rounded-lg text-sm font-medium hover:bg-[#243560] transition-colors"
            >
              Upload & Process
            </button>
          )}

          {processingResult && (
            <div className="bg-green-50 border border-green-200 rounded-lg p-4 space-y-2">
              <div className="flex items-center gap-2">
                <CheckCircle size={18} className="text-green-500" />
                <span className="font-semibold text-green-800">Import berhasil</span>
              </div>
              <div className="grid grid-cols-2 gap-2 text-sm text-gray-700">
                <div>
                  {processingResult.file_type === 'site_master' ? (
                    <>Inserted: {processingResult.inserted?.toLocaleString()}, Updated: {processingResult.updated?.toLocaleString()}</>
                  ) : (
                    <>{processingResult.imported?.toLocaleString()} tiket di-import</>
                  )}
                </div>
                {processingResult.skipped > 0 && (
                  <div>{processingResult.skipped?.toLocaleString()} duplikat di-skip</div>
                )}
                {processingResult.period && (
                  <div>Periode: {processingResult.period}</div>
                )}
                <div className="flex items-center gap-1">
                  <Clock size={12} className="text-gray-400" />
                  {formatDuration(processingResult.duration_sec)}
                </div>
              </div>
              {processingResult.orphans && Object.values(processingResult.orphans).some(v => v > 0) && (
                <div className="mt-2 bg-yellow-50 border border-yellow-200 rounded p-2">
                  <div className="flex items-center gap-1 text-sm text-yellow-700">
                    <AlertTriangle size={14} />
                    <span>Orphan ditemukan:</span>
                  </div>
                  <ul className="text-xs text-yellow-600 mt-1 ml-5 list-disc">
                    {Object.entries(processingResult.orphans).map(([k, v]) =>
                      v > 0 ? <li key={k}>{v} {k} tidak ter-mapping</li> : null
                    )}
                  </ul>
                </div>
              )}
              <button
                onClick={resetState}
                className="mt-2 text-sm text-blue-600 hover:text-blue-700 font-medium"
              >
                Upload file lain
              </button>
            </div>
          )}
        </div>
      )}

      <div className="mt-8">
        <div className="flex items-center justify-between mb-3">
          <h3 className="text-sm font-semibold text-gray-700">Riwayat Import</h3>
          <button onClick={loadImportHistory} className="text-xs text-gray-400 hover:text-gray-600 flex items-center gap-1">
            <RefreshCw size={12} /> Refresh
          </button>
        </div>
        {importHistory.length === 0 ? (
          <div className="bg-white border border-gray-200 rounded-lg p-6 text-center text-sm text-gray-400">
            Belum ada data import
          </div>
        ) : (
          <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead className="bg-gray-50 text-gray-500 text-xs">
                  <tr>
                    <th className="px-3 py-2 text-left">Tanggal</th>
                    <th className="px-3 py-2 text-left">File</th>
                    <th className="px-3 py-2 text-left">Tipe</th>
                    <th className="px-3 py-2 text-left">Periode</th>
                    <th className="px-3 py-2 text-right">Rows</th>
                    <th className="px-3 py-2 text-right">Orphan</th>
                    <th className="px-3 py-2 text-center">Status</th>
                    <th className="px-3 py-2 text-center"></th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {importHistory.map((item) => (
                    <tr key={item.id} className="hover:bg-gray-50">
                      <td className="px-3 py-2 text-gray-600 whitespace-nowrap">
                        {item.imported_at ? new Date(item.imported_at).toLocaleDateString('id-ID', { day: 'numeric', month: 'short' }) : '-'}
                      </td>
                      <td className="px-3 py-2 text-gray-800 font-medium truncate max-w-[200px]">{item.filename}</td>
                      <td className="px-3 py-2 text-gray-600">{FILE_TYPE_LABELS[item.file_type] || item.file_type}</td>
                      <td className="px-3 py-2 text-gray-600">{item.period || '-'}</td>
                      <td className="px-3 py-2 text-right text-gray-700">{item.rows_imported?.toLocaleString()}</td>
                      <td className="px-3 py-2 text-right">
                        {item.orphan_count > 0 ? (
                          <span className="text-yellow-600 font-medium">{item.orphan_count}</span>
                        ) : (
                          <span className="text-gray-400">0</span>
                        )}
                      </td>
                      <td className="px-3 py-2 text-center">
                        {item.status === 'completed' ? (
                          <span className="inline-flex items-center gap-1 text-xs text-green-600">
                            <CheckCircle size={12} /> OK
                          </span>
                        ) : (
                          <span className="inline-flex items-center gap-1 text-xs text-red-600">
                            <XCircle size={12} /> {item.status}
                          </span>
                        )}
                      </td>
                      <td className="px-3 py-2 text-center">
                        <button onClick={() => deleteImport(item.id)} className="text-gray-400 hover:text-red-500">
                          <Trash2 size={14} />
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default UploadPage;
