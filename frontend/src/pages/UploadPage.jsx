import { useState, useRef, useCallback, useEffect } from 'react';
import { Upload, FileUp, CheckCircle, XCircle, RefreshCw, AlertTriangle, Clock, ChevronDown, Trash2, Check, X, Eye, Calendar } from 'lucide-react';
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

const COVERAGE_SOURCES = ['swfm_event', 'swfm_incident', 'swfm_realtime', 'fault_center'];

const MONTH_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'Mei', 'Jun', 'Jul', 'Agu', 'Sep', 'Okt', 'Nov', 'Des'];
const MONTH_FULL = ['Januari', 'Februari', 'Maret', 'April', 'Mei', 'Juni', 'Juli', 'Agustus', 'September', 'Oktober', 'November', 'Desember'];

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

function formatCount(n) {
  if (!n && n !== 0) return '0';
  if (n >= 1000000) return `${(n / 1000000).toFixed(1)}M`;
  if (n >= 1000) return `${(n / 1000).toFixed(0)}K`;
  return n.toString();
}

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

  const [coverageData, setCoverageData] = useState(null);
  const [coverageYear, setCoverageYear] = useState(null);
  const [coverageLoading, setCoverageLoading] = useState(false);
  const [cellPopover, setCellPopover] = useState(null);

  const [mgmtImports, setMgmtImports] = useState([]);
  const [periodeMonth, setPeriodeMonth] = useState('');
  const [periodeSource, setPeriodeSource] = useState('');
  const [periodePreview, setPeriodePreview] = useState(null);
  const [rangeFrom, setRangeFrom] = useState('');
  const [rangeTo, setRangeTo] = useState('');
  const [rangeSource, setRangeSource] = useState('');
  const [rangePreview, setRangePreview] = useState(null);
  const [deleteConfirm, setDeleteConfirm] = useState(null);

  const [resyncStatus, setResyncStatus] = useState(null);
  const [resyncResult, setResyncResult] = useState(null);
  const resyncPollRef = useRef(null);

  useEffect(() => {
    loadImportHistory();
    loadCoverage();
    loadMgmtImports();
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
      if (resyncPollRef.current) clearInterval(resyncPollRef.current);
    };
  }, []);

  const loadImportHistory = async () => {
    try {
      const res = await client.get('/imports');
      setImportHistory(res.data);
    } catch (e) {}
  };

  const loadCoverage = async () => {
    setCoverageLoading(true);
    try {
      const res = await client.get('/data/coverage');
      const raw = res.data;
      const flatCoverage = [];
      const yearSet = new Set();
      if (raw?.coverage) {
        for (const [period, sources] of Object.entries(raw.coverage)) {
          const yr = period.split('-')[0];
          yearSet.add(yr);
          for (const [source, info] of Object.entries(sources)) {
            if (info.exists) {
              flatCoverage.push({
                year_month: period,
                source,
                count: info.count || 0,
                has_orphans: !!info.has_orphans,
                items: info.imports || [],
              });
            }
          }
        }
      }
      const years = [...yearSet].sort().reverse();
      const transformed = { coverage: flatCoverage, years, summary: raw?.summary || {} };
      setCoverageData(transformed);
      if (!coverageYear && years.length > 0) {
        setCoverageYear(years[0]);
      }
    } catch (e) {
      setError('Gagal memuat data coverage');
    }
    setCoverageLoading(false);
  };

  const startResync = async () => {
    try {
      setResyncResult(null);
      setResyncStatus({ phase: 'starting', detail: 'Memulai sinkronisasi...' });
      const res = await client.post('/data/resync');
      const jobId = res.data.job_id;

      resyncPollRef.current = setInterval(async () => {
        try {
          const statusRes = await client.get(`/data/resync/status/${jobId}`);
          const job = statusRes.data;
          setResyncStatus(job.progress);

          if (job.status === 'completed') {
            clearInterval(resyncPollRef.current);
            resyncPollRef.current = null;
            setResyncResult(job.result);
            setResyncStatus(null);
            loadCoverage();
            loadImportHistory();
          } else if (job.status === 'failed') {
            clearInterval(resyncPollRef.current);
            resyncPollRef.current = null;
            setResyncStatus(null);
            setError(job.error || 'Sinkronisasi gagal');
          }
        } catch (e) {
          clearInterval(resyncPollRef.current);
          resyncPollRef.current = null;
          setResyncStatus(null);
          setError('Gagal memeriksa status sinkronisasi');
        }
      }, 2000);
    } catch (e) {
      setResyncStatus(null);
      setError(e.response?.data?.detail || 'Gagal memulai sinkronisasi');
    }
  };

  const loadMgmtImports = async () => {
    try {
      const res = await client.get('/imports');
      setMgmtImports(res.data);
    } catch (e) {}
  };

  const refreshAll = () => {
    loadCoverage();
    loadMgmtImports();
    loadImportHistory();
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
            loadCoverage();
            loadMgmtImports();
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

  const getCoverageCell = (source, monthIdx) => {
    if (!coverageData?.coverage || !coverageYear) return null;
    const ym = `${coverageYear}-${String(monthIdx + 1).padStart(2, '0')}`;
    const items = coverageData.coverage.filter(c => c.source === source && c.year_month === ym);
    if (items.length === 0) return null;
    const total = items.reduce((s, i) => s + (i.count || 0), 0);
    const hasOrphans = items.some(i => i.has_orphans);
    return { items, total, hasOrphans, ym };
  };

  const getCoverageSummary = () => {
    if (!coverageData?.coverage || !coverageYear) return { total: 0, months: 0, firstMonth: null, lastMonth: null };
    const yearItems = coverageData.coverage.filter(c => c.year_month?.startsWith(coverageYear));
    const total = yearItems.reduce((s, i) => s + (i.count || 0), 0);
    const uniqueMonths = [...new Set(yearItems.map(i => i.year_month))].sort();
    const months = uniqueMonths.length;
    let firstMonth = null, lastMonth = null;
    if (uniqueMonths.length > 0) {
      const fIdx = parseInt(uniqueMonths[0].split('-')[1]) - 1;
      const lIdx = parseInt(uniqueMonths[uniqueMonths.length - 1].split('-')[1]) - 1;
      firstMonth = MONTH_NAMES[fIdx];
      lastMonth = MONTH_NAMES[lIdx];
    }
    return { total, months, firstMonth, lastMonth };
  };

  const handleCellClick = (source, monthIdx) => {
    const cell = getCoverageCell(source, monthIdx);
    setCellPopover({ source, monthIdx, cell });
  };

  const handleDeleteByPeriod = async (ym, source) => {
    try {
      const countRes = await client.get('/data/tickets/count', { params: { year_month: ym, source } });
      const count = countRes.data?.count || 0;
      setDeleteConfirm({
        message: `Anda akan menghapus ${count.toLocaleString()} tiket. Backup otomatis akan dibuat. Lanjutkan?`,
        onConfirm: async () => {
          try {
            await client.delete('/data/tickets/by-period', { data: { year_month: ym, source } });
            setDeleteConfirm(null);
            setCellPopover(null);
            refreshAll();
          } catch (e) {
            setError('Gagal menghapus data');
            setDeleteConfirm(null);
          }
        },
      });
    } catch (e) {
      setError('Gagal mendapatkan jumlah tiket');
    }
  };

  const handleDeleteByImport = async (id) => {
    const imp = mgmtImports.find(i => i.id === id);
    const count = imp?.rows_imported || 0;
    setDeleteConfirm({
      message: `Anda akan menghapus data import #${id} (${count.toLocaleString()} tiket). Backup otomatis akan dibuat. Lanjutkan?`,
      onConfirm: async () => {
        try {
          await client.delete(`/data/tickets/by-import/${id}`);
          setDeleteConfirm(null);
          refreshAll();
        } catch (e) {
          setError('Gagal menghapus data');
          setDeleteConfirm(null);
        }
      },
    });
  };

  const handlePeriodePreview = async () => {
    if (!periodeMonth || !periodeSource) return;
    try {
      const res = await client.get('/data/tickets/count', { params: { year_month: periodeMonth, source: periodeSource } });
      setPeriodePreview(res.data?.count || 0);
    } catch (e) {
      setError('Gagal mendapatkan jumlah tiket');
    }
  };

  const handlePeriodeDelete = () => {
    if (periodePreview === null || periodePreview === undefined) return;
    setDeleteConfirm({
      message: `Anda akan menghapus ${periodePreview.toLocaleString()} tiket. Backup otomatis akan dibuat. Lanjutkan?`,
      onConfirm: async () => {
        try {
          await client.delete('/data/tickets/by-period', { data: { year_month: periodeMonth, source: periodeSource } });
          setDeleteConfirm(null);
          setPeriodePreview(null);
          refreshAll();
        } catch (e) {
          setError('Gagal menghapus data');
          setDeleteConfirm(null);
        }
      },
    });
  };

  const handleRangePreview = async () => {
    if (!rangeFrom || !rangeTo) return;
    try {
      const params = { from_month: rangeFrom, to_month: rangeTo };
      if (rangeSource) params.source = rangeSource;
      const res = await client.get('/data/tickets/count', { params });
      setRangePreview(res.data?.count || 0);
    } catch (e) {
      setError('Gagal mendapatkan jumlah tiket');
    }
  };

  const handleRangeDelete = () => {
    if (rangePreview === null || rangePreview === undefined) return;
    setDeleteConfirm({
      message: `Anda akan menghapus ${rangePreview.toLocaleString()} tiket. Backup otomatis akan dibuat. Lanjutkan?`,
      onConfirm: async () => {
        try {
          await client.delete('/data/tickets/by-period-range', {
            data: { from: rangeFrom, to: rangeTo, source: rangeSource || undefined },
          });
          setDeleteConfirm(null);
          setRangePreview(null);
          refreshAll();
        } catch (e) {
          setError('Gagal menghapus data');
          setDeleteConfirm(null);
        }
      },
    });
  };

  const getAvailablePeriods = () => {
    if (!coverageData?.coverage) return [];
    const periods = [...new Set(coverageData.coverage.map(c => c.year_month))].sort();
    return periods;
  };

  const summary = getCoverageSummary();

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

      <div className="mt-10">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-bold text-[#1B2A4A]">Data Coverage</h3>
          <div className="flex items-center gap-3">
            {coverageData?.years?.length > 0 && (
              <div className="relative">
                <select
                  value={coverageYear || ''}
                  onChange={(e) => setCoverageYear(e.target.value)}
                  className="appearance-none bg-white border border-gray-300 rounded-lg px-3 py-1.5 text-sm pr-8 focus:outline-none focus:ring-2 focus:ring-blue-300"
                >
                  {coverageData.years.map(y => (
                    <option key={y} value={y}>{y}</option>
                  ))}
                </select>
                <ChevronDown size={14} className="absolute right-2 top-1/2 -translate-y-1/2 text-gray-400 pointer-events-none" />
              </div>
            )}
            <button
              onClick={startResync}
              disabled={!!resyncStatus || coverageLoading}
              className="flex items-center gap-1 text-xs text-white bg-[#1E40AF] hover:bg-[#1B2A4A] border border-[#1E40AF] rounded-lg px-3 py-1.5 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <RefreshCw size={12} className={resyncStatus ? 'animate-spin' : ''} />
              {resyncStatus ? 'Sinkronisasi...' : 'Sinkronisasi Hierarki'}
            </button>
            <button
              onClick={loadCoverage}
              disabled={coverageLoading}
              className="flex items-center gap-1 text-xs text-gray-500 hover:text-[#1B2A4A] border border-gray-300 rounded-lg px-3 py-1.5 hover:border-[#1B2A4A] transition-colors"
            >
              <RefreshCw size={12} className={coverageLoading ? 'animate-spin' : ''} /> Refresh
            </button>
          </div>
        </div>

        {resyncStatus && (
          <div className="mb-4 bg-blue-50 border border-blue-200 rounded-lg p-4">
            <div className="flex items-center gap-2 text-sm text-blue-800">
              <RefreshCw size={14} className="animate-spin" />
              <span className="font-medium">Sinkronisasi sedang berjalan</span>
            </div>
            <p className="text-xs text-blue-600 mt-1">{resyncStatus.detail}</p>
          </div>
        )}

        {resyncResult && (
          <div className="mb-4 bg-green-50 border border-green-200 rounded-lg p-4">
            <div className="flex items-center justify-between">
              <div>
                <div className="flex items-center gap-2 text-sm text-green-800 font-medium">
                  <CheckCircle size={14} />
                  Sinkronisasi selesai ({resyncResult.duration_sec}s)
                </div>
                <div className="mt-2 grid grid-cols-2 sm:grid-cols-4 gap-3 text-xs">
                  <div className="bg-white rounded p-2 border border-green-100">
                    <div className="text-gray-500">Area ter-resolusi</div>
                    <div className="font-bold text-gray-800">{resyncResult.resolved?.area?.toLocaleString()}</div>
                    {resyncResult.remaining_orphans?.area > 0 && (
                      <div className="text-orange-500 text-[10px]">sisa orphan: {resyncResult.remaining_orphans.area.toLocaleString()}</div>
                    )}
                  </div>
                  <div className="bg-white rounded p-2 border border-green-100">
                    <div className="text-gray-500">Regional ter-resolusi</div>
                    <div className="font-bold text-gray-800">{resyncResult.resolved?.regional?.toLocaleString()}</div>
                    {resyncResult.remaining_orphans?.regional > 0 && (
                      <div className="text-orange-500 text-[10px]">sisa orphan: {resyncResult.remaining_orphans.regional.toLocaleString()}</div>
                    )}
                  </div>
                  <div className="bg-white rounded p-2 border border-green-100">
                    <div className="text-gray-500">NOP ter-resolusi</div>
                    <div className="font-bold text-gray-800">{resyncResult.resolved?.nop?.toLocaleString()}</div>
                    {resyncResult.remaining_orphans?.nop > 0 && (
                      <div className="text-orange-500 text-[10px]">sisa orphan: {resyncResult.remaining_orphans.nop.toLocaleString()}</div>
                    )}
                  </div>
                  <div className="bg-white rounded p-2 border border-green-100">
                    <div className="text-gray-500">TO ter-resolusi</div>
                    <div className="font-bold text-gray-800">{resyncResult.resolved?.to?.toLocaleString()}</div>
                    {resyncResult.remaining_orphans?.to > 0 && (
                      <div className="text-orange-500 text-[10px]">sisa orphan: {resyncResult.remaining_orphans.to.toLocaleString()}</div>
                    )}
                  </div>
                </div>
              </div>
              <button onClick={() => setResyncResult(null)} className="text-gray-400 hover:text-gray-600 self-start">
                <X size={14} />
              </button>
            </div>
          </div>
        )}

        <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead className="bg-[#1B2A4A] text-white">
                <tr>
                  <th className="px-3 py-2 text-left font-medium">Sumber Data</th>
                  {MONTH_NAMES.map((m, i) => (
                    <th key={i} className="px-2 py-2 text-center font-medium">{m}</th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {COVERAGE_SOURCES.map(source => (
                  <tr key={source} className="hover:bg-gray-50">
                    <td className="px-3 py-2.5 font-medium text-gray-700 whitespace-nowrap">
                      {FILE_TYPE_LABELS[source]}
                    </td>
                    {MONTH_NAMES.map((_, monthIdx) => {
                      const cell = getCoverageCell(source, monthIdx);
                      return (
                        <td
                          key={monthIdx}
                          className="px-2 py-2.5 text-center cursor-pointer hover:bg-gray-100 transition-colors"
                          onClick={() => handleCellClick(source, monthIdx)}
                        >
                          {cell ? (
                            <span className="inline-flex items-center gap-0.5">
                              {cell.hasOrphans ? (
                                <AlertTriangle size={12} className="text-yellow-500" />
                              ) : (
                                <Check size={12} className="text-green-500" />
                              )}
                              <span className={cell.hasOrphans ? 'text-yellow-700 font-medium' : 'text-green-700 font-medium'}>
                                {formatCount(cell.total)}
                              </span>
                            </span>
                          ) : (
                            <span className="text-gray-300">-</span>
                          )}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="px-4 py-3 bg-gray-50 border-t border-gray-200 flex items-center justify-between text-xs text-gray-600">
            <span>
              Total Tiket: <strong>{summary.total.toLocaleString()}</strong>
              {summary.months > 0 && (
                <> | Periode: {summary.firstMonth}-{summary.lastMonth} {coverageYear} ({summary.months} bulan)</>
              )}
            </span>
            <div className="flex items-center gap-4">
              <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-sm bg-green-100 border border-green-300 inline-block"></span> Data tersedia</span>
              <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-sm bg-yellow-100 border border-yellow-300 inline-block"></span> Ada orphan/warning</span>
              <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-sm bg-gray-100 border border-gray-300 inline-block"></span> Belum ada data</span>
            </div>
          </div>
        </div>
      </div>

      {cellPopover && (
        <div className="fixed inset-0 bg-black/30 z-50 flex items-center justify-center" onClick={() => setCellPopover(null)}>
          <div className="bg-white rounded-xl shadow-xl p-5 max-w-md w-full mx-4" onClick={e => e.stopPropagation()}>
            <div className="flex items-center justify-between mb-4">
              <h4 className="font-semibold text-[#1B2A4A]">
                {FILE_TYPE_LABELS[cellPopover.source]} - {MONTH_FULL[cellPopover.monthIdx]} {coverageYear}
              </h4>
              <button onClick={() => setCellPopover(null)} className="text-gray-400 hover:text-gray-600">
                <X size={18} />
              </button>
            </div>

            {cellPopover.cell ? (
              <div className="space-y-3">
                {cellPopover.cell.items.map((item, idx) => (
                  <div key={idx} className="bg-gray-50 rounded-lg p-3 text-sm">
                    <div className="grid grid-cols-2 gap-1">
                      {item.id && <div className="text-gray-500">Import ID: <span className="text-gray-800">{item.id}</span></div>}
                      {item.imported_at && <div className="text-gray-500">Tanggal: <span className="text-gray-800">{new Date(item.imported_at).toLocaleDateString('id-ID')}</span></div>}
                      <div className="text-gray-500">Jumlah: <span className="text-gray-800 font-medium">{(item.rows_imported || 0).toLocaleString()}</span></div>
                      <div className="text-gray-500">Orphans: <span className={(item.orphan_count || 0) > 0 ? 'text-yellow-600 font-medium' : 'text-gray-800'}>{item.orphan_count || 0}</span></div>
                    </div>
                  </div>
                ))}
                <button
                  onClick={() => handleDeleteByPeriod(cellPopover.cell.ym, cellPopover.source)}
                  className="w-full mt-2 bg-red-600 text-white py-2 rounded-lg text-sm font-medium hover:bg-red-700 transition-colors flex items-center justify-center gap-2"
                >
                  <Trash2 size={14} /> Hapus Data Ini
                </button>
              </div>
            ) : (
              <div className="text-center py-6">
                <div className="text-gray-400 mb-2">
                  <Calendar size={32} className="mx-auto" />
                </div>
                <p className="text-sm text-gray-600">
                  Data belum tersedia. Upload file {FILE_TYPE_LABELS[cellPopover.source]} {MONTH_FULL[cellPopover.monthIdx]} {coverageYear}.
                </p>
              </div>
            )}
          </div>
        </div>
      )}

      <div className="mt-10">
        <h3 className="text-lg font-bold text-[#1B2A4A] mb-4">Manajemen Data Tiket</h3>

        <div className="bg-white border border-gray-200 rounded-lg overflow-hidden mb-6">
          <div className="px-4 py-3 bg-gray-50 border-b border-gray-200 flex items-center justify-between">
            <span className="text-sm font-semibold text-gray-700">Riwayat Import</span>
            <button onClick={loadMgmtImports} className="text-xs text-gray-400 hover:text-gray-600 flex items-center gap-1">
              <RefreshCw size={12} /> Refresh
            </button>
          </div>
          {mgmtImports.length === 0 ? (
            <div className="p-6 text-center text-sm text-gray-400">Belum ada data import</div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead className="bg-gray-50 text-gray-500 text-xs">
                  <tr>
                    <th className="px-3 py-2 text-left">#</th>
                    <th className="px-3 py-2 text-left">Tanggal</th>
                    <th className="px-3 py-2 text-left">File</th>
                    <th className="px-3 py-2 text-left">Tipe</th>
                    <th className="px-3 py-2 text-right">Rows</th>
                    <th className="px-3 py-2 text-center">Status</th>
                    <th className="px-3 py-2 text-center">Action</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {mgmtImports.map((item, idx) => (
                    <tr key={item.id} className="hover:bg-gray-50">
                      <td className="px-3 py-2 text-gray-400">{idx + 1}</td>
                      <td className="px-3 py-2 text-gray-600 whitespace-nowrap">
                        {item.imported_at ? new Date(item.imported_at).toLocaleDateString('id-ID', { day: 'numeric', month: 'short', year: 'numeric' }) : '-'}
                      </td>
                      <td className="px-3 py-2 text-gray-800 font-medium truncate max-w-[200px]">{item.filename}</td>
                      <td className="px-3 py-2 text-gray-600">{FILE_TYPE_LABELS[item.file_type] || item.file_type}</td>
                      <td className="px-3 py-2 text-right text-gray-700">{item.rows_imported?.toLocaleString()}</td>
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
                        <button onClick={() => handleDeleteByImport(item.id)} className="text-red-400 hover:text-red-600">
                          <Trash2 size={14} />
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div className="bg-white border border-gray-200 rounded-lg p-5">
            <h4 className="text-sm font-semibold text-[#1B2A4A] mb-4">Hapus per Periode</h4>
            <div className="space-y-3">
              <div>
                <label className="text-xs text-gray-500 mb-1 block">Periode</label>
                <div className="relative">
                  <select
                    value={periodeMonth}
                    onChange={(e) => { setPeriodeMonth(e.target.value); setPeriodePreview(null); }}
                    className="appearance-none w-full bg-white border border-gray-300 rounded-lg px-3 py-2 text-sm pr-8 focus:outline-none focus:ring-2 focus:ring-blue-300"
                  >
                    <option value="">Pilih periode...</option>
                    {getAvailablePeriods().map(p => (
                      <option key={p} value={p}>{p}</option>
                    ))}
                  </select>
                  <ChevronDown size={14} className="absolute right-2 top-1/2 -translate-y-1/2 text-gray-400 pointer-events-none" />
                </div>
              </div>
              <div>
                <label className="text-xs text-gray-500 mb-1 block">Source</label>
                <div className="relative">
                  <select
                    value={periodeSource}
                    onChange={(e) => { setPeriodeSource(e.target.value); setPeriodePreview(null); }}
                    className="appearance-none w-full bg-white border border-gray-300 rounded-lg px-3 py-2 text-sm pr-8 focus:outline-none focus:ring-2 focus:ring-blue-300"
                  >
                    <option value="">Pilih source...</option>
                    {COVERAGE_SOURCES.map(s => (
                      <option key={s} value={s}>{FILE_TYPE_LABELS[s]}</option>
                    ))}
                  </select>
                  <ChevronDown size={14} className="absolute right-2 top-1/2 -translate-y-1/2 text-gray-400 pointer-events-none" />
                </div>
              </div>
              <div className="flex gap-2">
                <button
                  onClick={handlePeriodePreview}
                  disabled={!periodeMonth || !periodeSource}
                  className="flex-1 flex items-center justify-center gap-1 border border-gray-300 text-gray-700 py-2 rounded-lg text-sm hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                >
                  <Eye size={14} /> Preview
                </button>
                <button
                  onClick={handlePeriodeDelete}
                  disabled={periodePreview === null || periodePreview === undefined}
                  className="flex-1 flex items-center justify-center gap-1 bg-red-600 text-white py-2 rounded-lg text-sm hover:bg-red-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                >
                  <Trash2 size={14} /> Hapus
                </button>
              </div>
              {periodePreview !== null && periodePreview !== undefined && (
                <div className="bg-blue-50 border border-blue-200 rounded-lg p-3 text-sm text-blue-800">
                  Tiket yang akan dihapus: <strong>{periodePreview.toLocaleString()}</strong>
                </div>
              )}
            </div>
          </div>

          <div className="bg-white border border-gray-200 rounded-lg p-5">
            <h4 className="text-sm font-semibold text-[#1B2A4A] mb-4">Hapus per Range</h4>
            <div className="space-y-3">
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="text-xs text-gray-500 mb-1 block">Dari</label>
                  <input
                    type="month"
                    value={rangeFrom}
                    onChange={(e) => { setRangeFrom(e.target.value); setRangePreview(null); }}
                    className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-300"
                  />
                </div>
                <div>
                  <label className="text-xs text-gray-500 mb-1 block">Sampai</label>
                  <input
                    type="month"
                    value={rangeTo}
                    onChange={(e) => { setRangeTo(e.target.value); setRangePreview(null); }}
                    className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-300"
                  />
                </div>
              </div>
              <div>
                <label className="text-xs text-gray-500 mb-1 block">Source (opsional)</label>
                <div className="relative">
                  <select
                    value={rangeSource}
                    onChange={(e) => { setRangeSource(e.target.value); setRangePreview(null); }}
                    className="appearance-none w-full bg-white border border-gray-300 rounded-lg px-3 py-2 text-sm pr-8 focus:outline-none focus:ring-2 focus:ring-blue-300"
                  >
                    <option value="">Semua source</option>
                    {COVERAGE_SOURCES.map(s => (
                      <option key={s} value={s}>{FILE_TYPE_LABELS[s]}</option>
                    ))}
                  </select>
                  <ChevronDown size={14} className="absolute right-2 top-1/2 -translate-y-1/2 text-gray-400 pointer-events-none" />
                </div>
              </div>
              <div className="flex gap-2">
                <button
                  onClick={handleRangePreview}
                  disabled={!rangeFrom || !rangeTo}
                  className="flex-1 flex items-center justify-center gap-1 border border-gray-300 text-gray-700 py-2 rounded-lg text-sm hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                >
                  <Eye size={14} /> Preview
                </button>
                <button
                  onClick={handleRangeDelete}
                  disabled={rangePreview === null || rangePreview === undefined}
                  className="flex-1 flex items-center justify-center gap-1 bg-red-600 text-white py-2 rounded-lg text-sm hover:bg-red-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                >
                  <Trash2 size={14} /> Hapus
                </button>
              </div>
              {rangePreview !== null && rangePreview !== undefined && (
                <div className="bg-blue-50 border border-blue-200 rounded-lg p-3 text-sm text-blue-800">
                  Tiket yang akan dihapus: <strong>{rangePreview.toLocaleString()}</strong>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>

      {deleteConfirm && (
        <div className="fixed inset-0 bg-black/30 z-50 flex items-center justify-center" onClick={() => setDeleteConfirm(null)}>
          <div className="bg-white rounded-xl shadow-xl p-6 max-w-sm w-full mx-4" onClick={e => e.stopPropagation()}>
            <div className="flex items-center gap-3 mb-4">
              <div className="w-10 h-10 rounded-full bg-red-100 flex items-center justify-center">
                <AlertTriangle size={20} className="text-red-600" />
              </div>
              <h4 className="font-semibold text-gray-800">Konfirmasi Hapus</h4>
            </div>
            <p className="text-sm text-gray-600 mb-6">{deleteConfirm.message}</p>
            <div className="flex gap-3">
              <button
                onClick={() => setDeleteConfirm(null)}
                className="flex-1 border border-gray-300 text-gray-700 py-2 rounded-lg text-sm hover:bg-gray-50 transition-colors"
              >
                Batal
              </button>
              <button
                onClick={deleteConfirm.onConfirm}
                className="flex-1 bg-red-600 text-white py-2 rounded-lg text-sm hover:bg-red-700 transition-colors"
              >
                Hapus
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default UploadPage;
