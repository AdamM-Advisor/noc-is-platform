import { useState, useEffect } from 'react';
import { Database, HardDrive, RotateCcw, Download } from 'lucide-react';
import client from '../api/client';
import LoadingWrapper from '../components/LoadingWrapper';
import DangerZone from '../components/DangerZone';

function SettingsPage() {
  const [dbInfo, setDbInfo] = useState(null);
  const [health, setHealth] = useState(null);
  const [backups, setBackups] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [actionLoading, setActionLoading] = useState(false);

  const fetchData = async () => {
    setLoading(true);
    setError(null);
    try {
      const [dbRes, healthRes, backupRes] = await Promise.all([
        client.get('/admin/db-info'),
        client.get('/health'),
        client.get('/admin/backups'),
      ]);
      setDbInfo(dbRes.data);
      setHealth(healthRes.data);
      setBackups(backupRes.data.backups);
    } catch (err) {
      setError('Gagal memuat data settings.');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  const handleBackup = async () => {
    setActionLoading(true);
    try {
      await client.post('/admin/backup');
      await fetchData();
    } catch (err) {
      // handled by interceptor
    } finally {
      setActionLoading(false);
    }
  };

  const handleRestore = async (filename) => {
    if (!confirm(`Restore database dari ${filename}?`)) return;
    setActionLoading(true);
    try {
      await client.post('/admin/restore', { backup_filename: filename });
      await fetchData();
    } catch (err) {
      // handled by interceptor
    } finally {
      setActionLoading(false);
    }
  };

  const handleDeleteData = async () => {
    setActionLoading(true);
    try {
      await client.post('/admin/delete-data');
      await fetchData();
    } catch (err) {
      // handled by interceptor
    } finally {
      setActionLoading(false);
    }
  };

  const handleResetDatabase = async () => {
    setActionLoading(true);
    try {
      await client.post('/admin/reset-database');
      await fetchData();
    } catch (err) {
      // handled by interceptor
    } finally {
      setActionLoading(false);
    }
  };

  return (
    <div>
      <h2 className="text-xl font-bold text-gray-800 mb-6">Settings</h2>

      <LoadingWrapper loading={loading} error={error} onRetry={fetchData}>
        <div className="space-y-6">
          <div className="bg-white rounded-xl border border-gray-200 p-5">
            <div className="flex items-center gap-2 mb-4">
              <Database size={18} className="text-blue-600" />
              <h3 className="font-semibold text-gray-800 text-sm">Database Info</h3>
            </div>
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
              <div>
                <p className="text-xs text-gray-500">Database size</p>
                <p className="text-sm font-semibold text-gray-800">{dbInfo?.db_size_mb || 0} MB</p>
              </div>
              <div>
                <p className="text-xs text-gray-500">Tables</p>
                <p className="text-sm font-semibold text-gray-800">{dbInfo?.table_count || 0}</p>
              </div>
              <div>
                <p className="text-xs text-gray-500">Status</p>
                <p className="text-sm font-semibold flex items-center gap-1.5">
                  <span className={`w-2 h-2 rounded-full ${health?.status === 'healthy' ? 'bg-green-500' : 'bg-red-500'}`} />
                  {health?.status === 'healthy' ? 'Healthy' : health?.status || 'Unknown'}
                </p>
              </div>
            </div>
          </div>

          <div className="bg-white rounded-xl border border-gray-200 p-5">
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-2">
                <HardDrive size={18} className="text-blue-600" />
                <h3 className="font-semibold text-gray-800 text-sm">Backup & Restore</h3>
              </div>
              <button
                onClick={handleBackup}
                disabled={actionLoading}
                className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg text-xs font-medium hover:bg-blue-700 transition-colors disabled:opacity-50"
              >
                <Download size={14} />
                Backup Sekarang
              </button>
            </div>

            {backups.length === 0 ? (
              <p className="text-sm text-gray-400">Belum ada backup.</p>
            ) : (
              <div className="space-y-2">
                <p className="text-xs text-gray-500 mb-2">Backup tersedia:</p>
                {backups.map((b) => (
                  <div
                    key={b.name}
                    className="flex items-center justify-between bg-gray-50 rounded-lg px-4 py-2.5 border border-gray-100"
                  >
                    <div className="min-w-0">
                      <p className="text-sm font-medium text-gray-800 truncate">{b.name}</p>
                      <p className="text-xs text-gray-400">{b.size_mb} MB — {new Date(b.date).toLocaleString()}</p>
                    </div>
                    <button
                      onClick={() => handleRestore(b.name)}
                      disabled={actionLoading}
                      className="flex items-center gap-1 px-3 py-1.5 text-xs text-blue-600 hover:bg-blue-50 rounded-lg transition-colors disabled:opacity-50"
                      title="Restore"
                    >
                      <RotateCcw size={14} />
                      Restore
                    </button>
                  </div>
                ))}
              </div>
            )}
          </div>

          <DangerZone
            onDeleteData={handleDeleteData}
            onResetDatabase={handleResetDatabase}
            loading={actionLoading}
          />
        </div>
      </LoadingWrapper>
    </div>
  );
}

export default SettingsPage;
