import { useState, useEffect } from 'react';
import { Database, HardDrive, RotateCcw, Download, CheckCircle, XCircle, TableProperties, SlidersHorizontal } from 'lucide-react';
import client from '../api/client';
import LoadingWrapper from '../components/LoadingWrapper';
import DangerZone from '../components/DangerZone';
import ThresholdForm from '../components/settings/ThresholdForm';

const TABLE_GROUPS = {
  "Master Tables": [
    "master_area", "master_regional", "master_nop", "master_to",
    "master_site", "master_sla_target", "master_threshold"
  ],
  "Data Tables": [
    "noc_tickets", "summary_monthly", "summary_weekly", "risk_score_history"
  ],
  "System Tables": [
    "saved_views", "report_history", "import_logs", "orphan_log"
  ],
};

const TABS = [
  { key: 'system', label: 'System', icon: Database },
  { key: 'threshold', label: 'Threshold', icon: SlidersHorizontal },
];

function SettingsPage() {
  const [activeTab, setActiveTab] = useState('system');
  const [dbInfo, setDbInfo] = useState(null);
  const [health, setHealth] = useState(null);
  const [backups, setBackups] = useState([]);
  const [schemaStatus, setSchemaStatus] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [actionLoading, setActionLoading] = useState(false);

  const fetchData = async () => {
    setLoading(true);
    setError(null);
    try {
      const [dbRes, healthRes, backupRes, schemaRes] = await Promise.all([
        client.get('/admin/db-info'),
        client.get('/health'),
        client.get('/admin/backups'),
        client.get('/schema/status'),
      ]);
      setDbInfo(dbRes.data);
      setHealth(healthRes.data);
      setBackups(backupRes.data.backups);
      setSchemaStatus(schemaRes.data);
    } catch (err) {
      setError('Gagal memuat data settings.');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  const handleInitSchema = async () => {
    setActionLoading(true);
    try {
      await client.post('/schema/init');
      await fetchData();
    } catch (err) {
    } finally {
      setActionLoading(false);
    }
  };

  const handleResetSeed = async () => {
    if (!confirm('Reset seed data (threshold & SLA target) ke default?')) return;
    setActionLoading(true);
    try {
      await client.post('/schema/seed-reset');
      await fetchData();
    } catch (err) {
    } finally {
      setActionLoading(false);
    }
  };

  const handleBackup = async () => {
    setActionLoading(true);
    try {
      await client.post('/admin/backup');
      await fetchData();
    } catch (err) {
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
    } finally {
      setActionLoading(false);
    }
  };

  return (
    <div>
      <h2 className="text-xl font-bold text-gray-800 mb-4">Settings</h2>

      <div className="flex gap-1 mb-6 border-b border-gray-200">
        {TABS.map(({ key, label, icon: Icon }) => (
          <button
            key={key}
            onClick={() => setActiveTab(key)}
            className={`flex items-center gap-2 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
              activeTab === key
                ? 'border-blue-600 text-blue-600'
                : 'border-transparent text-gray-500 hover:text-gray-700'
            }`}
          >
            <Icon size={16} />
            {label}
          </button>
        ))}
      </div>

      {activeTab === 'threshold' && (
        <div className="bg-white rounded-xl border border-gray-200 p-5">
          <h3 className="font-semibold text-gray-800 text-sm mb-4 flex items-center gap-2">
            <SlidersHorizontal size={18} className="text-blue-600" />
            Threshold Configuration
          </h3>
          <ThresholdForm />
        </div>
      )}

      {activeTab === 'system' && (
        <LoadingWrapper loading={loading} error={error} onRetry={fetchData}>
          <div className="space-y-6">
          <div className="bg-white rounded-xl border border-gray-200 p-5">
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-2">
                <TableProperties size={18} className="text-blue-600" />
                <h3 className="font-semibold text-gray-800 text-sm">Database Schema</h3>
              </div>
              <div className="flex items-center gap-2">
                {schemaStatus?.initialized ? (
                  <span className="flex items-center gap-1.5 text-xs text-green-600 font-medium">
                    <span className="w-2 h-2 rounded-full bg-green-500" />
                    Initialized ({schemaStatus.total_tables} tables)
                  </span>
                ) : (
                  <span className="flex items-center gap-1.5 text-xs text-amber-600 font-medium">
                    <span className="w-2 h-2 rounded-full bg-amber-500" />
                    Not Initialized
                  </span>
                )}
              </div>
            </div>

            {schemaStatus && (
              <div className="space-y-4">
                {Object.entries(TABLE_GROUPS).map(([groupName, tableNames]) => (
                  <div key={groupName}>
                    <p className="text-xs font-semibold text-gray-500 mb-1.5">{groupName}</p>
                    <div className="space-y-1">
                      {tableNames.map((tbl) => {
                        const info = schemaStatus.tables?.[tbl];
                        return (
                          <div key={tbl} className="flex items-center gap-2 text-xs">
                            {info?.exists ? (
                              <CheckCircle size={14} className="text-green-500" />
                            ) : (
                              <XCircle size={14} className="text-gray-300" />
                            )}
                            <span className="text-gray-700 font-mono">{tbl}</span>
                            <span className="text-gray-400">
                              ({info?.exists ? `${info.rows} rows` : 'not created'})
                            </span>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                ))}
              </div>
            )}

            <div className="flex gap-2 mt-4 pt-3 border-t border-gray-100">
              <button
                onClick={handleInitSchema}
                disabled={actionLoading}
                className={`px-4 py-2 rounded-lg text-xs font-medium transition-colors disabled:opacity-50 ${
                  schemaStatus?.initialized
                    ? 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                    : 'bg-blue-600 text-white hover:bg-blue-700'
                }`}
              >
                {actionLoading ? 'Processing...' : 'Initialize Schema'}
              </button>
              <button
                onClick={handleResetSeed}
                disabled={actionLoading}
                className="px-4 py-2 bg-gray-100 text-gray-600 rounded-lg text-xs font-medium hover:bg-gray-200 transition-colors disabled:opacity-50"
              >
                Reset Seed Data
              </button>
            </div>
          </div>

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
      )}
    </div>
  );
}

export default SettingsPage;
