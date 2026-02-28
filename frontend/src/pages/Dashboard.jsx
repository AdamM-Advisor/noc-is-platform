import { useState, useEffect } from 'react';
import { BarChart3, Upload } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import client from '../api/client';
import LoadingWrapper from '../components/LoadingWrapper';

function Dashboard() {
  const [health, setHealth] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const navigate = useNavigate();

  const fetchHealth = async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await client.get('/health');
      setHealth(res.data);
    } catch (err) {
      setError('Gagal memuat status sistem.');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchHealth();
  }, []);

  return (
    <div>
      <h2 className="text-xl font-bold text-gray-800 mb-6">Dashboard</h2>

      <LoadingWrapper loading={loading} error={error} onRetry={fetchHealth}>
        <div className="bg-white rounded-xl border border-gray-200 p-8 text-center">
          <BarChart3 size={48} className="mx-auto text-blue-500 mb-4" />
          <h3 className="text-lg font-semibold text-gray-800 mb-2">Platform siap</h3>
          <p className="text-gray-500 text-sm mb-6">
            Upload data untuk memulai analisis.
          </p>
          <button
            onClick={() => navigate('/upload')}
            className="inline-flex items-center gap-2 px-5 py-2.5 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 transition-colors"
          >
            <Upload size={16} />
            Upload Data
          </button>
        </div>

        {health && (
          <div className="mt-6 grid grid-cols-1 sm:grid-cols-3 gap-4">
            <div className="bg-white rounded-xl border border-gray-200 p-4">
              <p className="text-xs text-gray-500 mb-1">Status</p>
              <p className="text-sm font-semibold flex items-center gap-2">
                <span className={`w-2 h-2 rounded-full ${health.status === 'healthy' ? 'bg-green-500' : 'bg-red-500'}`} />
                {health.status === 'healthy' ? 'Healthy' : health.status}
              </p>
            </div>
            <div className="bg-white rounded-xl border border-gray-200 p-4">
              <p className="text-xs text-gray-500 mb-1">Database</p>
              <p className="text-sm font-semibold">{health.database?.size_mb} MB</p>
            </div>
            <div className="bg-white rounded-xl border border-gray-200 p-4">
              <p className="text-xs text-gray-500 mb-1">Backups</p>
              <p className="text-sm font-semibold">{health.disk?.backup_count} file</p>
            </div>
          </div>
        )}
      </LoadingWrapper>
    </div>
  );
}

export default Dashboard;
