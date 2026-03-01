import { useEffect } from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import Layout from './components/Layout';
import LoginPage from './pages/LoginPage';
import Dashboard from './pages/Dashboard';
import ProfilerPage from './pages/ProfilerPage';
import UploadPage from './pages/UploadPage';
import MasterDataPage from './pages/MasterDataPage';
import ExternalDataPage from './pages/ExternalDataPage';
import SettingsPage from './pages/SettingsPage';
import ReportCardPage from './pages/ReportCardPage';
import SavedViewsPage from './pages/SavedViewsPage';
import ComparisonPage from './pages/ComparisonPage';
import ReportGeneratorPage from './pages/ReportGeneratorPage';
import NdcBrowserPage from './pages/NdcBrowserPage';
import useAuthStore from './stores/authStore';

function App() {
  const { authenticated, checking, checkAuth } = useAuthStore();

  useEffect(() => {
    checkAuth();
  }, []);

  if (checking) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-[#0F172A] via-[#1B2A4A] to-[#1E3A5F] flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin w-8 h-8 border-3 border-white border-t-transparent rounded-full mx-auto mb-4" />
          <p className="text-white/60 text-sm">Memuat...</p>
        </div>
      </div>
    );
  }

  if (!authenticated) {
    return <LoginPage />;
  }

  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={<Navigate to="/dashboard" replace />} />
          <Route path="dashboard" element={<Dashboard />} />
          <Route path="profiler" element={<ProfilerPage />} />
          <Route path="report-card" element={<ReportCardPage />} />
          <Route path="saved-views" element={<SavedViewsPage />} />
          <Route path="comparison" element={<ComparisonPage />} />
          <Route path="reports" element={<ReportGeneratorPage />} />
          <Route path="ndc" element={<NdcBrowserPage />} />
          <Route path="upload" element={<UploadPage />} />
          <Route path="master-data" element={<MasterDataPage />} />
          <Route path="external-data" element={<ExternalDataPage />} />
          <Route path="settings" element={<SettingsPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}

export default App;
