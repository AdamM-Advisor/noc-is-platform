import { useEffect, lazy, Suspense } from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import Layout from './components/Layout';
import LoginPage from './pages/LoginPage';
import useAuthStore from './stores/authStore';

const Dashboard = lazy(() => import('./pages/Dashboard'));
const ProfilerPage = lazy(() => import('./pages/ProfilerPage'));
const UploadPage = lazy(() => import('./pages/UploadPage'));
const MasterDataPage = lazy(() => import('./pages/MasterDataPage'));
const ExternalDataPage = lazy(() => import('./pages/ExternalDataPage'));
const SettingsPage = lazy(() => import('./pages/SettingsPage'));
const ReportCardPage = lazy(() => import('./pages/ReportCardPage'));
const SavedViewsPage = lazy(() => import('./pages/SavedViewsPage'));
const ComparisonPage = lazy(() => import('./pages/ComparisonPage'));
const ReportGeneratorPage = lazy(() => import('./pages/ReportGeneratorPage'));
const NdcBrowserPage = lazy(() => import('./pages/NdcBrowserPage'));

function PageLoader() {
  return (
    <div className="flex items-center justify-center py-24">
      <div className="text-center">
        <div className="animate-spin w-6 h-6 border-2 border-[#1E40AF] border-t-transparent rounded-full mx-auto mb-3" />
        <p className="text-sm text-gray-400">Memuat halaman...</p>
      </div>
    </div>
  );
}

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
      <Suspense fallback={<PageLoader />}>
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
      </Suspense>
    </BrowserRouter>
  );
}

export default App;
