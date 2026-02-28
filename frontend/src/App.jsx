import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import Layout from './components/Layout';
import Dashboard from './pages/Dashboard';
import ProfilerPage from './pages/ProfilerPage';
import UploadPage from './pages/UploadPage';
import MasterDataPage from './pages/MasterDataPage';
import ExternalDataPage from './pages/ExternalDataPage';
import SettingsPage from './pages/SettingsPage';
import ReportCardPage from './pages/ReportCardPage';

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={<Navigate to="/dashboard" replace />} />
          <Route path="dashboard" element={<Dashboard />} />
          <Route path="profiler" element={<ProfilerPage />} />
          <Route path="report-card" element={<ReportCardPage />} />
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
