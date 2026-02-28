import { useState } from 'react';
import { NavLink, Outlet } from 'react-router-dom';
import { BarChart3, Upload, Settings, Menu, X, Info, Database, Globe, Search, FileText } from 'lucide-react';

const navItems = [
  { to: '/dashboard', icon: BarChart3, label: 'Dashboard' },
  { to: '/profiler', icon: Search, label: 'Profiler' },
  { to: '/report-card', icon: FileText, label: 'Report Card' },
  { to: '/upload', icon: Upload, label: 'Upload' },
  { to: '/master-data', icon: Database, label: 'Master Data' },
  { to: '/external-data', icon: Globe, label: 'Data Eksternal' },
  { to: '/settings', icon: Settings, label: 'Settings' },
];

function Layout() {
  const [sidebarOpen, setSidebarOpen] = useState(false);

  return (
    <div className="min-h-screen bg-gray-50 flex flex-col">
      <header className="bg-[#1B2A4A] text-white h-14 flex items-center justify-between px-4 shrink-0 z-30">
        <div className="flex items-center gap-3">
          <button
            className="lg:hidden p-1 hover:bg-white/10 rounded"
            onClick={() => setSidebarOpen(!sidebarOpen)}
          >
            {sidebarOpen ? <X size={20} /> : <Menu size={20} />}
          </button>
          <h1 className="text-lg font-semibold tracking-wide">NOC-IS Analytics Platform</h1>
        </div>
        <div className="flex items-center gap-3 text-sm">
          <span className="hidden sm:inline text-gray-300">Dr. Adam M.</span>
          <NavLink to="/settings" className="p-1 hover:bg-white/10 rounded">
            <Settings size={18} />
          </NavLink>
        </div>
      </header>

      <div className="flex flex-1 overflow-hidden">
        {sidebarOpen && (
          <div
            className="fixed inset-0 bg-black/40 z-20 lg:hidden"
            onClick={() => setSidebarOpen(false)}
          />
        )}

        <aside
          className={`
            fixed lg:static inset-y-0 left-0 z-20 w-56
            bg-[#1B2A4A] text-white flex flex-col
            transition-transform duration-200
            lg:translate-x-0 pt-14 lg:pt-0
            ${sidebarOpen ? 'translate-x-0' : '-translate-x-full'}
          `}
        >
          <nav className="flex-1 py-4 px-3 space-y-1">
            {navItems.map(({ to, icon: Icon, label }) => (
              <NavLink
                key={to}
                to={to}
                onClick={() => setSidebarOpen(false)}
                className={({ isActive }) =>
                  `flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-colors ${
                    isActive
                      ? 'bg-blue-600 text-white'
                      : 'text-gray-300 hover:bg-white/10 hover:text-white'
                  }`
                }
              >
                <Icon size={18} />
                {label}
              </NavLink>
            ))}
          </nav>
          <div className="px-4 py-3 border-t border-white/10">
            <div className="flex items-center gap-2 text-xs text-gray-400">
              <Info size={14} />
              <span>v1.0</span>
            </div>
          </div>
        </aside>

        <main className="flex-1 overflow-auto">
          <div className="p-6 max-w-6xl mx-auto">
            <Outlet />
            <footer className="mt-12 pt-4 border-t border-gray-200 text-center text-xs text-gray-400">
              NOC-IS Analytics Platform v1.0 — Dr. Adam M.
            </footer>
          </div>
        </main>
      </div>
    </div>
  );
}

export default Layout;
