import { useState } from 'react';
import HierarchyTab from './master/HierarchyTab';
import SiteTab from './master/SiteTab';
import SlaTargetTab from './master/SlaTargetTab';
import ThresholdTab from './master/ThresholdTab';
import DataQualityTab from './master/DataQualityTab';

const TABS = [
  { key: 'hierarchy', label: 'Hierarki' },
  { key: 'site', label: 'Site' },
  { key: 'sla', label: 'SLA Target' },
  { key: 'threshold', label: 'Threshold' },
  { key: 'quality', label: 'Data Quality' },
];

function MasterDataPage() {
  const [activeTab, setActiveTab] = useState('hierarchy');

  return (
    <div>
      <h2 className="text-xl font-bold text-gray-800 mb-4">Master Data</h2>

      <div className="border-b border-gray-200 mb-6">
        <nav className="flex gap-0 -mb-px">
          {TABS.map(({ key, label }) => (
            <button
              key={key}
              onClick={() => setActiveTab(key)}
              className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
                activeTab === key
                  ? 'border-[#1B2A4A] text-[#1B2A4A]'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              {label}
            </button>
          ))}
        </nav>
      </div>

      {activeTab === 'hierarchy' && <HierarchyTab />}
      {activeTab === 'site' && <SiteTab />}
      {activeTab === 'sla' && <SlaTargetTab />}
      {activeTab === 'threshold' && <ThresholdTab />}
      {activeTab === 'quality' && <DataQualityTab />}
    </div>
  );
}

export default MasterDataPage;
