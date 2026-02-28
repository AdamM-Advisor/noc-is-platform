import { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import { ArrowRightLeft } from 'lucide-react';
import axios from 'axios';
import ComparisonSelector from '../components/comparison/ComparisonSelector';
import DeltaSummary from '../components/comparison/DeltaSummary';
import DeltaNarrative from '../components/comparison/DeltaNarrative';
import SideBySideKpi from '../components/comparison/SideBySideKpi';
import RadarOverlay from '../components/comparison/RadarOverlay';
import ChildDeltaTable from '../components/comparison/ChildDeltaTable';
import CompositionWarning from '../components/comparison/CompositionWarning';

const emptyProfile = { entity_level: 'area', entity_id: '', entity_name: '', date_from: '', date_to: '' };

export default function ComparisonPage() {
  const [searchParams] = useSearchParams();
  const [profileA, setProfileA] = useState({ ...emptyProfile });
  const [profileB, setProfileB] = useState({ ...emptyProfile });
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    const level = searchParams.get('level');
    const id = searchParams.get('id');
    if (level && id) {
      setProfileA({
        entity_level: level,
        entity_id: id,
        entity_name: id,
        date_from: searchParams.get('from') || '',
        date_to: searchParams.get('to') || '',
      });
    }
    const viewId = searchParams.get('view_id');
    if (viewId) {
      axios.get(`/api/saved-views/${viewId}`).then(res => {
        const v = res.data;
        setProfileA({
          entity_level: v.entity_level,
          entity_id: v.entity_id,
          entity_name: v.entity_name || v.entity_id,
          date_from: v.date_from || '',
          date_to: v.date_to || '',
        });
      }).catch(() => {});
    }
  }, []);

  const handleCompare = async () => {
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const res = await axios.post('/api/comparison/generate', {
        profile_a: profileA,
        profile_b: profileB,
      });
      if (res.data.error) {
        setError(res.data.errors?.join(', ') || 'Gagal membandingkan.');
      } else {
        setResult(res.data);
      }
    } catch (err) {
      setError(err.response?.data?.detail || 'Gagal membandingkan profil.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-2">
        <ArrowRightLeft size={22} className="text-blue-600" />
        <h2 className="text-xl font-bold text-gray-800">Comparison Mode</h2>
      </div>

      <ComparisonSelector
        profileA={profileA}
        profileB={profileB}
        onChangeA={setProfileA}
        onChangeB={setProfileB}
        onCompare={handleCompare}
        loading={loading}
        comparisonType={result?.comparison_type}
      />

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-red-700 text-sm">
          {error}
        </div>
      )}

      {result && (
        <>
          <DeltaNarrative
            narrative={result.narrative}
            comparisonType={result.comparison_type}
            profileA={result.profile_a}
            profileB={result.profile_b}
          />

          <DeltaSummary deltas={result.deltas} />

          <div className="grid lg:grid-cols-2 gap-6">
            <SideBySideKpi
              kpisA={result.kpis_a}
              kpisB={result.kpis_b}
              statusA={result.status_a}
              statusB={result.status_b}
              profileA={result.profile_a}
              profileB={result.profile_b}
            />
            <RadarOverlay
              radar={result.radar}
              profileA={result.profile_a}
              profileB={result.profile_b}
            />
          </div>

          <CompositionWarning compositionCheck={result.composition_check} />

          <ChildDeltaTable
            childrenDelta={result.children_delta}
            profileA={result.profile_a}
            profileB={result.profile_b}
          />
        </>
      )}

      {!result && !loading && !error && (
        <div className="bg-white rounded-lg border p-12 text-center text-gray-500">
          <ArrowRightLeft size={48} className="mx-auto mb-4 text-gray-300" />
          <h3 className="text-lg font-medium text-gray-700 mb-2">Comparison Mode</h3>
          <p>Pilih dua profil untuk dibandingkan, lalu klik "Bandingkan".</p>
        </div>
      )}
    </div>
  );
}
