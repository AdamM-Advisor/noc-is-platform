import { RefreshCw, Target } from 'lucide-react';
import RiskRadarChart from './RiskRadarChart';
import RiskAggregation from './RiskAggregation';
import RiskSiteTable from './RiskSiteTable';
import VolumeForecast from './VolumeForecast';
import SlaProjection from './SlaProjection';
import BreachCountdown from './BreachCountdown';
import PatternScatter from './PatternScatter';
import MaintenanceCalendar from './MaintenanceCalendar';
import MaintenanceTable from './MaintenanceTable';

export default function PredictivePanel({
  entityLevel,
  riskScore,
  riskAggregation,
  forecast,
  slaBreach,
  pattern,
  patternBatch,
  maintenanceCalendar,
  loading,
}) {
  const isSite = entityLevel === 'site';
  const isTO = entityLevel === 'to';
  const isHighLevel = ['nop', 'regional', 'area'].includes(entityLevel);

  const show4A = isSite;
  const show4B = !isSite;
  const show4C = true;
  const show4D = true;
  const show4E = isSite || isTO;
  const show4F = isTO;

  if (loading) {
    return (
      <div className="bg-white rounded-lg border p-5">
        <div className="flex items-center justify-center py-12 text-gray-400">
          <RefreshCw size={24} className="animate-spin mr-2" /> Menghitung prediksi & risk score...
        </div>
      </div>
    );
  }

  const hasAnyData = riskScore || riskAggregation || forecast || slaBreach || pattern || patternBatch || maintenanceCalendar;
  if (!hasAnyData) return null;

  return (
    <div className="bg-white rounded-lg border p-5 space-y-6">
      <div className="flex items-center gap-2">
        <Target size={18} className="text-purple-500" />
        <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide">
          Panel 4: Prediktif — Risk Score, Forecast & Maintenance
        </h3>
      </div>

      {show4A && riskScore && (
        <div className="space-y-2">
          <p className="text-xs font-semibold text-gray-500 uppercase flex items-center gap-1">
            4A: Risk Assessment
          </p>
          <RiskRadarChart data={riskScore} />
        </div>
      )}

      {show4B && riskAggregation && (
        <div className="space-y-2">
          <p className="text-xs font-semibold text-gray-500 uppercase flex items-center gap-1">
            4B: Risk Overview
          </p>
          <RiskAggregation data={riskAggregation} />
          <RiskSiteTable sites={riskAggregation.top_sites} />
        </div>
      )}

      {show4C && forecast && (
        <div className="space-y-2">
          <p className="text-xs font-semibold text-gray-500 uppercase flex items-center gap-1">
            4C: Volume Forecast
          </p>
          <VolumeForecast data={forecast} />
        </div>
      )}

      {show4D && slaBreach && (
        <div className="space-y-2">
          <p className="text-xs font-semibold text-gray-500 uppercase flex items-center gap-1">
            4D: SLA Breach Prediction
          </p>
          <SlaProjection data={slaBreach} />
          {!isSite && slaBreach.children_breach && (
            <BreachCountdown narrative={slaBreach.children_breach_narrative}>
              {slaBreach.children_breach}
            </BreachCountdown>
          )}
        </div>
      )}

      {show4E && (
        <div className="space-y-2">
          <p className="text-xs font-semibold text-gray-500 uppercase flex items-center gap-1">
            4E: Pattern Detection
          </p>
          {isSite && pattern && (
            <div className="space-y-2">
              {pattern.pattern_detected ? (
                <div className="text-sm px-3 py-2 rounded border-l-4 border-amber-400 bg-gray-50 text-gray-700">
                  {pattern.narrative}
                </div>
              ) : (
                <div className="text-sm px-3 py-2 rounded border bg-gray-50 border-gray-200 text-gray-600">
                  {pattern.narrative || 'Pola tidak terdeteksi untuk site ini.'}
                </div>
              )}
              {pattern.maintenance_window && (
                <div className="text-sm px-3 py-2 rounded border-l-4 border-gray-300 bg-gray-50 text-gray-700">
                  {pattern.maintenance_window.narrative}
                </div>
              )}
            </div>
          )}
          {isTO && patternBatch && (
            <PatternScatter
              data={patternBatch.scatter_data}
              consistentSites={patternBatch.consistent_sites}
            />
          )}
        </div>
      )}

      {show4F && maintenanceCalendar && (
        <div className="space-y-2">
          <p className="text-xs font-semibold text-gray-500 uppercase flex items-center gap-1">
            4F: Maintenance Calendar
          </p>
          <MaintenanceCalendar data={maintenanceCalendar} />
          <MaintenanceTable schedule={maintenanceCalendar.schedule} />
        </div>
      )}
    </div>
  );
}
