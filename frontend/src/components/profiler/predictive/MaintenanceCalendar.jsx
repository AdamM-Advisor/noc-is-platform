import { useMemo } from 'react';
import FormulaBox from './FormulaBox';

const PRIORITY_STYLES = {
  high: { bg: 'bg-red-100', text: 'text-red-700', dot: 'bg-red-500', icon: '🔴' },
  medium: { bg: 'bg-yellow-100', text: 'text-yellow-700', dot: 'bg-yellow-500', icon: '🟡' },
  low: { bg: 'bg-green-50', text: 'text-green-700', dot: 'bg-green-500', icon: '🟢' },
};

const DAY_NAMES = ['Sen', 'Sel', 'Rab', 'Kam', 'Jum', 'Sab', 'Min'];

export default function MaintenanceCalendar({ data }) {
  if (!data?.schedule?.length && !data?.calendar) return null;

  const calendarGrid = useMemo(() => {
    if (data.calendar?.grid) return data.calendar.grid;

    if (!data.schedule?.length) return null;

    const items = data.schedule.filter(s => s.date);
    if (!items.length) return null;

    const firstDate = new Date(items[0].date);
    const year = firstDate.getFullYear();
    const month = firstDate.getMonth();

    const startOfMonth = new Date(year, month, 1);
    let startDow = startOfMonth.getDay();
    startDow = startDow === 0 ? 6 : startDow - 1;

    const daysInMonth = new Date(year, month + 1, 0).getDate();

    const byDay = {};
    items.forEach(item => {
      const d = new Date(item.date);
      const day = d.getDate();
      if (!byDay[day]) byDay[day] = [];
      byDay[day].push(item);
    });

    const weeks = [];
    let week = new Array(7).fill(null);
    let dayNum = 1;

    for (let i = 0; i < startDow; i++) {
      week[i] = { day: null, items: [] };
    }

    while (dayNum <= daysInMonth) {
      const dow = (startDow + dayNum - 1) % 7;
      if (dow === 0 && dayNum > 1) {
        weeks.push(week);
        week = new Array(7).fill(null);
      }
      week[dow] = { day: dayNum, items: byDay[dayNum] || [] };
      dayNum++;
    }

    for (let i = 0; i < 7; i++) {
      if (!week[i]) week[i] = { day: null, items: [] };
    }
    weeks.push(week);

    return { weeks, monthLabel: data.calendar?.month_label || `${year}-${String(month + 1).padStart(2, '0')}` };
  }, [data]);

  const summary = data.summary || {};

  return (
    <div className="space-y-4">
      {calendarGrid && (
        <div>
          {calendarGrid.monthLabel && (
            <p className="text-xs font-semibold text-gray-500 uppercase mb-2">{calendarGrid.monthLabel}</p>
          )}
          <div className="overflow-x-auto">
            <table className="w-full border-collapse text-xs">
              <thead>
                <tr>
                  {DAY_NAMES.map(d => (
                    <th key={d} className="px-1 py-1 text-center text-gray-500 font-medium w-[14%]">{d}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {(calendarGrid.weeks || []).map((week, wi) => (
                  <tr key={wi}>
                    {week.map((cell, di) => (
                      <td key={di} className="border p-1 align-top h-16 min-w-[60px]">
                        {cell?.day != null && (
                          <>
                            <div className="text-[10px] text-gray-400 mb-0.5">{cell.day}</div>
                            {cell.items?.map((item, ii) => {
                              const ps = PRIORITY_STYLES[item.priority] || PRIORITY_STYLES.low;
                              return (
                                <div
                                  key={ii}
                                  className={`${ps.bg} ${ps.text} rounded px-1 py-0.5 text-[9px] font-medium truncate mb-0.5`}
                                  title={`${item.site_name}: ${item.reason}`}
                                >
                                  {ps.icon} {item.site_name || item.site_id}
                                </div>
                              );
                            })}
                          </>
                        )}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      <div className="flex items-center gap-4 text-xs text-gray-500">
        <span className="flex items-center gap-1"><span className="w-2.5 h-2.5 rounded-full bg-red-500 inline-block" /> High priority</span>
        <span className="flex items-center gap-1"><span className="w-2.5 h-2.5 rounded-full bg-yellow-500 inline-block" /> Medium</span>
        <span className="flex items-center gap-1"><span className="w-2.5 h-2.5 rounded-full bg-green-500 inline-block" /> Low</span>
      </div>

      {(summary.n_high != null || summary.total_hours != null) && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-2 text-xs">
          <div className="bg-red-50 rounded p-2 border border-red-100">
            <div className="font-bold text-red-700">{summary.n_high ?? 0}</div>
            <div className="text-gray-500">High Priority</div>
          </div>
          <div className="bg-yellow-50 rounded p-2 border border-yellow-100">
            <div className="font-bold text-yellow-700">{summary.n_medium ?? 0}</div>
            <div className="text-gray-500">Medium</div>
          </div>
          <div className="bg-green-50 rounded p-2 border border-green-100">
            <div className="font-bold text-green-700">{summary.n_low ?? 0}</div>
            <div className="text-gray-500">Low</div>
          </div>
          <div className="bg-gray-50 rounded p-2 border">
            <div className="font-bold text-gray-700">{summary.total_hours ?? 0} jam</div>
            <div className="text-gray-500">
              {summary.capacity_hours ? `/ ${summary.capacity_hours} jam (${summary.utilization_pct?.toFixed(0)}%)` : 'Est. Effort'}
            </div>
          </div>
        </div>
      )}

      {data.capacity_alert?.narrative && (
        <div className={`text-sm px-3 py-2 rounded border ${
          data.capacity_alert.status === 'overload' ? 'bg-red-50 border-red-200 text-red-700' :
          data.capacity_alert.status === 'tight' ? 'bg-amber-50 border-amber-200 text-amber-700' :
          'bg-green-50 border-green-200 text-green-700'
        }`}>
          {data.capacity_alert.narrative}
        </div>
      )}

      <FormulaBox title="Maintenance Scheduling Formula" formulaText={data.formula_text || 'PM window = predicted_next - buffer hari\nBuffer: HIGH=3 hari, MEDIUM=5 hari, LOW=7 hari'} />
    </div>
  );
}
