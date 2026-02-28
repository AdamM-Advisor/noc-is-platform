import { useState, useEffect } from 'react';

function getMonthRange(yearMonth) {
  const [y, m] = yearMonth.split('-').map(Number);
  const start = `${y}-${String(m).padStart(2, '0')}-01`;
  const lastDay = new Date(y, m, 0).getDate();
  const end = `${y}-${String(m).padStart(2, '0')}-${String(lastDay).padStart(2, '0')}`;
  return [start, end];
}

function getWeekRange(dateStr) {
  const d = new Date(dateStr);
  const day = d.getDay();
  const monday = new Date(d);
  monday.setDate(d.getDate() - (day === 0 ? 6 : day - 1));
  const sunday = new Date(monday);
  sunday.setDate(monday.getDate() + 6);
  const fmt = dt => dt.toISOString().slice(0, 10);
  return [fmt(monday), fmt(sunday)];
}

function getQuarterRange(year, quarter) {
  const startMonth = (quarter - 1) * 3 + 1;
  const endMonth = startMonth + 2;
  const start = `${year}-${String(startMonth).padStart(2, '0')}-01`;
  const lastDay = new Date(year, endMonth, 0).getDate();
  const end = `${year}-${String(endMonth).padStart(2, '0')}-${String(lastDay).padStart(2, '0')}`;
  return [start, end];
}

function PeriodPicker({ reportType, onPeriodChange }) {
  const [month, setMonth] = useState('2025-07');
  const [date, setDate] = useState('2025-07-15');
  const [year, setYear] = useState('2025');
  const [quarter, setQuarter] = useState('3');

  useEffect(() => {
    let start, end;
    switch (reportType) {
      case 'daily':
        start = date;
        end = date;
        break;
      case 'weekly':
        [start, end] = getWeekRange(date);
        break;
      case 'monthly':
        [start, end] = getMonthRange(month);
        break;
      case 'quarterly':
        [start, end] = getQuarterRange(Number(year), Number(quarter));
        break;
      case 'annual':
        start = `${year}-01-01`;
        end = `${year}-12-31`;
        break;
      default:
        [start, end] = getMonthRange(month);
    }
    onPeriodChange(start, end);
  }, [reportType, month, date, year, quarter]);

  if (reportType === 'daily' || reportType === 'weekly') {
    return (
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">
          {reportType === 'daily' ? 'Tanggal' : 'Pilih Tanggal (akan otomatis ke Senin-Minggu)'}
        </label>
        <input
          type="date"
          value={date}
          onChange={e => setDate(e.target.value)}
          className="border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
        />
      </div>
    );
  }

  if (reportType === 'monthly') {
    return (
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">Bulan</label>
        <input
          type="month"
          value={month}
          onChange={e => setMonth(e.target.value)}
          className="border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
        />
      </div>
    );
  }

  if (reportType === 'quarterly') {
    return (
      <div className="flex gap-4">
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Tahun</label>
          <input
            type="number"
            value={year}
            onChange={e => setYear(e.target.value)}
            min="2020"
            max="2030"
            className="border border-gray-300 rounded-lg px-3 py-2 text-sm w-24 focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
          />
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Triwulan</label>
          <select
            value={quarter}
            onChange={e => setQuarter(e.target.value)}
            className="border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
          >
            <option value="1">Q1 (Jan-Mar)</option>
            <option value="2">Q2 (Apr-Jun)</option>
            <option value="3">Q3 (Jul-Sep)</option>
            <option value="4">Q4 (Okt-Des)</option>
          </select>
        </div>
      </div>
    );
  }

  return (
    <div>
      <label className="block text-sm font-medium text-gray-700 mb-1">Tahun</label>
      <input
        type="number"
        value={year}
        onChange={e => setYear(e.target.value)}
        min="2020"
        max="2030"
        className="border border-gray-300 rounded-lg px-3 py-2 text-sm w-24 focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
      />
    </div>
  );
}

export default PeriodPicker;
