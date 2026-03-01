import logging
import math
from datetime import datetime, timedelta
from statistics import mean, stdev

from backend.database import get_connection

logger = logging.getLogger(__name__)

WEIGHTS = {
    "frequency": 0.20,
    "recency": 0.15,
    "severity": 0.15,
    "mttr_trend": 0.15,
    "repeat": 0.15,
    "device": 0.10,
    "escalation": 0.10,
}

COMP_LABELS = {
    "frequency": "Frekuensi",
    "recency": "Recency",
    "severity": "Severity Mix",
    "mttr_trend": "MTTR Trend",
    "repeat": "Repeat Rate",
    "device": "Device Aging",
    "escalation": "Eskalasi",
}

COMP_NARRATIVES = {
    "frequency": lambda v: f"Frekuensi gangguan sangat tinggi ({v:.0f}/100).",
    "recency": lambda v: "Gangguan terakhir sangat baru.",
    "severity": lambda v: "Proporsi severity tinggi di atas normal.",
    "mttr_trend": lambda v: "Waktu perbaikan cenderung semakin lama.",
    "repeat": lambda v: "Insiden berulang sangat tinggi — RC belum solved.",
    "device": lambda v: "Perangkat sudah melewati usia ekonomis.",
    "escalation": lambda v: "Tingkat eskalasi tinggi — kompleksitas meningkat.",
}


def linear_regression_slope(x, y):
    n = len(x)
    if n < 2:
        return 0.0
    x_list = list(x)
    y_list = list(y)
    x_mean = sum(x_list) / n
    y_mean = sum(y_list) / n
    num = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x_list, y_list))
    den = sum((xi - x_mean) ** 2 for xi in x_list)
    if den == 0:
        return 0.0
    return num / den


def classify_risk(score):
    if score >= 70:
        return {"level": "HIGH", "color": "#DC2626", "icon": ""}
    elif score >= 40:
        return {"level": "MEDIUM", "color": "#D97706", "icon": ""}
    else:
        return {"level": "LOW", "color": "#16A34A", "icon": ""}


def interpret_risk(score, components):
    parts = []
    top_comp = max(components, key=components.get)

    if score >= 70:
        parts.append(
            f"Site berisiko TINGGI (skor {score:.0f}/100). "
            f"Komponen risiko tertinggi: {COMP_LABELS[top_comp]} "
            f"({components[top_comp]:.0f})."
        )
    elif score >= 40:
        parts.append(f"Site berisiko SEDANG (skor {score:.0f}/100).")
    else:
        parts.append(f"Site berisiko rendah (skor {score:.0f}/100).")

    for comp, val in sorted(components.items(), key=lambda x: -x[1]):
        if val > 80:
            parts.append(COMP_NARRATIVES[comp](val))

    return {"narrative": " ".join(parts)}


def calculate_risk_score(conn, site_id, date_to):
    try:
        date_to_dt = datetime.strptime(date_to, "%Y-%m-%d") if isinstance(date_to, str) else date_to
    except Exception:
        date_to_dt = datetime.now()

    date_from_90d = (date_to_dt - timedelta(days=90)).strftime("%Y-%m-%d")
    date_to_str = date_to_dt.strftime("%Y-%m-%d")

    ticket_count_90d = 0
    days_since_last = 999
    crit_major_pct = 0.0
    mttr_slope = 0.0
    repeat_pct = 0.0
    device_age = 0.0
    esc_pct = 0.0

    try:
        row = conn.execute("""
            SELECT COUNT(*) FROM noc_tickets
            WHERE site_id = ? AND occured_time >= ? AND occured_time <= ?
        """, [site_id, date_from_90d, date_to_str]).fetchone()
        ticket_count_90d = row[0] if row else 0
    except Exception:
        pass

    try:
        row = conn.execute("""
            SELECT MAX(occured_time) FROM noc_tickets
            WHERE site_id = ? AND occured_time <= ?
        """, [site_id, date_to_str]).fetchone()
        if row and row[0]:
            last_dt = row[0] if isinstance(row[0], datetime) else datetime.strptime(str(row[0])[:19], "%Y-%m-%d %H:%M:%S")
            days_since_last = max(0, (date_to_dt - last_dt).days)
    except Exception:
        pass

    try:
        row = conn.execute("""
            SELECT
                COUNT(CASE WHEN severity IN ('Critical','CRITICAL','Major','MAJOR') THEN 1 END) as cm,
                COUNT(*) as total
            FROM noc_tickets
            WHERE site_id = ? AND occured_time >= ? AND occured_time <= ?
        """, [site_id, date_from_90d, date_to_str]).fetchone()
        if row and row[1] > 0:
            crit_major_pct = row[0] / row[1] * 100
    except Exception:
        pass

    try:
        rows = conn.execute("""
            SELECT calc_year_month, AVG(calc_repair_time_min) as avg_mttr
            FROM noc_tickets
            WHERE site_id = ? AND occured_time >= ? AND occured_time <= ?
                AND calc_repair_time_min IS NOT NULL
            GROUP BY calc_year_month
            ORDER BY calc_year_month
        """, [site_id, date_from_90d, date_to_str]).fetchall()
        if len(rows) >= 2:
            mttr_values = [r[1] for r in rows if r[1] is not None]
            if len(mttr_values) >= 2:
                mttr_slope = linear_regression_slope(range(len(mttr_values)), mttr_values)
    except Exception:
        pass

    try:
        row = conn.execute("""
            SELECT COUNT(*) as total,
                   COUNT(CASE WHEN is_escalate IN ('Yes','YES','true','1') THEN 1 END) as esc
            FROM noc_tickets
            WHERE site_id = ? AND occured_time >= ? AND occured_time <= ?
        """, [site_id, date_from_90d, date_to_str]).fetchone()
        if row and row[0] > 0:
            esc_pct = row[1] / row[0] * 100
    except Exception:
        pass

    try:
        rows = conn.execute("""
            SELECT occured_time FROM noc_tickets
            WHERE site_id = ? AND occured_time >= ? AND occured_time <= ?
            ORDER BY occured_time
        """, [site_id, date_from_90d, date_to_str]).fetchall()
        if len(rows) >= 2:
            repeat_count = 0
            for i in range(1, len(rows)):
                prev = rows[i-1][0]
                curr = rows[i][0]
                if prev and curr:
                    try:
                        p = prev if isinstance(prev, datetime) else datetime.strptime(str(prev)[:19], "%Y-%m-%d %H:%M:%S")
                        c = curr if isinstance(curr, datetime) else datetime.strptime(str(curr)[:19], "%Y-%m-%d %H:%M:%S")
                        if (c - p).total_seconds() < 7 * 86400:
                            repeat_count += 1
                    except Exception:
                        pass
            if len(rows) > 0:
                repeat_pct = repeat_count / len(rows) * 100
    except Exception:
        pass

    try:
        row = conn.execute("""
            SELECT equipment_age_years FROM master_site WHERE site_id = ?
        """, [site_id]).fetchone()
        if row and row[0]:
            device_age = float(row[0])
    except Exception:
        pass

    frequency_score = min(ticket_count_90d / 50 * 100, 100)
    recency_score = max(0, (90 - days_since_last) / 90 * 100)
    severity_score = min(crit_major_pct / 50 * 100, 100)
    mttr_trend_score = min(max(mttr_slope, 0) / 100 * 100, 100)
    repeat_score = min(repeat_pct / 40 * 100, 100)
    device_score = min(device_age / 7 * 100, 100)
    escalation_score = min(esc_pct / 20 * 100, 100)

    components = {
        "frequency": round(frequency_score, 1),
        "recency": round(recency_score, 1),
        "severity": round(severity_score, 1),
        "mttr_trend": round(mttr_trend_score, 1),
        "repeat": round(repeat_score, 1),
        "device": round(device_score, 1),
        "escalation": round(escalation_score, 1),
    }

    total_score = sum(components[k] * WEIGHTS[k] for k in WEIGHTS)
    total_score = round(total_score, 1)

    status = classify_risk(total_score)
    top_component = max(components, key=components.get)
    interpretation = interpret_risk(total_score, components)

    formula_parts = []
    for k in WEIGHTS:
        formula_parts.append(f"{components[k]:.1f}×{WEIGHTS[k]:.2f}")
    formula_text = (
        f"Risk = Σ(komponen × bobot)\n"
        f"= ({') + ('.join(formula_parts)})\n"
        f"= {total_score:.1f}"
    )

    radar_data = [
        {"axis": COMP_LABELS[k], "value": components[k], "weight": WEIGHTS[k]}
        for k in WEIGHTS
    ]

    return {
        "risk_score": total_score,
        "components": components,
        "weights": WEIGHTS,
        "status": status,
        "top_component": top_component,
        "top_component_label": COMP_LABELS[top_component],
        "narrative": interpretation["narrative"],
        "formula_text": formula_text,
        "radar_data": radar_data,
    }


def batch_calculate_risk_scores(conn, site_ids, date_to, entity_level=None, entity_id=None):
    if not site_ids:
        return {}

    try:
        date_to_dt = datetime.strptime(date_to, "%Y-%m-%d") if isinstance(date_to, str) else date_to
    except Exception:
        date_to_dt = datetime.now()

    date_to_ym = date_to_dt.strftime("%Y-%m")
    date_from_90d_ym = (date_to_dt - timedelta(days=90)).strftime("%Y-%m")

    safe_ids = [sid.replace("'", "''") for sid in set(site_ids)]
    id_list_sql = ",".join([f"'{sid}'" for sid in safe_ids])
    where = f"site_id IN ({id_list_sql}) AND site_id IS NOT NULL AND severity IS NULL AND type_ticket IS NULL"

    try:
        rows = conn.execute(f"""
            SELECT site_id,
                   SUM(total_tickets) as tickets,
                   SUM(count_critical + count_major) as cm_total,
                   SUM(total_escalated) as esc_total,
                   SUM(total_repeat) as rep_total,
                   MAX(year_month) as last_month
            FROM summary_monthly
            WHERE {where}
              AND year_month >= '{date_from_90d_ym}' AND year_month <= '{date_to_ym}'
            GROUP BY site_id
        """).fetchall()
    except Exception as e:
        logger.warning(f"Batch summary query failed: {e}")
        rows = []

    freq_map = {}
    severity_map = {}
    esc_map = {}
    repeat_map = {}
    recency_map = {}

    for r in rows:
        sid = r[0]
        tickets = r[1] or 0
        freq_map[sid] = tickets
        severity_map[sid] = ((r[2] or 0) / tickets * 100) if tickets > 0 else 0
        esc_map[sid] = ((r[3] or 0) / tickets * 100) if tickets > 0 else 0
        repeat_map[sid] = ((r[4] or 0) / tickets * 100) if tickets > 0 else 0
        if r[5]:
            try:
                last_ym = datetime.strptime(r[5], "%Y-%m")
                last_ym_end = (last_ym.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
                recency_map[sid] = max(0, (date_to_dt - last_ym_end).days)
            except Exception:
                pass

    mttr_map = {}
    try:
        mrows = conn.execute(f"""
            SELECT site_id, year_month, avg_mttr_min
            FROM summary_monthly
            WHERE {where}
              AND year_month >= '{date_from_90d_ym}' AND year_month <= '{date_to_ym}'
              AND avg_mttr_min IS NOT NULL AND avg_mttr_min > 0
            ORDER BY site_id, year_month
        """).fetchall()
        current_site = None
        mttr_values = []
        for r in mrows:
            if r[0] != current_site:
                if current_site and len(mttr_values) >= 2:
                    mttr_map[current_site] = linear_regression_slope(range(len(mttr_values)), mttr_values)
                current_site = r[0]
                mttr_values = []
            if r[2] is not None:
                mttr_values.append(r[2])
        if current_site and len(mttr_values) >= 2:
            mttr_map[current_site] = linear_regression_slope(range(len(mttr_values)), mttr_values)
    except Exception as e:
        logger.warning(f"Batch MTTR query failed: {e}")

    device_map = {}
    try:
        drows = conn.execute(f"""
            SELECT site_id, equipment_age_years
            FROM master_site
            WHERE site_id IN (SELECT DISTINCT site_id FROM summary_monthly WHERE {where} AND year_month >= '{date_from_90d_ym}')
        """).fetchall()
        for r in drows:
            device_map[r[0]] = float(r[1]) if r[1] else 0
    except Exception as e:
        logger.warning(f"Batch device query failed: {e}")

    results = {}
    all_site_ids = set(site_ids) | set(freq_map.keys())
    for sid in all_site_ids:
        ticket_count = freq_map.get(sid, 0)
        days_since_last = recency_map.get(sid, 999)
        crit_major_pct = severity_map.get(sid, 0)
        mttr_slope = mttr_map.get(sid, 0)
        rep_pct = repeat_map.get(sid, 0)
        dev_age = device_map.get(sid, 0)
        escalation_pct = esc_map.get(sid, 0)

        frequency_score = min(ticket_count / 50 * 100, 100)
        recency_score = max(0, (90 - days_since_last) / 90 * 100)
        severity_score = min(crit_major_pct / 50 * 100, 100)
        mttr_trend_score = min(max(mttr_slope, 0) / 100 * 100, 100)
        repeat_score = min(rep_pct / 40 * 100, 100)
        device_score = min(dev_age / 7 * 100, 100)
        escalation_score = min(escalation_pct / 20 * 100, 100)

        components = {
            "frequency": round(frequency_score, 1),
            "recency": round(recency_score, 1),
            "severity": round(severity_score, 1),
            "mttr_trend": round(mttr_trend_score, 1),
            "repeat": round(repeat_score, 1),
            "device": round(device_score, 1),
            "escalation": round(escalation_score, 1),
        }

        total_score = sum(components[k] * WEIGHTS[k] for k in WEIGHTS)
        total_score = round(total_score, 1)

        status = classify_risk(total_score)
        top_component = max(components, key=components.get)

        results[sid] = {
            "risk_score": total_score,
            "components": components,
            "status": status,
            "top_component": top_component,
            "top_component_label": COMP_LABELS[top_component],
        }

    return results


def interpret_risk_aggregation(n_high, n_medium, n_low, total, worst):
    pct_high = n_high / total * 100 if total > 0 else 0

    parts = []
    if pct_high > 20:
        parts.append(
            f" {n_high} dari {total} site ({pct_high:.0f}%) berisiko tinggi. "
            f"Masalah bersifat sistemik — diperlukan program perbaikan menyeluruh."
        )
    elif pct_high > 10:
        parts.append(
            f" Proporsi site berisiko tinggi ({pct_high:.0f}%) di atas 10%. "
            f"Program maintenance intensif diperlukan."
        )
    elif pct_high > 0:
        parts.append(
            f"{n_high} site ({pct_high:.0f}%) berisiko tinggi. "
            f"Fokus PM pada site-site ini."
        )
    else:
        parts.append(" Tidak ada site berisiko tinggi.")

    if worst and worst.get("score", 0) > 85:
        parts.append(
            f" {worst['name']} memiliki risk score {worst['score']} — sangat kritis."
        )

    return {"narrative": " ".join(parts)}


def forecast_volume(monthly_data, horizon=3):
    values = [d["total_tickets"] for d in monthly_data]
    n = len(values)

    if n < 3:
        return {"error": "Minimal 3 bulan data untuk forecast"}

    wma_window = min(4, n)
    weights_wma = list(range(1, wma_window + 1))
    wma = sum(w * v for w, v in zip(weights_wma, values[-wma_window:])) / sum(weights_wma)

    x = list(range(n))
    slope = linear_regression_slope(x, values)

    seasonal_factor = 1.0
    if n >= 12:
        avg_all = mean(values)
        if avg_all > 0:
            for h in range(1, horizon + 1):
                target_month_idx = (n + h - 1) % 12
                if target_month_idx < n:
                    seasonal_factor = values[target_month_idx] / avg_all

    forecasts = []
    residuals_std = stdev(values) if n > 2 else 0
    for h in range(1, horizon + 1):
        forecast_val = wma * seasonal_factor + slope * h
        ci_margin = 1.96 * residuals_std * (1 + h * 0.1)

        forecasts.append({
            "period_offset": h,
            "forecast": max(0, round(forecast_val)),
            "ci_lower": max(0, round(forecast_val - ci_margin)),
            "ci_upper": round(forecast_val + ci_margin),
        })

    last_actual = values[-1]
    first_forecast = forecasts[0]["forecast"]
    pct_change = (first_forecast - last_actual) / last_actual * 100 if last_actual > 0 else 0

    return {
        "historical": [
            {"period": d["year_month"], "value": d["total_tickets"]}
            for d in monthly_data
        ],
        "forecasts": forecasts,
        "method": {
            "wma_value": round(wma),
            "wma_window": wma_window,
            "trend_slope": round(slope, 1),
            "seasonal_factor": round(seasonal_factor, 3),
        },
        "pct_change": round(pct_change, 1),
        "trend_word": "Naik" if pct_change > 5 else ("Turun" if pct_change < -5 else "Stabil"),
        "formula_text": (
            f"Base: WMA({wma_window}) = {wma:,.0f}\n"
            f"Trend: {slope:+,.0f}/bulan\n"
            f"Seasonal: ×{seasonal_factor:.3f}\n"
            f"Forecast = {wma:,.0f} × {seasonal_factor:.3f} + ({slope:+,.0f} × t)"
        ),
    }


def predict_sla_breach(sla_data, target, horizon_weeks=8):
    values = [d["sla_pct"] for d in sla_data]
    n = len(values)

    if n < 3:
        return {"error": "Minimal 3 periode data"}

    slope = linear_regression_slope(range(n), values)
    current = values[-1]

    projections = []
    breach_week = None
    for w in range(1, horizon_weeks + 1):
        projected = current + slope * w
        projections.append({
            "week_offset": w,
            "projected_sla": round(projected, 2),
        })
        if projected < target and breach_week is None:
            breach_week = w

    if current < target:
        weeks_ago = 0
        for i in range(len(values) - 1, -1, -1):
            if values[i] >= target:
                weeks_ago = n - 1 - i
                break
        status = "already_breached"
        narrative = (
            f" SLA SUDAH di bawah target ({current:.1f}% vs {target}%). "
            f"Breach dimulai ~{weeks_ago} periode lalu."
        )
    elif breach_week is not None:
        status = "breach_predicted"
        narrative = (
            f" SLA saat ini {current:.1f}% (memenuhi target {target}%), "
            f"namun tren menunjukkan BREACH dalam ~{breach_week} minggu "
            f"jika tidak ada intervensi."
        )
    else:
        status = "safe"
        narrative = (
            f" SLA {current:.1f}% diprediksi tetap di atas target "
            f"dalam {horizon_weeks} minggu ke depan."
        )

    return {
        "current_sla": current,
        "target": target,
        "slope_per_period": round(slope, 3),
        "projections": projections,
        "breach_week": breach_week,
        "status": status,
        "narrative": narrative,
        "formula_text": (
            f"SLA(t+w) = {current:.1f} + ({slope:+.3f} × w)\n"
            f"Target: {target}%\n"
            f"Breach ketika SLA(t+w) < {target}%"
        ),
    }


def detect_pattern(conn, site_id, date_from, date_to):
    try:
        rows = conn.execute("""
            SELECT occured_time FROM noc_tickets
            WHERE site_id = ? AND occured_time >= ? AND occured_time <= ?
            ORDER BY occured_time
        """, [site_id, date_from, date_to]).fetchall()
    except Exception:
        return {"pattern_detected": False, "reason": "Query error"}

    if len(rows) < 3:
        return {"pattern_detected": False, "reason": "Kurang dari 3 tiket"}

    timestamps = []
    for r in rows:
        if r[0]:
            try:
                t = r[0] if isinstance(r[0], datetime) else datetime.strptime(str(r[0])[:19], "%Y-%m-%d %H:%M:%S")
                timestamps.append(t)
            except Exception:
                pass

    if len(timestamps) < 3:
        return {"pattern_detected": False, "reason": "Kurang dari 3 tiket valid"}

    intervals = []
    for i in range(1, len(timestamps)):
        gap_hours = (timestamps[i] - timestamps[i-1]).total_seconds() / 3600
        gap_days = gap_hours / 24
        if gap_days > 0.5:
            intervals.append(gap_days)

    if len(intervals) < 2:
        return {"pattern_detected": False, "reason": "Interval terlalu sedikit"}

    avg_gap = mean(intervals)
    std_gap = stdev(intervals) if len(intervals) > 1 else 0
    cv = std_gap / avg_gap if avg_gap > 0 else 999

    if cv < 0.5:
        last_ticket_date = timestamps[-1]
        predicted_next = last_ticket_date + timedelta(days=avg_gap)
        days_until_next = max(0, (predicted_next - datetime.now()).days)
        buffer_days = max(3, int(avg_gap * 0.3))

        return {
            "pattern_detected": True,
            "avg_gap_days": round(avg_gap, 1),
            "cv": round(cv, 3),
            "consistency_pct": round((1 - cv) * 100, 0),
            "n_intervals": len(intervals),
            "predicted_next": predicted_next.isoformat(),
            "days_until_next": days_until_next,
            "narrative": (
                f"Pola terdeteksi: gangguan terjadi setiap ~{avg_gap:.0f} hari "
                f"(konsistensi {(1-cv)*100:.0f}%). "
                f"Prediksi insiden berikutnya: ~{predicted_next.strftime('%d %b %Y')}."
            ),
            "maintenance_window": {
                "start": (predicted_next - timedelta(days=buffer_days)).isoformat(),
                "end": predicted_next.isoformat(),
                "narrative": (
                    f"Maintenance direkomendasikan {buffer_days} hari "
                    f"sebelum prediksi insiden."
                ),
            },
        }
    else:
        return {
            "pattern_detected": False,
            "avg_gap_days": round(avg_gap, 1),
            "cv": round(cv, 3),
            "narrative": (
                f"Pola TIDAK konsisten — interval bervariasi "
                f"(rata-rata {avg_gap:.0f} hari, CV: {cv:.2f}). "
                f"Prediksi kurang reliable."
            ),
        }


def batch_detect_patterns(conn, site_ids, date_from, date_to):
    if not site_ids:
        return {}

    safe_ids = [sid.replace("'", "''") for sid in site_ids]
    id_list = ",".join([f"'{sid}'" for sid in safe_ids])

    try:
        rows = conn.execute(f"""
            SELECT site_id, occured_time FROM noc_tickets
            WHERE site_id IN ({id_list})
              AND occured_time >= ? AND occured_time <= ?
            ORDER BY site_id, occured_time
        """, [date_from, date_to]).fetchall()
    except Exception as e:
        logger.warning(f"batch_detect_patterns query failed: {e}")
        return {}

    from collections import defaultdict
    site_timestamps = defaultdict(list)
    for r in rows:
        if r[0] and r[1]:
            try:
                t = r[1] if isinstance(r[1], datetime) else datetime.strptime(str(r[1])[:19], "%Y-%m-%d %H:%M:%S")
                site_timestamps[r[0]].append(t)
            except Exception:
                pass

    results = {}
    for sid in site_ids:
        timestamps = site_timestamps.get(sid, [])
        if len(timestamps) < 3:
            results[sid] = {"pattern_detected": False, "reason": "Kurang dari 3 tiket"}
            continue

        intervals = []
        for i in range(1, len(timestamps)):
            gap_days = (timestamps[i] - timestamps[i-1]).total_seconds() / 86400
            if gap_days > 0.5:
                intervals.append(gap_days)

        if len(intervals) < 2:
            results[sid] = {"pattern_detected": False, "reason": "Interval terlalu sedikit"}
            continue

        avg_gap = mean(intervals)
        std_gap = stdev(intervals) if len(intervals) > 1 else 0
        cv = std_gap / avg_gap if avg_gap > 0 else 999

        if cv < 0.5:
            last_ticket_date = timestamps[-1]
            predicted_next = last_ticket_date + timedelta(days=avg_gap)
            days_until_next = max(0, (predicted_next - datetime.now()).days)
            buffer_days = max(3, int(avg_gap * 0.3))
            results[sid] = {
                "pattern_detected": True,
                "avg_gap_days": round(avg_gap, 1),
                "cv": round(cv, 3),
                "consistency_pct": round((1 - cv) * 100, 0),
                "n_intervals": len(intervals),
                "predicted_next": predicted_next.isoformat(),
                "days_until_next": days_until_next,
                "narrative": (
                    f"Pola terdeteksi: gangguan terjadi setiap ~{avg_gap:.0f} hari "
                    f"(konsistensi {(1-cv)*100:.0f}%). "
                    f"Prediksi insiden berikutnya: ~{predicted_next.strftime('%d %b %Y')}."
                ),
                "maintenance_window": {
                    "start": (predicted_next - timedelta(days=buffer_days)).isoformat(),
                    "end": predicted_next.isoformat(),
                    "narrative": f"Maintenance direkomendasikan {buffer_days} hari sebelum prediksi insiden.",
                },
            }
        else:
            results[sid] = {
                "pattern_detected": False,
                "avg_gap_days": round(avg_gap, 1),
                "cv": round(cv, 3),
                "narrative": (
                    f"Pola TIDAK konsisten — interval bervariasi "
                    f"(rata-rata {avg_gap:.0f} hari, CV: {cv:.2f}). "
                    f"Prediksi kurang reliable."
                ),
            }

    return results


def generate_maintenance_schedule(items):
    schedule = []
    for item in items:
        risk_score = item.get("risk_score", 0)
        pattern = item.get("pattern", {})
        device_age = item.get("device_age", 0)
        site_id = item.get("site_id", "")
        site_name = item.get("site_name", "")
        target_month = item.get("target_month")
        target_year = item.get("target_year")

        est_hours = 4 if risk_score >= 70 else 3

        if pattern.get("pattern_detected") and pattern.get("days_until_next", 999) <= 30:
            buffer_days = 3 if risk_score >= 70 else 5
            predicted_next_str = pattern.get("predicted_next", "")
            try:
                predicted_next = datetime.fromisoformat(predicted_next_str)
                pm_date = predicted_next - timedelta(days=buffer_days)
                if target_month and target_year:
                    if pm_date.month != target_month or pm_date.year != target_year:
                        continue
                schedule.append({
                    "site_id": site_id,
                    "site_name": site_name,
                    "date": pm_date.strftime("%Y-%m-%d"),
                    "priority": "high" if risk_score >= 70 else "medium",
                    "reason": f"Predicted incident ~{predicted_next.strftime('%d %b')}",
                    "estimated_hours": est_hours,
                    "risk_score": risk_score,
                })
            except Exception:
                pass

        elif device_age > 7 and risk_score > 50:
            if target_month and target_year:
                import calendar
                _, last_day = calendar.monthrange(target_year, target_month)
                pm_date = datetime(target_year, target_month, min(15, last_day))
            else:
                pm_date = datetime.now() + timedelta(days=14)
            schedule.append({
                "site_id": site_id,
                "site_name": site_name,
                "date": pm_date.strftime("%Y-%m-%d"),
                "priority": "medium",
                "reason": f"Device aging ({device_age:.0f} tahun)",
                "estimated_hours": est_hours,
                "risk_score": risk_score,
            })

        elif risk_score >= 70 and not pattern.get("pattern_detected"):
            if target_month and target_year:
                pm_date = datetime(target_year, target_month, 7)
            else:
                pm_date = datetime.now() + timedelta(days=7)
            schedule.append({
                "site_id": site_id,
                "site_name": site_name,
                "date": pm_date.strftime("%Y-%m-%d"),
                "priority": "high",
                "reason": f"High risk ({risk_score:.0f}), no pattern",
                "estimated_hours": est_hours,
                "risk_score": risk_score,
            })

    priority_order = {"high": 0, "medium": 1, "low": 2}
    schedule.sort(key=lambda s: (priority_order.get(s["priority"], 2), s["date"]))

    total_hours = sum(s["estimated_hours"] for s in schedule)
    n_high = sum(1 for s in schedule if s["priority"] == "high")
    n_medium = sum(1 for s in schedule if s["priority"] == "medium")
    n_low = sum(1 for s in schedule if s["priority"] == "low")

    calendar_data = {}
    for s in schedule:
        d = s["date"]
        if d not in calendar_data:
            calendar_data[d] = []
        calendar_data[d].append({
            "id": s["site_id"],
            "name": s["site_name"],
            "priority": s["priority"],
        })

    calendar_list = [
        {"date": d, "sites": sites}
        for d, sites in sorted(calendar_data.items())
    ]

    return {
        "schedule": schedule,
        "summary": {
            "n_high": n_high,
            "n_medium": n_medium,
            "n_low": n_low,
            "total_hours": total_hours,
            "capacity_hours": None,
            "utilization_pct": None,
        },
        "capacity_alert": None,
        "calendar_data": calendar_list,
        "formula_text": (
            "PM window = predicted_next - buffer hari\n"
            "Buffer: HIGH=3 hari, MEDIUM=5 hari, LOW=7 hari"
        ),
    }


def get_monthly_volume_data(conn, entity_level, entity_id, date_from=None, date_to=None):
    level_col_map = {
        "site": "site_id",
        "to": "to_id",
        "nop": "nop_id",
        "regional": "regional_id",
        "area": "area_id",
    }
    col = level_col_map.get(entity_level)
    if not col:
        return []

    query = f"""
        SELECT year_month, SUM(total_tickets) as total_tickets
        FROM summary_monthly
        WHERE {col} = ?
    """
    params = [entity_id]

    if date_from:
        query += " AND year_month >= ?"
        params.append(date_from)
    if date_to:
        query += " AND year_month <= ?"
        params.append(date_to)

    query += " GROUP BY year_month ORDER BY year_month"

    try:
        rows = conn.execute(query, params).fetchall()
        return [{"year_month": r[0], "total_tickets": r[1] or 0} for r in rows]
    except Exception:
        return []


def get_monthly_sla_data(conn, entity_level, entity_id, date_from=None, date_to=None):
    level_col_map = {
        "site": "site_id",
        "to": "to_id",
        "nop": "nop_id",
        "regional": "regional_id",
        "area": "area_id",
    }
    col = level_col_map.get(entity_level)
    if not col:
        return []

    query = f"""
        SELECT year_month,
               SUM(total_sla_met) as met,
               SUM(total_tickets) as total
        FROM summary_monthly
        WHERE {col} = ?
    """
    params = [entity_id]

    if date_from:
        query += " AND year_month >= ?"
        params.append(date_from)
    if date_to:
        query += " AND year_month <= ?"
        params.append(date_to)

    query += " GROUP BY year_month ORDER BY year_month"

    try:
        rows = conn.execute(query, params).fetchall()
        result = []
        for r in rows:
            met = r[1] or 0
            total = r[2] or 0
            sla_pct = (met / total * 100) if total > 0 else 0
            result.append({"period": r[0], "sla_pct": round(sla_pct, 2)})
        return result
    except Exception:
        return []


def get_child_sites(conn, entity_level, entity_id):
    if entity_level == "to":
        query = "SELECT site_id, site_name, equipment_age_years FROM master_site WHERE to_id = ? AND status = 'ACTIVE'"
    elif entity_level == "nop":
        query = """
            SELECT s.site_id, s.site_name, s.equipment_age_years
            FROM master_site s JOIN master_to t ON s.to_id = t.to_id
            WHERE t.nop_id = ? AND s.status = 'ACTIVE'
        """
    elif entity_level == "regional":
        query = """
            SELECT s.site_id, s.site_name, s.equipment_age_years
            FROM master_site s
            JOIN master_to t ON s.to_id = t.to_id
            JOIN master_nop n ON t.nop_id = n.nop_id
            WHERE n.regional_id = ? AND s.status = 'ACTIVE'
        """
    elif entity_level == "area":
        query = """
            SELECT s.site_id, s.site_name, s.equipment_age_years
            FROM master_site s
            JOIN master_to t ON s.to_id = t.to_id
            JOIN master_nop n ON t.nop_id = n.nop_id
            JOIN master_regional r ON n.regional_id = r.regional_id
            WHERE r.area_id = ? AND s.status = 'ACTIVE'
        """
    else:
        return []

    try:
        rows = conn.execute(query, [entity_id]).fetchall()
        if rows:
            return [
                {"site_id": r[0], "site_name": r[1], "equipment_age_years": r[2] or 0}
                for r in rows
            ]
    except Exception:
        pass

    if entity_level in ("area", "regional", "nop"):
        col_map = {"area": "calc_area_id", "regional": "calc_regional_id", "nop": "calc_nop_id"}
        col = col_map.get(entity_level)
        if col:
            try:
                rows = conn.execute(f"""
                    SELECT DISTINCT t.site_id, COALESCE(s.site_name, t.site_name) as site_name,
                           COALESCE(s.equipment_age_years, 0) as equipment_age_years
                    FROM noc_tickets t
                    LEFT JOIN master_site s ON t.site_id = s.site_id
                    WHERE t.{col} = ?
                """, [entity_id]).fetchall()
                return [
                    {"site_id": r[0], "site_name": r[1], "equipment_age_years": r[2] or 0}
                    for r in rows
                ]
            except Exception:
                pass

    return []


def get_child_entities(conn, entity_level, entity_id):
    if entity_level == "to":
        return get_child_sites(conn, entity_level, entity_id)
    elif entity_level == "nop":
        try:
            rows = conn.execute(
                "SELECT to_id, to_name FROM master_to WHERE nop_id = ? AND status = 'ACTIVE'",
                [entity_id]
            ).fetchall()
            return [{"id": r[0], "name": r[1], "level": "to"} for r in rows]
        except Exception:
            return []
    elif entity_level == "regional":
        try:
            rows = conn.execute(
                "SELECT nop_id, nop_name FROM master_nop WHERE regional_id = ? AND status = 'ACTIVE'",
                [entity_id]
            ).fetchall()
            return [{"id": r[0], "name": r[1], "level": "nop"} for r in rows]
        except Exception:
            return []
    elif entity_level == "area":
        try:
            rows = conn.execute(
                "SELECT regional_id, regional_name FROM master_regional WHERE area_id = ? AND status = 'ACTIVE'",
                [entity_id]
            ).fetchall()
            return [{"id": r[0], "name": r[1], "level": "regional"} for r in rows]
        except Exception:
            return []
    return []
