import math
from statistics import mean


VOLUME_THRESHOLDS = {
    "site": 20,
    "to": 500,
    "nop": 2000,
    "regional": 10000,
    "area": 30000,
}

BEHAVIOR_META = {
    "CHRONIC": {"icon": "", "color": "red", "label_id": "Kronis"},
    "DETERIORATING": {"icon": "", "color": "orange", "label_id": "Memburuk"},
    "SPORADIC": {"icon": "", "color": "amber", "label_id": "Sporadis"},
    "SEASONAL": {"icon": "", "color": "yellow", "label_id": "Musiman"},
    "IMPROVING": {"icon": "", "color": "blue", "label_id": "Membaik"},
    "HEALTHY": {"icon": "", "color": "green", "label_id": "Sehat"},
}


def linear_slope(values):
    n = len(values)
    if n < 2:
        return 0.0
    x_mean = (n - 1) / 2.0
    y_mean = mean(values)
    num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
    den = sum((i - x_mean) ** 2 for i in range(n))
    if den == 0:
        return 0.0
    return num / den


def coefficient_of_variation(values):
    if not values or len(values) < 2:
        return 0.0
    avg = mean(values)
    if avg == 0:
        return 0.0
    variance = sum((v - avg) ** 2 for v in values) / len(values)
    return math.sqrt(variance) / avg


def count_consecutive_above(values, threshold):
    max_run = 0
    current = 0
    for v in values:
        if v > threshold:
            current += 1
            max_run = max(max_run, current)
        else:
            current = 0
    return max_run


def calc_critical_major_pct(months_data):
    total = sum(m.get("total_tickets", 0) for m in months_data)
    if total == 0:
        return 0.0
    crit_maj = sum(m.get("count_critical", 0) + m.get("count_major", 0) for m in months_data)
    return crit_maj / total * 100


def classify_behavior(entity_level, months_data):
    if not months_data or len(months_data) < 2:
        if months_data and len(months_data) == 1:
            sla = months_data[0].get("sla_pct", 100)
            if sla >= 90:
                return {"label": "HEALTHY", "reason": f"SLA {sla:.1f}% (data 1 bulan)"}
            return {"label": "DETERIORATING", "reason": f"SLA {sla:.1f}% di bawah target (data 1 bulan)"}
        return {"label": "HEALTHY", "reason": "Data tidak cukup untuk klasifikasi"}

    tickets_list = [m.get("total_tickets", 0) for m in months_data]
    sla_list = [m.get("sla_pct", 0) for m in months_data]
    avg_tickets = mean(tickets_list)
    avg_sla = mean(sla_list)
    sla_slope = linear_slope(sla_list)
    high_thr = VOLUME_THRESHOLDS.get(entity_level, 20)

    consecutive_high = count_consecutive_above(tickets_list, high_thr)
    if consecutive_high >= 3 and avg_sla < 90:
        return {"label": "CHRONIC", "reason": f"Volume >{high_thr}/bln, {consecutive_high} bln, SLA {avg_sla:.1f}%"}

    if sla_slope < -1.5 and len(months_data) >= 3:
        return {"label": "DETERIORATING", "reason": f"SLA turun {abs(sla_slope):.1f}pp/bln, {len(months_data)} bulan"}
    if sla_slope < -0.5 and len(months_data) >= 2:
        return {"label": "DETERIORATING", "reason": f"SLA turun {abs(sla_slope):.1f}pp/bln"}

    crit_maj_pct = calc_critical_major_pct(months_data)
    if avg_tickets <= high_thr * 0.3 and crit_maj_pct > 30:
        return {"label": "SPORADIC", "reason": f"Volume rendah, {crit_maj_pct:.0f}% severity tinggi"}

    cv = coefficient_of_variation(tickets_list)
    if cv < 0.5 and len(months_data) >= 4:
        peak_factor = max(tickets_list) / max(avg_tickets, 1)
        if peak_factor > 1.3:
            return {"label": "SEASONAL", "reason": f"Peak factor {peak_factor:.1f}×"}

    if sla_slope > 0.5 and len(months_data) >= 2:
        return {"label": "IMPROVING", "reason": f"SLA naik {sla_slope:.1f}pp/bln"}

    if avg_sla >= 95 or (avg_sla >= 90 and avg_tickets <= high_thr * 0.5):
        return {"label": "HEALTHY", "reason": f"SLA {avg_sla:.1f}%"}

    if avg_sla >= 90:
        return {"label": "HEALTHY", "reason": "SLA memenuhi target"}
    else:
        return {"label": "DETERIORATING", "reason": f"SLA {avg_sla:.1f}% di bawah target"}


def get_behavior_with_meta(entity_level, months_data):
    result = classify_behavior(entity_level, months_data)
    meta = BEHAVIOR_META.get(result["label"], BEHAVIOR_META["HEALTHY"])
    return {**result, **meta}


def interpret_sla(sla_pct, target=90.0):
    delta = sla_pct - target
    if delta >= 0:
        return {"status": "good", "color": "green", "text": "Memenuhi target", "delta": delta}
    elif delta >= -2:
        return {"status": "warning", "color": "yellow", "text": f"Mendekati batas, selisih {abs(delta):.1f}pp", "delta": delta}
    elif delta >= -5:
        return {"status": "alert", "color": "orange", "text": f"Di bawah target {abs(delta):.1f}pp, perlu tindakan", "delta": delta}
    else:
        return {"status": "critical", "color": "red", "text": f"Kritis, jauh di bawah target {abs(delta):.1f}pp", "delta": delta}


def interpret_mttr(mttr_min):
    if mttr_min <= 240:
        return {"status": "good", "color": "green", "text": "Baik"}
    elif mttr_min <= 720:
        return {"status": "warning", "color": "yellow", "text": "Perlu perhatian"}
    elif mttr_min <= 1440:
        return {"status": "alert", "color": "orange", "text": "Lambat"}
    else:
        return {"status": "critical", "color": "red", "text": "Sangat lambat, investigasi proses perbaikan"}


def interpret_escalation(esc_pct):
    if esc_pct <= 3:
        return {"status": "good", "color": "green", "text": "Terkendali"}
    elif esc_pct <= 7:
        return {"status": "warning", "color": "yellow", "text": "Di atas normal (3%)"}
    else:
        return {"status": "critical", "color": "red", "text": "Tinggi, review kompetensi/kompleksitas"}


def interpret_auto_resolve(ar_pct):
    if ar_pct >= 60:
        return {"status": "good", "color": "green", "text": "Otomasi efektif"}
    elif ar_pct >= 40:
        return {"status": "warning", "color": "yellow", "text": "Ruang peningkatan otomasi"}
    else:
        return {"status": "alert", "color": "orange", "text": "Rendah, banyak manual"}


def interpret_repeat(rep_pct):
    if rep_pct <= 10:
        return {"status": "good", "color": "green", "text": "Minimal"}
    elif rep_pct <= 25:
        return {"status": "warning", "color": "yellow", "text": "RC perlu ditinjau"}
    else:
        return {"status": "critical", "color": "red", "text": "Tinggi — root cause belum terselesaikan"}


def interpret_volume(change_pct):
    if change_pct > 10:
        return {"status": "warning", "color": "yellow", "text": "Naik signifikan, periksa insiden massal"}
    elif change_pct < -10:
        return {"status": "good", "color": "green", "text": "Menurun"}
    else:
        return {"status": "neutral", "color": "gray", "text": "Stabil"}


def generate_summary_narrative(entity_name, entity_level, kpis, behavior, children_summary=None, granularity="bulan"):
    parts = []

    vol_label = kpis.get("avg_volume", 0)
    sla = kpis.get("sla_pct", 0)
    target = kpis.get("sla_target", 90.0)

    parts.append(f"{entity_name} memproses rata-rata {vol_label:,.0f} tiket/{granularity} dengan SLA {sla:.1f}%")

    delta = sla - target
    if delta >= 0:
        parts.append(f"({delta:.1f}pp di atas target {target:.0f}%).")
    else:
        parts.append(f"({abs(delta):.1f}pp di bawah target {target:.0f}%).")

    sla_slope = kpis.get("sla_trend_slope")
    trend_months = kpis.get("sla_trend_months", 0)
    if sla_slope is not None and abs(sla_slope) > 0.5 and trend_months >= 2:
        direction = "menurun" if sla_slope < 0 else "membaik"
        parts.append(f"Tren SLA {direction} {abs(sla_slope):.1f}pp/bulan selama {trend_months} bulan terakhir.")

    if children_summary and children_summary.get("worst"):
        w = children_summary["worst"]
        parts.append(
            f"{w.get('chronic_count', 1)} dari {children_summary.get('total', 0)} {children_summary.get('type_label', 'entitas')} "
            f"({w['name']}) berstatus {w.get('behavior', 'Chronic')} dan menyumbang {w.get('contribution_pct', 0):.0f}% total tiket."
        )

    return " ".join(parts)


def generate_recommendations(kpis, behavior, children_summary=None):
    recs = []

    sla = kpis.get("sla_pct", 100)
    target = kpis.get("sla_target", 90.0)
    if sla < target:
        delta = target - sla
        worst_name = "entitas terbawah"
        if children_summary and children_summary.get("worst"):
            worst_name = children_summary["worst"].get("name", worst_name)
        recs.append({
            "priority": "critical" if delta > 5 else "warning",
            "icon": "",
            "text": f"SLA gap {delta:.1f}pp — fokus perbaikan pada {worst_name}."
        })

    mttr = kpis.get("avg_mttr_min", 0)
    if mttr > 720:
        recs.append({
            "priority": "warning",
            "icon": "",
            "text": f"MTTR {mttr:.0f} menit — perlu percepatan proses perbaikan."
        })

    if children_summary:
        n_det = children_summary.get("by_behavior", {}).get("DETERIORATING", 0)
        if n_det > 0:
            type_label = children_summary.get("type_label", "entitas")
            recs.append({
                "priority": "warning",
                "icon": "",
                "text": f"{n_det} {type_label} memburuk — monitor dan intervensi."
            })

    esc = kpis.get("escalation_pct", 0)
    if esc > 7:
        recs.append({
            "priority": "critical",
            "icon": "",
            "text": f"Eskalasi {esc:.1f}% (>7%) — review kompetensi/kompleksitas."
        })

    rep = kpis.get("repeat_pct", 0)
    if rep > 25:
        recs.append({
            "priority": "critical",
            "icon": "",
            "text": f"Repeat {rep:.1f}% — RC belum solved, RCA mendalam diperlukan."
        })

    recs.sort(key=lambda r: {"critical": 0, "warning": 1, "info": 2}.get(r["priority"], 9))
    return recs[:5]
