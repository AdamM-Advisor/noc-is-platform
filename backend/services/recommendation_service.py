import logging

logger = logging.getLogger(__name__)

PRIORITY_ORDER = {"SEGERA": 0, "MINGGU_INI": 1, "BULAN_INI": 2, "RUTIN": 3}

PRIORITY_LABELS = {
    "SEGERA": {"label": "SEGERA", "icon": "", "color": "#DC2626"},
    "MINGGU_INI": {"label": "MINGGU INI", "icon": "", "color": "#D97706"},
    "BULAN_INI": {"label": "BULAN INI", "icon": "", "color": "#2563EB"},
    "RUTIN": {"label": "RUTIN", "icon": "", "color": "#16A34A"},
}

CHAINS = [
    (("R01", "R05", "R14"), "SLA_MTTR_REPEAT"),
    (("R01", "R05"), "SLA_MTTR"),
    (("R01", "R02"), "SLA_CRISIS"),
    (("R05", "R14"), "MTTR_REPEAT"),
]


class RecommendationEngine:
    def generate(self, entity, kpis, trend, risk, children, anomalies=None):
        raw_recs = []
        raw_recs += self._eval_sla_rules(entity, kpis, trend, children)
        raw_recs += self._eval_mttr_rules(entity, kpis, trend)
        raw_recs += self._eval_volume_rules(entity, kpis, trend)
        raw_recs += self._eval_risk_rules(entity, risk, children)
        raw_recs += self._eval_escalation_rules(entity, kpis)
        raw_recs += self._eval_trend_rules(entity, trend, anomalies, children)
        raw_recs += self._eval_device_rules(entity, risk, children)
        raw_recs += self._eval_capacity_rules(entity, kpis, children)
        raw_recs += self._eval_positive_rules(entity, kpis, trend)

        chained = self._chain_related(raw_recs)
        deduped = self._dedup_hierarchy(chained)
        sorted_recs = sorted(deduped, key=lambda r: PRIORITY_ORDER.get(r["priority"], 3))

        max_recs = 5
        return sorted_recs[:max_recs]

    def _eval_sla_rules(self, entity, kpis, trend, children):
        recs = []
        sla = kpis.get("sla_pct", 100)
        target = kpis.get("sla_target", 90)
        sla_quality = trend.get("sla_quality", "neutral")
        slope = trend.get("sla_slope", 0)
        child_type = entity.get("child_type", "entitas")

        if sla < target - 5:
            n_worst = min(3, len(children))
            worst = sorted(children, key=lambda c: c.get("sla_pct", 100))[:n_worst]
            worst_names = ", ".join(str(c.get("name") or c.get("id") or "?") for c in worst)
            recs.append({
                "rule_id": "R01", "priority": "SEGERA", "category": "SLA",
                "message": f"SLA {entity.get('name', '')} ({sla:.1f}%) jauh di bawah target ({target}%).",
                "action": f"Identifikasi {n_worst} {child_type} penyumbang breach terbesar: {worst_names}.",
                "impact": f"Perbaikan {n_worst} {child_type} terburuk → SLA naik ~2-3pp.",
            })

        if sla < target and sla_quality == "worsening":
            weeks_to_breach = abs((sla - (target - 10)) / slope * 4) if slope != 0 else 99
            projected = sla + slope * 4
            recs.append({
                "rule_id": "R02", "priority": "SEGERA", "category": "SLA",
                "message": f"SLA di bawah target DAN terus menurun ({slope:.1f}pp/bulan).",
                "action": f"Rapat darurat. Review proses response & repair. Tanpa intervensi, SLA mencapai {projected:.1f}% dalam {weeks_to_breach:.0f} minggu.",
            })

        if sla >= target and sla_quality == "worsening":
            weeks = abs((sla - target) / slope * 4) if slope != 0 else 99
            recs.append({
                "rule_id": "R03", "priority": "MINGGU_INI", "category": "SLA",
                "message": f"SLA masih memenuhi target tapi tren MENURUN ({slope:.1f}pp/bulan).",
                "action": f"Monitoring ketat. Breach dalam ~{weeks:.0f} minggu jika dibiarkan.",
            })

        if sla >= target + 3 and sla_quality != "worsening":
            recs.append({
                "rule_id": "R04", "priority": "RUTIN", "category": "SLA",
                "message": f"SLA ({sla:.1f}%) di atas target dengan margin baik.",
                "action": "Pertahankan. Share best practice ke peers.",
            })

        return recs

    def _eval_mttr_rules(self, entity, kpis, trend):
        recs = []
        mttr = kpis.get("avg_mttr_min", 0)
        mttr_quality = trend.get("mttr_quality", "neutral")
        mttr_slope = trend.get("mttr_slope", 0)
        response = kpis.get("avg_response_min", 0)

        if mttr > 1440:
            recs.append({
                "rule_id": "R05", "priority": "SEGERA", "category": "MTTR",
                "message": f"MTTR sangat tinggi ({mttr:.0f} menit = {mttr/60:.1f} jam).",
                "action": "Review bottleneck proses repair.",
            })

        if mttr_quality == "worsening" and abs(mttr_slope) > 10:
            recs.append({
                "rule_id": "R06", "priority": "MINGGU_INI", "category": "MTTR",
                "message": f"MTTR cenderung naik ({mttr_slope:.0f}%/bulan).",
                "action": "Investigasi penyebab kelambatan repair.",
            })

        if response > 60:
            recs.append({
                "rule_id": "R07", "priority": "MINGGU_INI", "category": "MTTR",
                "message": f"Response time tinggi ({response:.0f} menit).",
                "action": "Review alokasi personel dan coverage area.",
            })

        return recs

    def _eval_volume_rules(self, entity, kpis, trend):
        recs = []
        vol_mom = kpis.get("volume_mom_pct", 0)

        if abs(vol_mom) > 15:
            direction = "naik" if vol_mom > 0 else "turun"
            recs.append({
                "rule_id": "R08", "priority": "MINGGU_INI", "category": "VOLUME",
                "message": f"Volume tiket {direction} signifikan ({vol_mom:+.1f}% MoM).",
                "action": "Analisis: insiden massal, aging, monitoring threshold, atau musiman?",
            })

        return recs

    def _eval_risk_rules(self, entity, risk, children):
        recs = []
        risk_score = risk.get("risk_score", 0) if risk else 0
        pct_high = risk.get("pct_high_risk", 0) if risk else 0
        n_high = risk.get("n_high", 0) if risk else 0
        total_sites = risk.get("total_sites", 0) if risk else 0

        if risk_score >= 70:
            recs.append({
                "rule_id": "R10", "priority": "SEGERA", "category": "RISK",
                "message": f"Site berisiko TINGGI ({risk_score:.0f}/100).",
                "action": "Jadwalkan preventive maintenance.",
                "impact": "PM dapat mencegah ~60-70% insiden.",
            })

        if pct_high > 10:
            recs.append({
                "rule_id": "R11", "priority": "MINGGU_INI", "category": "RISK",
                "message": f"{n_high} dari {total_sites} site ({pct_high:.0f}%) berisiko tinggi.",
                "action": "Buat program maintenance batch. Kelompokkan per lokasi.",
            })

        pattern_days = risk.get("pattern_days_until", 999) if risk else 999
        if pattern_days <= 7:
            recs.append({
                "rule_id": "R12", "priority": "MINGGU_INI", "category": "RISK",
                "message": f"Pola gangguan terdeteksi. Next incident: ~{pattern_days} hari.",
                "action": "Maintenance preventif segera.",
            })

        return recs

    def _eval_escalation_rules(self, entity, kpis):
        recs = []
        esc_pct = kpis.get("escalation_pct", 0)
        repeat_pct = kpis.get("repeat_pct", 0)

        if esc_pct > 7:
            recs.append({
                "rule_id": "R13", "priority": "MINGGU_INI", "category": "ESCALATION",
                "message": f"Eskalasi tinggi ({esc_pct:.1f}%).",
                "action": "Review: kurang kompetensi tier-1, kompleksitas naik, atau kurang resource?",
            })

        if repeat_pct > 25:
            recs.append({
                "rule_id": "R14", "priority": "SEGERA", "category": "REPEAT",
                "message": f"Repeat incident sangat tinggi ({repeat_pct:.0f}%). Root cause belum terselesaikan.",
                "action": "Wajibkan RCA untuk setiap repeat incident.",
            })

        return recs

    def _eval_trend_rules(self, entity, trend, anomalies, children):
        recs = []
        if anomalies:
            for a in anomalies[:1]:
                recs.append({
                    "rule_id": "R15", "priority": "SEGERA", "category": "TREND",
                    "message": f"Anomali terdeteksi: {a.get('kpi', '')} = {a.get('value', '')} (z-score: {a.get('z', 0):.1f}).",
                    "action": "Investigasi: insiden besar, perubahan config, atau error data?",
                })

        n_worsening = sum(1 for c in children if c.get("trend_quality") == "worsening")
        parent_stable = trend.get("sla_quality") != "worsening"
        if parent_stable and n_worsening > 0 and len(children) > 0:
            pct = n_worsening / len(children) * 100
            if pct > 20:
                child_type = entity.get("child_type", "entitas")
                recs.append({
                    "rule_id": "R16", "priority": "MINGGU_INI", "category": "TREND",
                    "message": f"Entity stabil, NAMUN {n_worsening} {child_type} ({pct:.0f}%) trennya memburuk.",
                    "action": f"Review individual {child_type} yang memburuk — masalah ter-mask.",
                })

        return recs

    def _eval_device_rules(self, entity, risk, children):
        recs = []
        device_age = risk.get("device_age", 0) if risk else 0
        risk_score = risk.get("risk_score", 0) if risk else 0

        if device_age > 7 and risk_score > 50:
            recs.append({
                "rule_id": "R17", "priority": "BULAN_INI", "category": "DEVICE",
                "message": f"Perangkat usia {device_age:.0f} tahun (melebihi usia ekonomis) + risk {risk_score:.0f}.",
                "action": "Evaluasi penggantian perangkat. Masukkan ke rencana CAPEX.",
            })

        mttr = entity.get("avg_mttr_min", 0) if isinstance(entity, dict) else 0
        if not mttr:
            mttr = risk.get("avg_mttr_min", 0) if risk else 0
        if mttr > 2160:
            recs.append({
                "rule_id": "R18", "priority": "MINGGU_INI", "category": "DEVICE",
                "message": f"MTTR melebihi 3T ({mttr/60:.1f} jam). Keterbatasan resource/spare part.",
                "action": "Review stock spare part dan ketersediaan teknisi.",
            })

        return recs

    def _eval_capacity_rules(self, entity, kpis, children):
        recs = []
        vol = kpis.get("total_volume", 0)
        auto_pct = kpis.get("auto_resolve_pct", 0)

        if vol > 0 and auto_pct < 30:
            recs.append({
                "rule_id": "R09", "priority": "BULAN_INI", "category": "CAPACITY",
                "message": f"Auto-resolve rate rendah ({auto_pct:.1f}%). Kapasitas manual handling tinggi.",
                "action": "Review automation script dan monitoring threshold.",
            })

        return recs

    def _eval_positive_rules(self, entity, kpis, trend):
        recs = []
        sla_quality = trend.get("sla_quality", "neutral")
        consecutive = trend.get("consecutive", 0)
        slope = trend.get("sla_slope", 0)

        if sla_quality == "improving" and consecutive >= 3:
            recs.append({
                "rule_id": "R19", "priority": "RUTIN", "category": "POSITIVE",
                "message": f"Perbaikan konsisten: SLA naik {slope:.1f}pp/bulan selama {consecutive} bulan.",
                "action": "Pertahankan. Dokumentasikan best practice untuk peers.",
            })

        mttr_quality = trend.get("mttr_quality", "neutral")
        if mttr_quality == "improving":
            recs.append({
                "rule_id": "R20", "priority": "RUTIN", "category": "POSITIVE",
                "message": "MTTR menunjukkan tren perbaikan.",
                "action": "Pertahankan efisiensi repair. Evaluasi faktor pendukung.",
            })

        return recs

    def _chain_related(self, raw_recs):
        triggered_ids = {r["rule_id"] for r in raw_recs}
        chained = []
        consumed = set()

        for chain_ids, chain_name in sorted(CHAINS, key=lambda x: -len(x[0])):
            if all(rid in triggered_ids for rid in chain_ids) and not any(rid in consumed for rid in chain_ids):
                primary = next(r for r in raw_recs if r["rule_id"] == chain_ids[0])
                supporting = [r for r in raw_recs if r["rule_id"] in chain_ids[1:]]
                primary["chained_with"] = [s["rule_id"] for s in supporting]
                primary["message"] += " " + " ".join(f"Faktor: {s['message']}" for s in supporting)
                chained.append(primary)
                consumed.update(chain_ids)

        for r in raw_recs:
            if r["rule_id"] not in consumed:
                chained.append(r)

        return chained

    def _dedup_hierarchy(self, recs):
        seen_categories = set()
        deduped = []
        for r in sorted(recs, key=lambda x: PRIORITY_ORDER.get(x["priority"], 3)):
            key = (r["category"], r["priority"])
            if key not in seen_categories:
                seen_categories.add(key)
                deduped.append(r)
            elif r["priority"] == "SEGERA":
                deduped.append(r)
        return deduped
