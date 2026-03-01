import json
import logging
import math
import re
import time
from datetime import datetime
from statistics import mean, stdev, median
from backend.database import get_connection, get_write_connection

logger = logging.getLogger(__name__)

CATEGORY_MAP = {
    'Power':      {'code': 'PWR', 'name': 'Power System'},
    'Radio':      {'code': 'RAD', 'name': 'Radio Access'},
    'Transmisi':  {'code': 'TRX', 'name': 'Transmission'},
    'Aktivitas':  {'code': 'AKT', 'name': 'Aktivitas Pihak Ketiga'},
    'CME':        {'code': 'CME', 'name': 'Civil Mechanical Electrical'},
    'LOGIC':      {'code': 'LOG', 'name': 'Logic/Software'},
    'HW':         {'code': 'HW',  'name': 'Hardware'},
    'Core':       {'code': 'COR', 'name': 'Core Network'},
    'Lain - lain':{'code': 'OTH', 'name': 'Other'},
}
DEFAULT_CATEGORY = {'code': 'OTH', 'name': 'Other'}
MIN_TICKETS = 10

SYMPTOM_PATTERNS = {
    "Site unreachable / NE down": {
        "patterns": [r"site.*down", r"ne.*down", r"unreachable", r"not reachable", r"site.*off", r"ne.*off"],
        "source": "summary"
    },
    "Multiple alarm: Power fail + Battery low": {
        "patterns": [r"power.*fail.*batt", r"batt.*low.*power", r"power.*batt", r"power fail"],
        "source": "alarm_correlation"
    },
    "EAS tidak muncul / no external alarm": {
        "patterns": [r"eas.*tidak", r"eas.*no", r"no.*eas", r"eas.*off", r"external alarm.*tidak"],
        "source": "description"
    },
    "Coverage gap di area site": {
        "patterns": [r"coverage.*gap", r"no.*coverage", r"blank.*spot", r"layanan.*terganggu"],
        "source": "impact_analysis"
    },
    "User complaint / gangguan layanan": {
        "patterns": [r"user.*complain", r"pelanggan.*keluh", r"complaint", r"gangguan.*layanan", r"customer"],
        "source": "description"
    },
    "Neighboring site overloaded": {
        "patterns": [r"neighbor.*overload", r"traffic.*shift", r"cell.*congestion", r"handover.*fail"],
        "source": "description"
    },
    "Alarm berulang / flapping": {
        "patterns": [r"flapping", r"berulang", r"intermittent", r"recurring", r"repeat"],
        "source": "summary"
    },
    "Timeout / response lambat": {
        "patterns": [r"timeout", r"time.*out", r"lambat", r"slow.*response", r"delay"],
        "source": "description"
    },
    "Link down / transmisi putus": {
        "patterns": [r"link.*down", r"transmisi.*putus", r"fo.*putus", r"fiber.*cut"],
        "source": "summary"
    },
}


def _safe_q(val):
    if val is None:
        return None
    return str(val).replace("'", "''")


def _top_dist(conn, field, rc_cat, rc_1, rc_2, limit=3):
    rc2_clause = "AND rc_2 = ?" if rc_2 else "AND rc_2 IS NULL"
    params = [rc_cat, rc_1] + ([rc_2] if rc_2 else [])
    try:
        rows = conn.execute(f"""
            SELECT {field} as val, COUNT(*) as cnt
            FROM noc_tickets
            WHERE rc_category = ? AND rc_1 = ? {rc2_clause}
              AND {field} IS NOT NULL
            GROUP BY {field}
            ORDER BY cnt DESC
            LIMIT {limit}
        """, params).fetchall()
        total = sum(r[1] for r in rows)
        if total == 0:
            return None
        parts = []
        for r in rows:
            pct = round(r[1] * 100.0 / total, 1) if total > 0 else 0
            parts.append(f"{r[0]} ({pct}%)")
        return ", ".join(parts)
    except Exception:
        return None


def full_refresh():
    logger.info("NDC full refresh started")
    start_time = time.time()

    pre_count = 0
    pre_enrichment = {}
    try:
        with get_connection() as conn:
            pre_count = conn.execute("SELECT COUNT(*) FROM ndc_entries").fetchone()[0]
            try:
                snap_count = conn.execute("SELECT COUNT(*) FROM ndc_alarm_snapshot").fetchone()[0]
                sym_count = conn.execute("SELECT COUNT(*) FROM ndc_symptoms").fetchone()[0]
                diag_count = conn.execute("SELECT COUNT(*) FROM ndc_diagnostic_steps").fetchone()[0]
                pre_enrichment = {"snapshots": snap_count, "symptoms": sym_count, "diagnostics": diag_count}
            except Exception:
                pass
    except Exception:
        pass

    errors = []

    with get_connection() as conn:
        entries = _discover_entries(conn)
        logger.info(f"NDC discovery: {len(entries)} entries found")

    with get_write_connection() as wconn:
        for entry in entries:
            _upsert_entry(wconn, entry)

    with get_connection() as conn:
        all_entries = conn.execute("SELECT ndc_code, rc_category, rc_1, rc_2 FROM ndc_entries").fetchall()

    for row in all_entries:
        code, rc_cat, rc_1, rc_2 = row[0], row[1], row[2], row[3]
        try:
            _generate_alarm_snapshot(code, rc_cat, rc_1, rc_2)
        except Exception as e:
            errors.append(f"Alarm snapshot {code}: {e}")
            logger.warning(f"Alarm snapshot failed for {code}: {e}")
        try:
            _extract_symptoms(code, rc_cat, rc_1, rc_2)
        except Exception as e:
            errors.append(f"Symptoms {code}: {e}")
            logger.warning(f"Symptoms failed for {code}: {e}")
        try:
            _generate_diagnostic_skeleton(code, rc_cat, rc_1, rc_2)
        except Exception as e:
            errors.append(f"Diagnostic {code}: {e}")
            logger.warning(f"Diagnostic tree failed for {code}: {e}")
        try:
            _generate_resolution_paths(code, rc_cat, rc_1, rc_2)
        except Exception as e:
            errors.append(f"Resolution {code}: {e}")
            logger.warning(f"Resolution paths failed for {code}: {e}")
        try:
            _generate_escalation_matrix(code, rc_cat, rc_1, rc_2)
        except Exception as e:
            errors.append(f"Escalation {code}: {e}")
            logger.warning(f"Escalation matrix failed for {code}: {e}")

    try:
        _build_confusion_matrix()
    except Exception as e:
        errors.append(f"Confusion matrix: {e}")
        logger.warning(f"Confusion matrix failed: {e}")

    try:
        _refresh_site_distributions()
    except Exception as e:
        errors.append(f"Site distributions: {e}")
        logger.warning(f"Site distributions failed: {e}")

    duration_sec = round(time.time() - start_time, 1)
    post_count = len(all_entries)

    post_enrichment = {}
    try:
        with get_connection() as conn:
            snap_count = conn.execute("SELECT COUNT(*) FROM ndc_alarm_snapshot").fetchone()[0]
            sym_count = conn.execute("SELECT COUNT(*) FROM ndc_symptoms").fetchone()[0]
            diag_count = conn.execute("SELECT COUNT(*) FROM ndc_diagnostic_steps").fetchone()[0]
            post_enrichment = {"snapshots": snap_count, "symptoms": sym_count, "diagnostics": diag_count}
    except Exception:
        pass

    details = json.dumps({
        "entries_before": pre_count,
        "entries_after": post_count,
        "enrichment_before": pre_enrichment,
        "enrichment_after": post_enrichment,
        "duration_sec": duration_sec,
        "errors": errors[:20],
    })

    try:
        _log_changelog("refresh", "system", details, post_count)
    except Exception as e:
        logger.warning(f"Failed to log changelog: {e}")

    logger.info("NDC full refresh completed")
    return {"status": "success", "entries": post_count}


def _log_changelog(action, performed_by, details, entries_affected):
    with get_write_connection() as wconn:
        max_id = wconn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM ndc_changelog").fetchone()[0]
        wconn.execute("""
            INSERT INTO ndc_changelog (id, action, performed_by, timestamp, details, entries_affected)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, ?)
        """, [max_id, action, performed_by, details, entries_affected])


def get_ndc_changelog(limit=50):
    with get_connection() as conn:
        try:
            rows = conn.execute("""
                SELECT id, action, performed_by, timestamp, details, entries_affected
                FROM ndc_changelog
                ORDER BY timestamp DESC
                LIMIT ?
            """, [limit]).fetchall()
            return [{
                "id": r[0],
                "action": r[1],
                "performed_by": r[2],
                "timestamp": str(r[3]) if r[3] else None,
                "details": json.loads(r[4]) if r[4] else None,
                "entries_affected": r[5],
            } for r in rows]
        except Exception:
            return []


def get_last_refresh_info():
    with get_connection() as conn:
        try:
            row = conn.execute("""
                SELECT timestamp, details, entries_affected
                FROM ndc_changelog
                WHERE action = 'refresh'
                ORDER BY timestamp DESC
                LIMIT 1
            """).fetchone()
            if row:
                return {
                    "timestamp": str(row[0]) if row[0] else None,
                    "details": json.loads(row[1]) if row[1] else None,
                    "entries_affected": row[2],
                }
        except Exception:
            pass
    return None


def _discover_entries(conn):
    rows = conn.execute(f"""
        SELECT
            rc_category, rc_1, rc_2,
            COUNT(*) as total_tickets,
            ROUND(AVG(CASE WHEN calc_is_sla_met = FALSE THEN 1 ELSE 0 END) * 100, 2) as sla_breach_pct,
            ROUND(AVG(CASE WHEN is_auto_resolved = 'Auto Resolved' THEN 1 ELSE 0 END) * 100, 2) as auto_resolve_pct,
            ROUND(AVG(calc_restore_time_min), 1) as avg_mttr_min,
            ROUND(MEDIAN(calc_restore_time_min), 1) as median_mttr_min,
            ROUND(AVG(CASE WHEN severity = 'Critical' THEN 1 ELSE 0 END) * 100, 2) as pct_critical,
            ROUND(AVG(CASE WHEN severity = 'Major' THEN 1 ELSE 0 END) * 100, 2) as pct_major,
            ROUND(AVG(CASE WHEN severity = 'Minor' THEN 1 ELSE 0 END) * 100, 2) as pct_minor,
            ROUND(AVG(CASE WHEN severity = 'Low' THEN 1 ELSE 0 END) * 100, 2) as pct_low,
            ROUND(AVG(CASE WHEN is_escalate = 'true' THEN 1 ELSE 0 END) * 100, 2) as escalation_pct,
            MIN(occured_time) as first_seen,
            MAX(occured_time) as last_seen
        FROM noc_tickets
        WHERE rc_category IS NOT NULL AND rc_1 IS NOT NULL
        GROUP BY rc_category, rc_1, rc_2
        HAVING COUNT(*) >= {MIN_TICKETS}
        ORDER BY COUNT(*) DESC
    """).fetchall()

    cat_counters = {}
    existing_codes = {}
    try:
        for r in conn.execute("SELECT ndc_code, rc_category, rc_1, rc_2 FROM ndc_entries").fetchall():
            key = (r[1], r[2], r[3])
            existing_codes[key] = r[0]
            cat_info = CATEGORY_MAP.get(r[1], DEFAULT_CATEGORY)
            code_num = int(r[0].split('-')[-1])
            cat_counters[cat_info['code']] = max(cat_counters.get(cat_info['code'], 0), code_num)
    except Exception as e:
        logger.debug(f"Loading existing NDC codes: {e}")

    entries = []
    for r in rows:
        rc_cat, rc_1, rc_2 = r[0], r[1], r[2]
        key = (rc_cat, rc_1, rc_2)

        if key in existing_codes:
            ndc_code = existing_codes[key]
        else:
            cat_info = CATEGORY_MAP.get(rc_cat, DEFAULT_CATEGORY)
            cat_code = cat_info['code']
            cat_counters[cat_code] = cat_counters.get(cat_code, 0) + 1
            ndc_code = f"NDC-{cat_code}-{cat_counters[cat_code]:04d}"

        cat_info = CATEGORY_MAP.get(rc_cat, DEFAULT_CATEGORY)
        title = rc_1
        if rc_2:
            title = f"{rc_1} — {rc_2}"

        sla_breach = r[4] or 0
        pct_crit = r[8] or 0
        pct_maj = r[9] or 0
        avg_mttr = r[6] or 0
        total = r[3]

        vol_score = min(100, math.log10(max(total, 1)) / math.log10(50000) * 100)
        breach_score = min(100, sla_breach * 2)
        sev_score = min(100, pct_crit * 2 + pct_maj)
        mttr_score = min(100, (avg_mttr / 1440) * 100)
        priority_score = round(vol_score * 0.3 + breach_score * 0.3 + sev_score * 0.2 + mttr_score * 0.2, 1)

        if priority_score >= 60:
            calc_priority = "HIGH"
        elif priority_score >= 30:
            calc_priority = "MEDIUM"
        else:
            calc_priority = "LOW"

        sev_vals = [("Critical", pct_crit), ("Major", pct_maj), ("Minor", r[10] or 0), ("Low", r[11] or 0)]
        typical_sev = max(sev_vals, key=lambda x: x[1])[0] if any(v[1] > 0 for v in sev_vals) else None

        first_seen = r[13]
        last_seen = r[14]
        data_months = 1
        if first_seen and last_seen:
            try:
                fs = first_seen if isinstance(first_seen, datetime) else datetime.fromisoformat(str(first_seen)[:19])
                ls = last_seen if isinstance(last_seen, datetime) else datetime.fromisoformat(str(last_seen)[:19])
                data_months = max(1, (ls.year - fs.year) * 12 + (ls.month - fs.month) + 1)
            except Exception as e:
                logger.debug(f"Date range calc error: {e}")

        entries.append({
            'ndc_code': ndc_code,
            'category_code': cat_info['code'],
            'category_name': cat_info['name'],
            'rc_category': rc_cat,
            'rc_1': rc_1,
            'rc_2': rc_2,
            'title': title,
            'total_tickets': total,
            'sla_breach_pct': sla_breach,
            'auto_resolve_pct': r[5],
            'avg_mttr_min': avg_mttr,
            'median_mttr_min': r[7],
            'pct_critical': pct_crit,
            'pct_major': pct_maj,
            'pct_minor': r[10],
            'pct_low': r[11],
            'typical_severity': typical_sev,
            'escalation_pct': r[12],
            'calculated_priority': calc_priority,
            'priority_score': priority_score,
            'first_seen': str(first_seen)[:10] if first_seen else None,
            'last_seen': str(last_seen)[:10] if last_seen else None,
            'data_months': data_months,
        })

    return entries


def _upsert_entry(wconn, entry):
    code = _safe_q(entry['ndc_code'])
    now = datetime.now().isoformat()

    site_class_sql = """
        SELECT
            ROUND(AVG(CASE WHEN ms.site_class = 'Diamond' THEN 1 ELSE 0 END) * 100, 2),
            ROUND(AVG(CASE WHEN ms.site_class = 'Platinum' THEN 1 ELSE 0 END) * 100, 2),
            ROUND(AVG(CASE WHEN ms.site_class = 'Gold' THEN 1 ELSE 0 END) * 100, 2),
            ROUND(AVG(CASE WHEN ms.site_class = 'Silver' THEN 1 ELSE 0 END) * 100, 2),
            ROUND(AVG(CASE WHEN ms.site_class = 'Bronze' THEN 1 ELSE 0 END) * 100, 2),
            ROUND(AVG(CASE WHEN ms.site_flag = '3T' THEN 1 ELSE 0 END) * 100, 2)
        FROM noc_tickets t
        LEFT JOIN master_site ms ON t.site_id = ms.site_id
        WHERE t.rc_category = ? AND t.rc_1 = ?
    """
    rc2 = entry.get('rc_2')
    if rc2:
        site_class_sql += " AND t.rc_2 = ?"
        params = [entry['rc_category'], entry['rc_1'], rc2]
    else:
        site_class_sql += " AND t.rc_2 IS NULL"
        params = [entry['rc_category'], entry['rc_1']]

    with get_connection() as conn:
        try:
            sc = conn.execute(site_class_sql, params).fetchone()
        except Exception:
            sc = (0, 0, 0, 0, 0, 0)

    vals = {
        'ndc_code': entry['ndc_code'],
        'category_code': entry['category_code'],
        'category_name': entry['category_name'],
        'rc_category': entry['rc_category'],
        'rc_1': entry['rc_1'],
        'rc_2': entry.get('rc_2'),
        'title': entry['title'],
        'total_tickets': entry['total_tickets'],
        'sla_breach_pct': entry.get('sla_breach_pct'),
        'auto_resolve_pct': entry.get('auto_resolve_pct'),
        'avg_mttr_min': entry.get('avg_mttr_min'),
        'median_mttr_min': entry.get('median_mttr_min'),
        'pct_critical': entry.get('pct_critical'),
        'pct_major': entry.get('pct_major'),
        'pct_minor': entry.get('pct_minor'),
        'pct_low': entry.get('pct_low'),
        'typical_severity': entry.get('typical_severity'),
        'escalation_pct': entry.get('escalation_pct'),
        'calculated_priority': entry.get('calculated_priority'),
        'priority_score': entry.get('priority_score'),
        'pct_in_diamond': sc[0] if sc else 0,
        'pct_in_platinum': sc[1] if sc else 0,
        'pct_in_gold': sc[2] if sc else 0,
        'pct_in_silver': sc[3] if sc else 0,
        'pct_in_bronze': sc[4] if sc else 0,
        'pct_in_3t': sc[5] if sc else 0,
        'first_seen': entry.get('first_seen'),
        'last_seen': entry.get('last_seen'),
        'data_months': entry.get('data_months'),
        'updated_at': now,
    }

    try:
        existing = wconn.execute("SELECT status, notes, differentiator, reviewed_by, reviewed_at FROM ndc_entries WHERE ndc_code = ?", [entry['ndc_code']]).fetchone()
    except Exception:
        existing = None

    if existing:
        set_parts = []
        set_params = []
        for k, v in vals.items():
            if k == 'ndc_code':
                continue
            set_parts.append(f"{k} = ?")
            set_params.append(v)
        set_params.append(entry['ndc_code'])
        wconn.execute(f"UPDATE ndc_entries SET {', '.join(set_parts)} WHERE ndc_code = ?", set_params)
    else:
        vals['created_at'] = now
        vals['status'] = 'auto'
        cols = list(vals.keys())
        placeholders = ', '.join(['?' for _ in cols])
        wconn.execute(
            f"INSERT INTO ndc_entries ({', '.join(cols)}) VALUES ({placeholders})",
            [vals[c] for c in cols]
        )


def _generate_alarm_snapshot(ndc_code, rc_cat, rc_1, rc_2):
    with get_connection() as conn:
        typical_severity = _top_dist(conn, "severity", rc_cat, rc_1, rc_2)
        typical_ne_class = _top_dist(conn, "ne_class", rc_cat, rc_1, rc_2)
        typical_fault_level = _top_dist(conn, "fault_level", rc_cat, rc_1, rc_2, 2)
        typical_impact = _top_dist(conn, "impact", rc_cat, rc_1, rc_2, 2)
        typical_type_ticket = _top_dist(conn, "type_ticket", rc_cat, rc_1, rc_2, 2)
        typical_rat = _top_dist(conn, "rat", rc_cat, rc_1, rc_2)

        rc2_clause = "AND rc_2 = ?" if rc_2 else "AND rc_2 IS NULL"
        params = [rc_cat, rc_1] + ([rc_2] if rc_2 else [])

        peak_hours_range = None
        peak_days = None
        seasonal_pattern = None
        try:
            hourly = conn.execute(f"""
                SELECT calc_hour_of_day as h, COUNT(*) as cnt
                FROM noc_tickets
                WHERE rc_category = ? AND rc_1 = ? {rc2_clause}
                  AND calc_hour_of_day IS NOT NULL
                GROUP BY calc_hour_of_day
                ORDER BY calc_hour_of_day
            """, params).fetchall()
            if hourly:
                total_h = sum(r[1] for r in hourly)
                h_map = {r[0]: r[1] for r in hourly}
                best_start = 0
                best_pct = 0
                for h in range(24):
                    window_cnt = sum(h_map.get((h + i) % 24, 0) for i in range(6))
                    pct = round(window_cnt * 100.0 / total_h, 0) if total_h > 0 else 0
                    if pct > best_pct:
                        best_pct = pct
                        best_start = h
                peak_hours_range = f"{best_start:02d}:00 — {(best_start+5)%24:02d}:00 ({best_pct:.0f}% tiket)"
        except Exception as e:
            logger.debug(f"Peak hours calc failed for {ndc_code}: {e}")

        try:
            day_names = {0: 'Senin', 1: 'Selasa', 2: 'Rabu', 3: 'Kamis', 4: 'Jumat', 5: 'Sabtu', 6: 'Minggu'}
            daily = conn.execute(f"""
                SELECT calc_day_of_week, COUNT(*) as cnt
                FROM noc_tickets
                WHERE rc_category = ? AND rc_1 = ? {rc2_clause}
                  AND calc_day_of_week IS NOT NULL
                GROUP BY calc_day_of_week
                ORDER BY cnt DESC
                LIMIT 2
            """, params).fetchall()
            if daily:
                total_d = conn.execute(f"""
                    SELECT COUNT(*) FROM noc_tickets
                    WHERE rc_category = ? AND rc_1 = ? {rc2_clause}
                """, params).fetchone()[0]
                parts = []
                for r in daily:
                    pct = round(r[1] * 100.0 / total_d, 1) if total_d > 0 else 0
                    parts.append(f"{day_names.get(r[0], str(r[0]))} ({pct}%)")
                peak_days = ", ".join(parts)
        except Exception as e:
            logger.debug(f"Peak days calc failed for {ndc_code}: {e}")

        try:
            month_names = {1:'Jan',2:'Feb',3:'Mar',4:'Apr',5:'Mei',6:'Jun',7:'Jul',8:'Agu',9:'Sep',10:'Okt',11:'Nov',12:'Des'}
            monthly = conn.execute(f"""
                SELECT calc_month, COUNT(*) as cnt
                FROM noc_tickets
                WHERE rc_category = ? AND rc_1 = ? {rc2_clause}
                  AND calc_month IS NOT NULL
                GROUP BY calc_month
                ORDER BY calc_month
            """, params).fetchall()
            if monthly:
                avg_monthly = sum(r[1] for r in monthly) / 12.0
                high_months = [month_names.get(r[0], str(r[0])) for r in monthly if r[1] > avg_monthly * 1.2]
                if high_months:
                    seasonal_pattern = ", ".join(high_months)
        except Exception as e:
            logger.debug(f"Seasonal pattern calc failed for {ndc_code}: {e}")

        site_class_dist = None
        pct_3t = 0
        pct_kriteria = 0
        top_regions = None
        try:
            sc_rows = conn.execute(f"""
                SELECT ms.site_class, COUNT(*) as cnt
                FROM noc_tickets t
                LEFT JOIN master_site ms ON t.site_id = ms.site_id
                WHERE t.rc_category = ? AND t.rc_1 = ? {rc2_clause.replace('rc_2', 't.rc_2')}
                  AND ms.site_class IS NOT NULL
                GROUP BY ms.site_class
                ORDER BY cnt DESC
            """, params).fetchall()
            if sc_rows:
                total_sc = sum(r[1] for r in sc_rows)
                parts = [f"{r[0]} ({round(r[1]*100.0/total_sc,1)}%)" for r in sc_rows]
                site_class_dist = ", ".join(parts)
        except Exception as e:
            logger.debug(f"Site class dist failed for {ndc_code}: {e}")

        try:
            flag_rows = conn.execute(f"""
                SELECT
                    ROUND(AVG(CASE WHEN ms.site_flag = '3T' THEN 1 ELSE 0 END) * 100, 1)
                FROM noc_tickets t
                LEFT JOIN master_site ms ON t.site_id = ms.site_id
                WHERE t.rc_category = ? AND t.rc_1 = ? {rc2_clause.replace('rc_2', 't.rc_2')}
            """, params).fetchone()
            if flag_rows and flag_rows[0]:
                pct_3t = flag_rows[0]
        except Exception as e:
            logger.debug(f"3T pct calc failed for {ndc_code}: {e}")

        try:
            reg_rows = conn.execute(f"""
                SELECT mr.regional_name, COUNT(*) as cnt
                FROM noc_tickets t
                LEFT JOIN master_site ms ON t.site_id = ms.site_id
                LEFT JOIN master_to mt ON ms.to_id = mt.to_id
                LEFT JOIN master_nop mn ON mt.nop_id = mn.nop_id
                LEFT JOIN master_regional mr ON mn.regional_id = mr.regional_id
                WHERE t.rc_category = ? AND t.rc_1 = ? {rc2_clause.replace('rc_2', 't.rc_2')}
                  AND mr.regional_name IS NOT NULL
                GROUP BY mr.regional_name
                ORDER BY cnt DESC
                LIMIT 3
            """, params).fetchall()
            if reg_rows:
                total_reg = sum(r[1] for r in reg_rows)
                parts = [f"{r[0]} ({round(r[1]*100.0/total_reg,1)}%)" for r in reg_rows]
                top_regions = ", ".join(parts)
        except Exception as e:
            logger.debug(f"Top regions calc failed for {ndc_code}: {e}")

        sample_size = conn.execute(f"""
            SELECT COUNT(*) FROM noc_tickets
            WHERE rc_category = ? AND rc_1 = ? {rc2_clause}
        """, params).fetchone()[0]

    with get_write_connection() as wconn:
        wconn.execute("DELETE FROM ndc_alarm_snapshot WHERE ndc_code = ?", [ndc_code])
        wconn.execute("""
            INSERT INTO ndc_alarm_snapshot (
                ndc_code, typical_severity, typical_ne_class, typical_fault_level,
                typical_impact, typical_type_ticket, typical_rat,
                peak_hours_range, peak_days, seasonal_pattern,
                site_class_distribution, pct_3t, pct_kriteria, top_regions,
                sample_size, calculated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, [ndc_code, typical_severity, typical_ne_class, typical_fault_level,
              typical_impact, typical_type_ticket, typical_rat,
              peak_hours_range, peak_days, seasonal_pattern,
              site_class_dist, pct_3t, pct_kriteria, top_regions, sample_size])

        wconn.execute("DELETE FROM ndc_co_occurring_alarms WHERE ndc_code = ?", [ndc_code])
        rc2_clause2 = "AND t1.rc_2 = ?" if rc_2 else "AND t1.rc_2 IS NULL"
        co_params = [rc_cat, rc_1] + ([rc_2] if rc_2 else []) + [rc_1]
        try:
            co_rows = wconn.execute(f"""
                WITH ndc_base AS (
                    SELECT ticket_number_inap, site_id, occured_time
                    FROM noc_tickets t1
                    WHERE t1.rc_category = ? AND t1.rc_1 = ? {rc2_clause2}
                    LIMIT 50000
                ),
                co_tickets AS (
                    SELECT
                        t2.rc_category as co_rc_cat,
                        t2.rc_1 as co_rc_1,
                        (EXTRACT(EPOCH FROM t2.occured_time - t1.occured_time) / 60.0) as lag_min
                    FROM ndc_base t1
                    JOIN noc_tickets t2
                        ON t1.site_id = t2.site_id
                        AND t2.ticket_number_inap != t1.ticket_number_inap
                        AND ABS(EXTRACT(EPOCH FROM t2.occured_time - t1.occured_time)) < 14400
                    WHERE t2.rc_1 != ?
                )
                SELECT
                    co_rc_1, co_rc_cat,
                    ROUND(COUNT(*) * 100.0 / GREATEST((SELECT COUNT(*) FROM ndc_base), 1), 1) as co_pct,
                    ROUND(AVG(lag_min), 0) as avg_lag,
                    CASE WHEN AVG(lag_min) > 5 THEN 'after'
                         WHEN AVG(lag_min) < -5 THEN 'before'
                         ELSE 'concurrent' END as direction,
                    COUNT(*) as sample
                FROM co_tickets
                GROUP BY co_rc_cat, co_rc_1
                HAVING COUNT(*) >= 10
                ORDER BY COUNT(*) DESC
                LIMIT 5
            """, co_params).fetchall()

            for i, cr in enumerate(co_rows):
                lag_desc = None
                if cr[3]:
                    abs_lag = abs(cr[3])
                    if abs_lag < 60:
                        lag_desc = f"~{abs_lag:.0f} menit {'setelah' if cr[4]=='after' else 'sebelum'}"
                    else:
                        lag_desc = f"~{abs_lag/60:.1f} jam {'setelah' if cr[4]=='after' else 'sebelum'}"

                wconn.execute("""
                    INSERT INTO ndc_co_occurring_alarms (
                        id, ndc_code, co_alarm_description, co_alarm_rc_category,
                        co_alarm_rc_1, co_occurrence_pct, typical_lag_description,
                        typical_lag_min, lag_direction, sample_size, calculated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, [
                    hash(f"{ndc_code}_{i}") % 2147483647,
                    ndc_code, cr[0], cr[1], cr[0], cr[2], lag_desc, cr[3], cr[4], cr[5]
                ])
        except Exception as e:
            logger.warning(f"Co-occurring alarms failed for {ndc_code}: {e}", exc_info=True)


def _extract_symptoms(ndc_code, rc_cat, rc_1, rc_2):
    with get_connection() as conn:
        rc2_clause = "AND rc_2 = ?" if rc_2 else "AND rc_2 IS NULL"
        params = [rc_cat, rc_1] + ([rc_2] if rc_2 else [])

        rows = conn.execute(f"""
            SELECT summary, description
            FROM noc_tickets
            WHERE rc_category = ? AND rc_1 = ? {rc2_clause}
              AND (summary IS NOT NULL OR description IS NOT NULL)
            LIMIT 10000
        """, params).fetchall()

    total = len(rows)
    if total == 0:
        return

    symptoms = []
    for symptom_name, config in SYMPTOM_PATTERNS.items():
        match_count = 0
        for row in rows:
            combined = f"{row[0] or ''} {row[1] or ''}".lower()
            if any(re.search(p, combined) for p in config["patterns"]):
                match_count += 1

        freq_pct = round(match_count / total * 100, 1)
        if freq_pct >= 5:
            symptoms.append({
                'symptom_text': symptom_name,
                'symptom_type': 'primary' if freq_pct >= 30 else 'secondary',
                'frequency_pct': freq_pct,
                'confidence': 'high' if freq_pct >= 50 else ('medium' if freq_pct >= 20 else 'low'),
                'source': config["source"],
            })

    symptoms.sort(key=lambda x: -x['frequency_pct'])

    with get_write_connection() as wconn:
        wconn.execute("DELETE FROM ndc_symptoms WHERE ndc_code = ? AND is_auto_generated = TRUE", [ndc_code])
        for i, s in enumerate(symptoms):
            wconn.execute("""
                INSERT INTO ndc_symptoms (
                    id, ndc_code, symptom_text, symptom_type, frequency_pct,
                    confidence, source, sort_order, is_auto_generated, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, TRUE, CURRENT_TIMESTAMP)
            """, [
                hash(f"{ndc_code}_sym_{i}") % 2147483647,
                ndc_code, s['symptom_text'], s['symptom_type'],
                s['frequency_pct'], s['confidence'], s['source'], i
            ])


def _generate_diagnostic_skeleton(ndc_code, rc_cat, rc_1, rc_2):
    with get_connection() as conn:
        has_reviewed = conn.execute(
            "SELECT COUNT(*) FROM ndc_diagnostic_steps WHERE ndc_code = ? AND reviewed = TRUE",
            [ndc_code]
        ).fetchone()[0]
        if has_reviewed > 0:
            return

        rc2_clause = "AND rc_2 = ?" if rc_2 else "AND rc_2 IS NULL"
        params = [rc_cat, rc_1] + ([rc_2] if rc_2 else [])

        q = conn.execute(f"""
            SELECT
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY calc_restore_time_min) as q1,
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY calc_restore_time_min) as q2,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY calc_restore_time_min) as q3,
                PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY calc_restore_time_min) as q4,
                COUNT(*) as total
            FROM noc_tickets
            WHERE rc_category = ? AND rc_1 = ? {rc2_clause}
              AND calc_restore_time_min IS NOT NULL AND calc_restore_time_min > 0
        """, params).fetchone()

        if not q or q[4] < 5:
            return

        total = q[4]
        quartiles = [q[0], q[1], q[2], q[3]]

        stage_params = list(quartiles) + [total] + params
        stages = conn.execute(f"""
            SELECT
                CASE
                    WHEN calc_restore_time_min <= ? THEN 'stage_1'
                    WHEN calc_restore_time_min <= ? THEN 'stage_2'
                    WHEN calc_restore_time_min <= ? THEN 'stage_3'
                    WHEN calc_restore_time_min <= ? THEN 'stage_4'
                    ELSE 'stage_5'
                END as stage,
                COUNT(*) as cnt,
                ROUND(COUNT(*) * 100.0 / ?, 1) as pct,
                ROUND(AVG(calc_restore_time_min), 0) as avg_duration
            FROM noc_tickets
            WHERE rc_category = ? AND rc_1 = ? {rc2_clause}
              AND calc_restore_time_min IS NOT NULL AND calc_restore_time_min > 0
            GROUP BY stage
            ORDER BY stage
        """, stage_params).fetchall()

    skeleton = [{
        'step_number': 1,
        'action': f"Identifikasi dan konfirmasi gangguan: {rc_1}" + (f" — {rc_2}" if rc_2 else ""),
        'expected_result': f"Konfirmasi bahwa root cause adalah {rc_1}",
        'if_yes': "Lanjut ke Step 2",
        'if_no': "Bukan gangguan ini — evaluasi ulang klasifikasi",
        'avg_duration_min': 5,
        'success_rate_at_step': None,
        'cumulative_resolve_pct': 0,
    }]

    cumulative = 0
    for i, stage in enumerate(stages):
        cumulative += stage[2]
        skeleton.append({
            'step_number': i + 2,
            'action': f"Resolusi stage {i+1} (MTTR ~{stage[3]:.0f}m)" if stage[3] else f"Resolusi stage {i+1}",
            'expected_result': "Gangguan teratasi",
            'if_yes': "Verifikasi dan close tiket" if i == len(stages) - 1 else f"Lanjut ke Step {i+3}",
            'if_no': "Eskalasi ke level berikutnya" if i < len(stages) - 1 else "Eskalasi ke management",
            'avg_duration_min': int(stage[3]) if stage[3] else 30,
            'success_rate_at_step': stage[2],
            'cumulative_resolve_pct': round(cumulative, 1),
        })

    skeleton.append({
        'step_number': len(skeleton) + 1,
        'action': "Verifikasi: semua NE up, alarm clear, layanan normal",
        'expected_result': "Site fully operational",
        'if_yes': "Close tiket. Update RCA.",
        'if_no': "Ada alarm tersisa — buat tiket baru per alarm type",
        'avg_duration_min': 15,
        'success_rate_at_step': round(max(0, 100 - cumulative), 1),
        'cumulative_resolve_pct': 100.0,
    })

    with get_write_connection() as wconn:
        wconn.execute("DELETE FROM ndc_diagnostic_steps WHERE ndc_code = ? AND reviewed = FALSE", [ndc_code])
        for s in skeleton:
            wconn.execute("""
                INSERT INTO ndc_diagnostic_steps (
                    id, ndc_code, step_number, action, expected_result,
                    if_yes, if_no, avg_duration_min, success_rate_at_step,
                    cumulative_resolve_pct, is_auto_generated, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE, CURRENT_TIMESTAMP)
            """, [
                hash(f"{ndc_code}_diag_{s['step_number']}") % 2147483647,
                ndc_code, s['step_number'], s['action'], s['expected_result'],
                s['if_yes'], s['if_no'], s['avg_duration_min'],
                s['success_rate_at_step'], s['cumulative_resolve_pct']
            ])


def _generate_resolution_paths(ndc_code, rc_cat, rc_1, rc_2):
    with get_connection() as conn:
        rc2_clause = "AND rc_2 = ?" if rc_2 else "AND rc_2 IS NULL"
        params = [rc_cat, rc_1] + ([rc_2] if rc_2 else [])

        total = conn.execute(f"""
            SELECT COUNT(*) FROM noc_tickets
            WHERE rc_category = ? AND rc_1 = ? {rc2_clause}
              AND calc_restore_time_min IS NOT NULL
        """, params).fetchone()[0]

        if total < 5:
            return

        med = conn.execute(f"""
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY calc_restore_time_min)
            FROM noc_tickets
            WHERE rc_category = ? AND rc_1 = ? {rc2_clause}
              AND calc_restore_time_min IS NOT NULL
        """, params).fetchone()[0]

        path_params = [med] + params
        paths = conn.execute(f"""
            SELECT
                CASE
                    WHEN is_auto_resolved = 'Auto Resolved' THEN 'Auto-Resolve'
                    WHEN is_escalate = 'true' THEN 'Eskalasi'
                    WHEN calc_restore_time_min <= ? THEN 'Standard Resolution'
                    ELSE 'Extended Resolution'
                END as path_cluster,
                COUNT(*) as cnt,
                ROUND(COUNT(*) * 100.0 / {total}, 1) as probability_pct,
                ROUND(AVG(calc_restore_time_min), 0) as avg_mttr_min,
                ROUND(AVG(CASE WHEN calc_is_sla_met THEN 1 ELSE 0 END) * 100, 1) as sla_met_pct
            FROM noc_tickets
            WHERE rc_category = ? AND rc_1 = ? {rc2_clause}
              AND calc_restore_time_min IS NOT NULL
            GROUP BY path_cluster
            HAVING COUNT(*) >= 5
            ORDER BY cnt DESC
        """, path_params).fetchall()

        res_actions = {}
        for path_row in paths:
            cluster = path_row[0]
            try:
                action_rows = conn.execute(f"""
                    SELECT resolution_action, COUNT(*) as cnt
                    FROM noc_tickets
                    WHERE rc_category = ? AND rc_1 = ? {rc2_clause}
                      AND resolution_action IS NOT NULL
                      AND CASE
                            WHEN is_auto_resolved = 'Auto Resolved' THEN 'Auto-Resolve'
                            WHEN is_escalate = 'true' THEN 'Eskalasi'
                            WHEN calc_restore_time_min <= ? THEN 'Standard Resolution'
                            ELSE 'Extended Resolution'
                          END = ?
                    GROUP BY resolution_action
                    ORDER BY cnt DESC
                    LIMIT 3
                """, params + [med, cluster]).fetchall()
                res_actions[cluster] = [r[0] for r in action_rows if r[0]]
            except Exception as e:
                logger.debug(f"Resolution actions query failed for {ndc_code} cluster {cluster}: {e}")
                res_actions[cluster] = []

    with get_write_connection() as wconn:
        old_path_ids = [r[0] for r in wconn.execute(
            "SELECT id FROM ndc_resolution_paths WHERE ndc_code = ? AND is_auto_generated = TRUE",
            [ndc_code]
        ).fetchall()]
        for pid in old_path_ids:
            wconn.execute("DELETE FROM ndc_resolution_steps WHERE path_id = ?", [pid])
        wconn.execute("DELETE FROM ndc_resolution_paths WHERE ndc_code = ? AND is_auto_generated = TRUE", [ndc_code])

        for i, p in enumerate(paths):
            path_id = hash(f"{ndc_code}_path_{i}") % 2147483647
            wconn.execute("""
                INSERT INTO ndc_resolution_paths (
                    id, ndc_code, path_name, sort_order, probability_pct,
                    avg_mttr_min, sla_met_pct, ticket_count, is_auto_generated, calculated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, TRUE, CURRENT_TIMESTAMP)
            """, [path_id, ndc_code, p[0], i, p[2], p[3], p[4], p[1]])

            actions = res_actions.get(p[0], [])
            for si, action_text in enumerate(actions):
                wconn.execute("""
                    INSERT INTO ndc_resolution_steps (
                        id, path_id, step_number, step_text, created_at
                    ) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, [hash(f"{path_id}_step_{si}") % 2147483647, path_id, si + 1, action_text])


def _generate_escalation_matrix(ndc_code, rc_cat, rc_1, rc_2):
    DEFAULT_MATRIX = [
        {'tier': 1, 'role': 'NOC Operator', 'action': 'Monitoring awal + identifikasi gangguan', 'max_duration': '1 jam'},
        {'tier': 2, 'role': 'TL NOC / Dispatch', 'action': 'Dispatch engineer + koordinasi lapangan', 'max_duration': '3 jam'},
        {'tier': 3, 'role': 'NOP Manager', 'action': 'Resource reallocation + eskalasi vendor', 'max_duration': '6 jam'},
        {'tier': 4, 'role': 'Regional Manager', 'action': 'Strategic decision + management eskalasi', 'max_duration': '24 jam'},
    ]

    with get_write_connection() as wconn:
        existing = wconn.execute(
            "SELECT COUNT(*) FROM ndc_escalation_matrix WHERE ndc_code = ?", [ndc_code]
        ).fetchone()[0]
        if existing > 0:
            return

        for entry in DEFAULT_MATRIX:
            wconn.execute("""
                INSERT INTO ndc_escalation_matrix (
                    id, ndc_code, tier, role, action, max_duration, sort_order
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                hash(f"{ndc_code}_esc_{entry['tier']}") % 2147483647,
                ndc_code, entry['tier'], entry['role'],
                entry['action'], entry['max_duration'], entry['tier']
            ])


def _build_confusion_matrix():
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT
                rc_category as inap_rc_cat,
                rc_1 as inap_rc_1,
                rc_category_engineer as confirmed_rc_cat,
                rc_1_engineer as confirmed_rc_1,
                COUNT(*) as ticket_count
            FROM noc_tickets
            WHERE rc_category IS NOT NULL
              AND rc_category_engineer IS NOT NULL
              AND rc_1 IS NOT NULL
              AND rc_1_engineer IS NOT NULL
            GROUP BY rc_category, rc_1, rc_category_engineer, rc_1_engineer
            HAVING COUNT(*) >= 5
            ORDER BY ticket_count DESC
        """).fetchall()

        period = conn.execute("""
            SELECT MIN(occured_time), MAX(occured_time) FROM noc_tickets
            WHERE rc_category IS NOT NULL
        """).fetchone()

    if not rows:
        return

    with get_write_connection() as wconn:
        wconn.execute("DELETE FROM ndc_confusion_matrix")

        cat_totals = {}
        for r in rows:
            key = (r[0], r[1])
            cat_totals[key] = cat_totals.get(key, 0) + r[4]

        for i, r in enumerate(rows):
            total_for_inap = cat_totals.get((r[0], r[1]), 1)
            match_pct = round(r[4] * 100.0 / total_for_inap, 1)
            wconn.execute("""
                INSERT INTO ndc_confusion_matrix (
                    id, inap_rc_category, inap_rc_1, confirmed_rc_category,
                    confirmed_rc_1, ticket_count, match_pct,
                    period_start, period_end, calculated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, [
                i + 1, r[0], r[1], r[2], r[3], r[4], match_pct,
                str(period[0])[:10] if period[0] else None,
                str(period[1])[:10] if period[1] else None,
            ])

        for row in wconn.execute("SELECT ndc_code, rc_category, rc_1 FROM ndc_entries").fetchall():
            try:
                match_row = wconn.execute("""
                    SELECT match_pct FROM ndc_confusion_matrix
                    WHERE inap_rc_category = ? AND inap_rc_1 = ?
                      AND confirmed_rc_category = ? AND confirmed_rc_1 = ?
                """, [row[1], row[2], row[1], row[2]]).fetchone()
                if match_row:
                    wconn.execute("UPDATE ndc_entries SET inap_match_pct = ? WHERE ndc_code = ?",
                                  [match_row[0], row[0]])

                misclass = wconn.execute("""
                    SELECT confirmed_rc_category || ': ' || confirmed_rc_1
                    FROM ndc_confusion_matrix
                    WHERE inap_rc_category = ? AND inap_rc_1 = ?
                      AND (confirmed_rc_category != ? OR confirmed_rc_1 != ?)
                    ORDER BY ticket_count DESC
                    LIMIT 1
                """, [row[1], row[2], row[1], row[2]]).fetchone()
                if misclass:
                    wconn.execute("UPDATE ndc_entries SET common_inap_misclass = ? WHERE ndc_code = ?",
                                  [misclass[0], row[0]])
            except Exception as e:
                logger.debug(f"Misclass update failed for {row[0]}: {e}")


def _refresh_site_distributions():
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT
                t.site_id, n.ndc_code, t.calc_year_month as period,
                COUNT(*) as ticket_count,
                ROUND(AVG(t.calc_restore_time_min), 1) as avg_mttr
            FROM noc_tickets t
            JOIN ndc_entries n ON t.rc_category = n.rc_category AND t.rc_1 = n.rc_1
                AND (t.rc_2 = n.rc_2 OR (t.rc_2 IS NULL AND n.rc_2 IS NULL))
            WHERE t.site_id IS NOT NULL AND t.calc_year_month IS NOT NULL
            GROUP BY t.site_id, n.ndc_code, t.calc_year_month
        """).fetchall()

    if not rows:
        return

    site_period_totals = {}
    for r in rows:
        key = (r[0], r[2])
        site_period_totals[key] = site_period_totals.get(key, 0) + r[3]

    with get_write_connection() as wconn:
        wconn.execute("DELETE FROM ndc_site_distribution")

        batch = []
        for r in rows:
            total = site_period_totals.get((r[0], r[2]), 1)
            pct = round(r[3] * 100.0 / total, 1)
            batch.append((r[0], r[1], r[2], r[3], pct, r[4]))

        for i in range(0, len(batch), 500):
            chunk = batch[i:i+500]
            for row in chunk:
                wconn.execute("""
                    INSERT OR REPLACE INTO ndc_site_distribution
                    (site_id, ndc_code, period, ticket_count, pct_of_site_total, avg_mttr_min)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, list(row))


def get_ndc_list(category=None, priority=None, status=None, search=None, sort_by="total_tickets", sort_dir="desc", limit=100, offset=0):
    with get_connection() as conn:
        where = ["1=1"]
        params = []

        if category:
            where.append("category_code = ?")
            params.append(category)
        if priority:
            where.append("calculated_priority = ?")
            params.append(priority)
        if status:
            where.append("status = ?")
            params.append(status)
        if search:
            where.append("(title ILIKE ? OR ndc_code ILIKE ? OR rc_1 ILIKE ?)")
            s = f"%{search}%"
            params.extend([s, s, s])

        where_str = " AND ".join(where)
        allowed_sort = {"total_tickets", "priority_score", "sla_breach_pct", "avg_mttr_min", "ndc_code", "title"}
        if sort_by not in allowed_sort:
            sort_by = "total_tickets"
        sort_dir = "DESC" if sort_dir.upper() == "DESC" else "ASC"

        total_count = conn.execute(f"SELECT COUNT(*) FROM ndc_entries WHERE {where_str}", params).fetchone()[0]

        rows = conn.execute(f"""
            SELECT * FROM ndc_entries
            WHERE {where_str}
            ORDER BY {sort_by} {sort_dir}
            LIMIT ? OFFSET ?
        """, params + [limit, offset]).fetchall()

        cols = [d[0] for d in conn.execute("SELECT * FROM ndc_entries LIMIT 0").description]

        total_all = conn.execute("SELECT SUM(total_tickets) FROM ndc_entries").fetchone()[0] or 0
        total_noc = conn.execute("SELECT COUNT(*) FROM noc_tickets WHERE rc_category IS NOT NULL AND rc_1 IS NOT NULL").fetchone()[0] or 1

        entries = [dict(zip(cols, r)) for r in rows]

        categories = conn.execute("""
            SELECT category_code, COUNT(*) FROM ndc_entries GROUP BY category_code ORDER BY COUNT(*) DESC
        """).fetchall()

        return {
            "entries": entries,
            "total": total_count,
            "coverage_pct": round(total_all * 100.0 / total_noc, 1) if total_noc > 0 else 0,
            "categories": [{"code": c[0], "count": c[1]} for c in categories],
        }


def get_ndc_detail(ndc_code):
    with get_connection() as conn:
        cols = [d[0] for d in conn.execute("SELECT * FROM ndc_entries LIMIT 0").description]
        row = conn.execute("SELECT * FROM ndc_entries WHERE ndc_code = ?", [ndc_code]).fetchone()
        if not row:
            return None

        entry = dict(zip(cols, row))

        snap_cols = [d[0] for d in conn.execute("SELECT * FROM ndc_alarm_snapshot LIMIT 0").description]
        snap = conn.execute("SELECT * FROM ndc_alarm_snapshot WHERE ndc_code = ?", [ndc_code]).fetchone()
        alarm_snapshot = dict(zip(snap_cols, snap)) if snap else None

        co_cols = [d[0] for d in conn.execute("SELECT * FROM ndc_co_occurring_alarms LIMIT 0").description]
        co_rows = conn.execute("SELECT * FROM ndc_co_occurring_alarms WHERE ndc_code = ? ORDER BY co_occurrence_pct DESC", [ndc_code]).fetchall()
        co_alarms = [dict(zip(co_cols, r)) for r in co_rows]

        sym_cols = [d[0] for d in conn.execute("SELECT * FROM ndc_symptoms LIMIT 0").description]
        sym_rows = conn.execute("SELECT * FROM ndc_symptoms WHERE ndc_code = ? ORDER BY symptom_type, sort_order", [ndc_code]).fetchall()
        symptoms = [dict(zip(sym_cols, r)) for r in sym_rows]

        diag_cols = [d[0] for d in conn.execute("SELECT * FROM ndc_diagnostic_steps LIMIT 0").description]
        diag_rows = conn.execute("SELECT * FROM ndc_diagnostic_steps WHERE ndc_code = ? ORDER BY step_number", [ndc_code]).fetchall()
        diagnostic_steps = [dict(zip(diag_cols, r)) for r in diag_rows]

        path_cols = [d[0] for d in conn.execute("SELECT * FROM ndc_resolution_paths LIMIT 0").description]
        path_rows = conn.execute("SELECT * FROM ndc_resolution_paths WHERE ndc_code = ? ORDER BY sort_order", [ndc_code]).fetchall()
        paths = []
        for pr in path_rows:
            p = dict(zip(path_cols, pr))
            step_cols = [d[0] for d in conn.execute("SELECT * FROM ndc_resolution_steps LIMIT 0").description]
            steps = conn.execute("SELECT * FROM ndc_resolution_steps WHERE path_id = ? ORDER BY step_number", [p['id']]).fetchall()
            p['steps'] = [dict(zip(step_cols, s)) for s in steps]
            paths.append(p)

        esc_cols = [d[0] for d in conn.execute("SELECT * FROM ndc_escalation_matrix LIMIT 0").description]
        esc_rows = conn.execute("SELECT * FROM ndc_escalation_matrix WHERE ndc_code = ? ORDER BY tier", [ndc_code]).fetchall()
        escalation = [dict(zip(esc_cols, r)) for r in esc_rows]

        prev_cols = [d[0] for d in conn.execute("SELECT * FROM ndc_preventive_actions LIMIT 0").description]
        prev_rows = conn.execute("SELECT * FROM ndc_preventive_actions WHERE ndc_code = ? ORDER BY sort_order", [ndc_code]).fetchall()
        preventive = [dict(zip(prev_cols, r)) for r in prev_rows]

        return {
            **entry,
            "alarm_snapshot": alarm_snapshot,
            "co_occurring_alarms": co_alarms,
            "symptoms": symptoms,
            "diagnostic_steps": diagnostic_steps,
            "resolution_paths": paths,
            "escalation_matrix": escalation,
            "preventive_actions": preventive,
        }


def get_confusion_matrix():
    with get_connection() as conn:
        cols = [d[0] for d in conn.execute("SELECT * FROM ndc_confusion_matrix LIMIT 0").description]
        rows = conn.execute("""
            SELECT * FROM ndc_confusion_matrix
            ORDER BY ticket_count DESC
        """).fetchall()

        categories = conn.execute("""
            SELECT DISTINCT inap_rc_category FROM ndc_confusion_matrix
            UNION
            SELECT DISTINCT confirmed_rc_category FROM ndc_confusion_matrix
        """).fetchall()

        return {
            "data": [dict(zip(cols, r)) for r in rows],
            "categories": sorted(set(c[0] for c in categories if c[0])),
        }


def get_ndc_for_entity(entity_level, entity_id, limit=10):
    level_col_map = {
        "site": "t.site_id",
        "to": "t.calc_to_id",
        "nop": "t.calc_nop_id",
        "regional": "t.calc_regional_id",
        "area": "t.calc_area_id",
    }
    col = level_col_map.get(entity_level)
    if not col:
        return []

    with get_connection() as conn:
        total = conn.execute(f"""
            SELECT COUNT(*) FROM noc_tickets t
            WHERE {col} = ? AND t.rc_category IS NOT NULL AND t.rc_1 IS NOT NULL
        """, [entity_id]).fetchone()[0]

        if total == 0:
            return []

        rows = conn.execute(f"""
            SELECT
                n.ndc_code, n.title, n.category_code, n.calculated_priority,
                COUNT(*) as ticket_count,
                ROUND(COUNT(*) * 100.0 / {total}, 1) as pct
            FROM noc_tickets t
            JOIN ndc_entries n ON t.rc_category = n.rc_category AND t.rc_1 = n.rc_1
                AND (t.rc_2 = n.rc_2 OR (t.rc_2 IS NULL AND n.rc_2 IS NULL))
            WHERE {col} = ?
            GROUP BY n.ndc_code, n.title, n.category_code, n.calculated_priority
            ORDER BY ticket_count DESC
            LIMIT ?
        """, [entity_id, limit]).fetchall()

        return [{
            "ndc_code": r[0],
            "title": r[1],
            "category_code": r[2],
            "priority": r[3],
            "ticket_count": r[4],
            "pct": r[5],
        } for r in rows]


def get_ndc_for_site(site_id):
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT ndc_code, period, ticket_count, pct_of_site_total, avg_mttr_min
            FROM ndc_site_distribution
            WHERE site_id = ?
            ORDER BY period DESC, ticket_count DESC
        """, [site_id]).fetchall()

        return [{
            "ndc_code": r[0],
            "period": r[1],
            "ticket_count": r[2],
            "pct_of_site_total": r[3],
            "avg_mttr_min": r[4],
        } for r in rows]


def update_ndc_curation(ndc_code, status=None, notes=None, differentiator=None, reviewed_by=None):
    with get_write_connection() as wconn:
        updates = []
        params = []
        if status:
            updates.append("status = ?")
            params.append(status)
        if notes is not None:
            updates.append("notes = ?")
            params.append(notes)
        if differentiator is not None:
            updates.append("differentiator = ?")
            params.append(differentiator)
        if reviewed_by:
            updates.append("reviewed_by = ?")
            params.append(reviewed_by)
            updates.append("reviewed_at = CURRENT_TIMESTAMP")

        updates.append("updated_at = CURRENT_TIMESTAMP")
        params.append(ndc_code)

        wconn.execute(f"UPDATE ndc_entries SET {', '.join(updates)} WHERE ndc_code = ?", params)
    return {"status": "updated"}
