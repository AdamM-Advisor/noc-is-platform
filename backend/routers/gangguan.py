from fastapi import APIRouter, Query
from typing import Optional
from backend.database import get_connection
from statistics import mean as stat_mean, stdev as stat_stdev

router = APIRouter(prefix="/profiler/gangguan", tags=["profiler-gangguan"])

LEVEL_COL_MAP = {
    "area": ("calc_area_id", "area_id"),
    "regional": ("calc_regional_id", "regional_id"),
    "nop": ("calc_nop_id", "nop_id"),
    "to": ("calc_to_id", "to_id"),
    "site": ("site_id", "site_id"),
}

CHILD_LEVEL_MAP = {
    "area": "regional",
    "regional": "nop",
    "nop": "to",
    "to": "site",
}

LEVEL_NAME_TABLE = {
    "area": ("master_area", "area_id", "area_name"),
    "regional": ("master_regional", "regional_id", "regional_name"),
    "nop": ("master_nop", "nop_id", "nop_name"),
    "to": ("master_to", "to_id", "to_name"),
    "site": ("master_site", "site_id", "site_name"),
}

TYPE_LABELS = {
    "area": "Area", "regional": "Regional",
    "nop": "NOP", "to": "TO", "site": "Site",
}


def _get_entity_name(conn, level, entity_id):
    info = LEVEL_NAME_TABLE.get(level)
    if not info:
        return entity_id
    tbl, col, name_col = info
    try:
        row = conn.execute(f"SELECT {name_col} FROM {tbl} WHERE {col} = ?", [entity_id]).fetchone()
        return row[0] if row else entity_id
    except:
        return entity_id


def _build_ticket_where(entity_level, entity_id, date_from, date_to, type_ticket, severities_str):
    entity_col = LEVEL_COL_MAP[entity_level][0]
    where = [f"{entity_col} = ?"]
    params = [entity_id]
    if date_from:
        where.append("calc_year_month >= ?")
        params.append(date_from)
    if date_to:
        where.append("calc_year_month <= ?")
        params.append(date_to)
    if type_ticket:
        where.append("type_ticket = ?")
        params.append(type_ticket)
    if severities_str:
        sev_list = [s.strip() for s in severities_str.split(",") if s.strip()]
        if sev_list:
            placeholders = ",".join(["?" for _ in sev_list])
            where.append(f"severity IN ({placeholders})")
            params.extend(sev_list)
    return " AND ".join(where), params


def _build_sm_where(entity_level, entity_id, date_from, date_to, type_ticket, severities_str):
    entity_col = LEVEL_COL_MAP[entity_level][1]
    where = [f"{entity_col} = ?"]
    params = [entity_id]
    if date_from:
        where.append("year_month >= ?")
        params.append(date_from)
    if date_to:
        where.append("year_month <= ?")
        params.append(date_to)
    if type_ticket:
        where.append("type_ticket = ?")
        params.append(type_ticket)
    if severities_str:
        sev_list = [s.strip() for s in severities_str.split(",") if s.strip()]
        if sev_list:
            placeholders = ",".join(["?" for _ in sev_list])
            where.append(f"severity IN ({placeholders})")
            params.extend(sev_list)
    return " AND ".join(where), params


def _interpret_severity_mix(counts):
    total = sum(counts.values())
    if total == 0:
        return "Tidak ada data tiket."

    sorted_sev = sorted(counts.items(), key=lambda x: -x[1])
    dominant = sorted_sev[0][0]
    dominant_pct = sorted_sev[0][1] / total * 100

    crit_pct = counts.get("Critical", 0) / total * 100
    maj_pct = counts.get("Major", 0) / total * 100
    high_pct = crit_pct + maj_pct

    parts = [f"Komposisi: {dominant} dominan ({dominant_pct:.0f}%)."]

    if crit_pct > 10:
        parts.append(
            f"⚠️ Proporsi Critical {crit_pct:.0f}% di atas 10%. "
            f"Perlu investigasi apakah ada masalah sistemik."
        )

    if high_pct > 30:
        parts.append(
            f"🔴 Lebih dari 30% tiket berseverity tinggi (Critical+Major = {high_pct:.0f}%). "
            f"Kualitas jaringan perlu evaluasi menyeluruh."
        )
    elif high_pct <= 20:
        parts.append(f"Critical+Major = {high_pct:.0f}% — dalam batas normal.")
    else:
        parts.append(f"Critical+Major = {high_pct:.0f}% — perlu dipantau.")

    return " ".join(parts)


def _interpret_fault_pareto(items):
    if not items:
        return "Tidak ada data fault level."
    total = sum(i["count"] for i in items)
    if total == 0:
        return "Tidak ada data fault level."

    cumulative = 0
    n_for_80 = 0
    top_names = []
    for it in items:
        cumulative += it["count"]
        n_for_80 += 1
        top_names.append(it["name"])
        if cumulative / total >= 0.80:
            break

    top_pct = cumulative / total * 100
    parts = [
        f"Top {n_for_80} gangguan ({', '.join(top_names)}) menyumbang "
        f"{top_pct:.0f}% total tiket."
    ]

    top1_pct = items[0]["count"] / total * 100
    if top1_pct > 40:
        parts.append(
            f"⚠️ {items[0]['name']} sangat dominan ({top1_pct:.0f}%). "
            f"Fokus prioritas pada gangguan ini."
        )

    if n_for_80 <= 3:
        parts.append(
            f"Fokus perbaikan pada {n_for_80} jenis ini berpotensi "
            f"mengurangi {top_pct:.0f}% beban operasional."
        )

    return " ".join(parts)


def _interpret_rc_severity_cross(rc_sev_matrix, total_all):
    if not rc_sev_matrix or total_all == 0:
        return "Distribusi severity antar RC category relatif merata."

    total_critical = sum(
        counts.get("Critical", 0) for counts in rc_sev_matrix.values()
    )
    overall_crit_pct = total_critical / total_all * 100 if total_all > 0 else 0

    alerts = []
    for rc, counts in rc_sev_matrix.items():
        rc_total = sum(counts.values())
        if rc_total == 0:
            continue
        rc_crit_pct = counts.get("Critical", 0) / rc_total * 100
        if overall_crit_pct > 0 and rc_crit_pct > overall_crit_pct * 1.5:
            alerts.append(
                f"RC {rc} punya proporsi Critical tinggi "
                f"({rc_crit_pct:.0f}% vs rata-rata {overall_crit_pct:.0f}%)"
            )

    if alerts:
        return ". ".join(alerts) + " — gangguan jenis ini cenderung lebih parah."
    return "Distribusi severity antar RC category relatif merata."


@router.get("/overview")
async def gangguan_overview(
    entity_level: str = Query(...),
    entity_id: str = Query(...),
    date_from: str = Query(""),
    date_to: str = Query(""),
    type_ticket: str = Query(""),
    severities: str = Query(""),
):
    sm_where, sm_params = _build_sm_where(entity_level, entity_id, date_from, date_to, type_ticket, severities)
    tk_where, tk_params = _build_ticket_where(entity_level, entity_id, date_from, date_to, type_ticket, severities)

    with get_connection() as conn:
        sev_rows = conn.execute(f"""
            SELECT severity, SUM(total_tickets) as cnt
            FROM summary_monthly
            WHERE {sm_where} AND severity IS NOT NULL AND severity != ''
            GROUP BY severity
            ORDER BY cnt DESC
        """, sm_params).fetchall()

        sev_counts = {}
        for r in sev_rows:
            sev_counts[r[0]] = r[1]
        sev_total = sum(sev_counts.values())

        fl_rows = conn.execute(f"""
            SELECT fault_level, SUM(total_tickets) as cnt
            FROM summary_monthly
            WHERE {sm_where} AND fault_level IS NOT NULL AND fault_level != ''
            GROUP BY fault_level
            ORDER BY cnt DESC
        """, sm_params).fetchall()

        if not fl_rows:
            fl_rows = conn.execute(f"""
                SELECT fault_level, COUNT(*) as cnt
                FROM noc_tickets
                WHERE {tk_where} AND fault_level IS NOT NULL AND fault_level != ''
                GROUP BY fault_level
                ORDER BY cnt DESC
            """, tk_params).fetchall()

        fault_items = []
        fl_total = sum(r[1] for r in fl_rows)
        cumulative = 0
        for r in fl_rows:
            cumulative += r[1]
            pct = r[1] / fl_total * 100 if fl_total > 0 else 0
            cum_pct = cumulative / fl_total * 100 if fl_total > 0 else 0
            fault_items.append({
                "name": r[0],
                "count": r[1],
                "pct": round(pct, 1),
                "cumulative_pct": round(cum_pct, 1),
            })

        rc_rows = conn.execute(f"""
            SELECT rc_owner, severity, COUNT(*) as cnt
            FROM noc_tickets
            WHERE {tk_where} AND rc_owner IS NOT NULL AND rc_owner != ''
            GROUP BY rc_owner, severity
            ORDER BY cnt DESC
        """, tk_params).fetchall()

        rc_agg = {}
        rc_sev_matrix = {}
        for r in rc_rows:
            rc = r[0]
            sev = r[1] or "Unknown"
            cnt = r[2]
            rc_agg[rc] = rc_agg.get(rc, 0) + cnt
            if rc not in rc_sev_matrix:
                rc_sev_matrix[rc] = {}
            rc_sev_matrix[rc][sev] = rc_sev_matrix[rc].get(sev, 0) + cnt

        rc_total = sum(rc_agg.values())
        rc_items = sorted(
            [{"name": k, "count": v, "pct": round(v / rc_total * 100, 1) if rc_total > 0 else 0}
             for k, v in rc_agg.items()],
            key=lambda x: -x["count"]
        )

        sev_narrative = _interpret_severity_mix(sev_counts)
        fault_narrative = _interpret_fault_pareto(fault_items)

        rc_dominant = rc_items[0]["name"] if rc_items else "-"
        rc_dominant_pct = rc_items[0]["pct"] if rc_items else 0
        rc_parts = []
        if rc_items:
            rc_parts.append(f"RC {rc_dominant} paling banyak ({rc_dominant_pct:.0f}%)")
            if len(rc_items) > 1:
                rc_parts.append(f"diikuti {rc_items[1]['name']} ({rc_items[1]['pct']:.0f}%)")
        rc_cross_narr = _interpret_rc_severity_cross(rc_sev_matrix, rc_total)
        if rc_cross_narr and rc_cross_narr != "Distribusi severity antar RC category relatif merata.":
            rc_parts.append(rc_cross_narr)
        rc_narrative = ". ".join(rc_parts) + "." if rc_parts else "Tidak ada data RC category."

    return {
        "severity_mix": {
            "counts": sev_counts,
            "total": sev_total,
            "narrative": sev_narrative,
        },
        "fault_pareto": {
            "items": fault_items,
            "narrative": fault_narrative,
        },
        "rc_category": {
            "items": rc_items,
            "rc_severity_matrix": rc_sev_matrix,
            "narrative": rc_narrative,
        },
    }


@router.get("/cross-dimension")
async def gangguan_cross_dimension(
    entity_level: str = Query(...),
    entity_id: str = Query(...),
    fault_level: str = Query(""),
    rc_category: str = Query(""),
    date_from: str = Query(""),
    date_to: str = Query(""),
    type_ticket: str = Query(""),
    severities: str = Query(""),
):
    fault_filter = fault_level or rc_category
    fault_name = fault_filter or "Unknown"
    is_rc = bool(rc_category and not fault_level)

    sm_where, sm_params = _build_sm_where(entity_level, entity_id, date_from, date_to, type_ticket, severities)
    tk_where, tk_params = _build_ticket_where(entity_level, entity_id, date_from, date_to, type_ticket, severities)

    fault_col_sm = "fault_level"
    fault_col_tk = "fault_level"
    if is_rc:
        fault_col_sm = None
        fault_col_tk = "rc_owner"

    with get_connection() as conn:
        entity_name = _get_entity_name(conn, entity_level, entity_id)

        overall_row = conn.execute(f"""
            SELECT SUM(total_tickets), 
                   CASE WHEN SUM(total_tickets)>0 THEN SUM(total_sla_met)*100.0/SUM(total_tickets) ELSE 0 END,
                   AVG(avg_mttr_min)
            FROM summary_monthly WHERE {sm_where}
        """, sm_params).fetchone()
        overall_volume = overall_row[0] or 0
        overall_sla = overall_row[1] or 0
        overall_mttr = overall_row[2] or 0

        fl_tk_where = tk_where + f" AND {fault_col_tk} = ?"
        fl_tk_params = tk_params + [fault_filter]

        if fault_col_sm:
            fl_sm_where = sm_where + f" AND {fault_col_sm} = ?"
            fl_sm_params = sm_params + [fault_filter]
            fault_row = conn.execute(f"""
                SELECT SUM(total_tickets),
                       CASE WHEN SUM(total_tickets)>0 THEN SUM(total_sla_met)*100.0/SUM(total_tickets) ELSE 0 END,
                       AVG(avg_mttr_min)
                FROM summary_monthly WHERE {fl_sm_where}
            """, fl_sm_params).fetchone()
            if not fault_row or not fault_row[0]:
                fault_row = conn.execute(f"""
                    SELECT COUNT(*),
                           CASE WHEN COUNT(*)>0 THEN SUM(CASE WHEN calc_is_sla_met THEN 1 ELSE 0 END)*100.0/COUNT(*) ELSE 0 END,
                           AVG(calc_restore_time_min)
                    FROM noc_tickets WHERE {fl_tk_where}
                """, fl_tk_params).fetchone()
        else:
            fault_row = conn.execute(f"""
                SELECT COUNT(*),
                       CASE WHEN COUNT(*)>0 THEN SUM(CASE WHEN calc_is_sla_met THEN 1 ELSE 0 END)*100.0/COUNT(*) ELSE 0 END,
                       AVG(calc_restore_time_min)
                FROM noc_tickets WHERE {fl_tk_where}
            """, fl_tk_params).fetchone()

        fault_volume = fault_row[0] or 0
        fault_sla = fault_row[1] or 0
        fault_mttr = fault_row[2] or 0
        pct_of_total = fault_volume / overall_volume * 100 if overall_volume > 0 else 0
        sla_delta = fault_sla - overall_sla
        mttr_ratio = fault_mttr / overall_mttr if overall_mttr > 0 else 0

        overview_parts = []
        if pct_of_total > 30:
            overview_parts.append(f"{fault_name} adalah gangguan DOMINAN di {entity_name} ({pct_of_total:.0f}% total).")
        elif pct_of_total > 15:
            overview_parts.append(f"{fault_name} menyumbang {pct_of_total:.0f}% total tiket di {entity_name}.")
        else:
            overview_parts.append(f"{fault_name}: {fault_volume:,} tiket ({pct_of_total:.0f}% total).")

        if sla_delta < -3:
            overview_parts.append(
                f"SLA khusus {fault_name} {abs(sla_delta):.1f}pp lebih buruk dari keseluruhan "
                f"({fault_sla:.1f}% vs {overall_sla:.1f}%)."
            )
        if mttr_ratio > 1.2:
            pct_slower = (mttr_ratio - 1) * 100
            overview_parts.append(f"MTTR {pct_slower:.0f}% lebih lama dari rata-rata.")
        if pct_of_total > 25 and sla_delta < -2:
            overview_parts.append(f"Ini adalah penyumbang utama penurunan SLA {entity_name}.")

        child_level = CHILD_LEVEL_MAP.get(entity_level)
        distribution = {"children": [], "narrative": ""}

        if child_level:
            child_col_tk = LEVEL_COL_MAP[child_level][0]
            child_col_sm = LEVEL_COL_MAP[child_level][1]

            overall_dist_rows = conn.execute(f"""
                SELECT {child_col_sm}, SUM(total_tickets) as cnt
                FROM summary_monthly WHERE {sm_where} AND {child_col_sm} IS NOT NULL
                GROUP BY {child_col_sm}
            """, sm_params).fetchall()
            overall_dist = {r[0]: r[1] for r in overall_dist_rows}
            overall_dist_total = sum(overall_dist.values())

            if fault_col_sm:
                fault_dist_rows = conn.execute(f"""
                    SELECT {child_col_sm}, SUM(total_tickets) as cnt
                    FROM summary_monthly WHERE {fl_sm_where} AND {child_col_sm} IS NOT NULL
                    GROUP BY {child_col_sm}
                """, fl_sm_params).fetchall()
                if not fault_dist_rows:
                    fault_dist_rows = conn.execute(f"""
                        SELECT {child_col_tk}, COUNT(*) as cnt
                        FROM noc_tickets WHERE {fl_tk_where} AND {child_col_tk} IS NOT NULL
                        GROUP BY {child_col_tk}
                    """, fl_tk_params).fetchall()
            else:
                fault_dist_rows = conn.execute(f"""
                    SELECT {child_col_tk}, COUNT(*) as cnt
                    FROM noc_tickets WHERE {fl_tk_where} AND {child_col_tk} IS NOT NULL
                    GROUP BY {child_col_tk}
                """, fl_tk_params).fetchall()

            fault_dist = {r[0]: r[1] for r in fault_dist_rows}
            fault_dist_total = sum(fault_dist.values())

            children = []
            for cid, cnt in sorted(fault_dist.items(), key=lambda x: -x[1]):
                actual_pct = cnt / fault_dist_total * 100 if fault_dist_total > 0 else 0
                expected_pct = (overall_dist.get(cid, 0) / overall_dist_total * 100) if overall_dist_total > 0 else 0
                diff_pp = actual_pct - expected_pct
                cname = _get_entity_name(conn, child_level, cid)
                children.append({
                    "entity_id": cid,
                    "entity_name": cname,
                    "count": cnt,
                    "actual_pct": round(actual_pct, 1),
                    "expected_pct": round(expected_pct, 1),
                    "diff_pp": round(diff_pp, 1),
                    "is_over": diff_pp > 5,
                    "is_under": diff_pp < -5,
                })

            over = [c for c in children if c["is_over"]]
            if over:
                worst = max(over, key=lambda c: c["diff_pp"])
                dist_narr = (
                    f"{fault_name} terkonsentrasi di {worst['entity_name']} ({worst['actual_pct']:.0f}%). "
                    f"{worst['entity_name']} menangani {worst['expected_pct']:.0f}% total tiket tapi "
                    f"{worst['actual_pct']:.0f}% {fault_name}. Over-representation {worst['diff_pp']:.0f}pp."
                )
            else:
                dist_narr = f"Distribusi {fault_name} proporsional — tidak ada konsentrasi signifikan."

            distribution = {"children": children, "narrative": dist_narr}

        repeat_patterns = []
        try:
            rp_where = tk_where + f" AND {fault_col_tk} = ?"
            rp_params = tk_params + [fault_filter]
            rp_rows = conn.execute(f"""
                WITH ticket_intervals AS (
                    SELECT site_id, occured_time,
                        LEAD(occured_time) OVER (PARTITION BY site_id ORDER BY occured_time) as next_time,
                        EXTRACT(EPOCH FROM (
                            LEAD(occured_time) OVER (PARTITION BY site_id ORDER BY occured_time) - occured_time
                        )) / 86400.0 as gap_days
                    FROM noc_tickets
                    WHERE {rp_where}
                )
                SELECT site_id,
                    COUNT(*) as ticket_count,
                    AVG(gap_days) as avg_gap,
                    STDDEV_POP(gap_days) as std_gap,
                    MIN(gap_days) as min_gap,
                    MAX(gap_days) as max_gap
                FROM ticket_intervals
                WHERE gap_days IS NOT NULL
                GROUP BY site_id
                HAVING COUNT(*) >= 2
                ORDER BY ticket_count DESC
                LIMIT 20
            """, rp_params).fetchall()

            for r in rp_rows:
                avg_gap = r[2] or 0
                std_gap = r[3] or 0
                cv = std_gap / avg_gap if avg_gap > 0 else 999
                if cv < 0.3:
                    pattern = "Regular"
                    pattern_icon = "🔴"
                elif cv < 0.6:
                    pattern = "Semi-regular"
                    pattern_icon = "🟡"
                else:
                    pattern = "Irregular"
                    pattern_icon = "─"

                site_name = _get_entity_name(conn, "site", r[0])
                repeat_patterns.append({
                    "site_id": r[0],
                    "site_name": site_name,
                    "ticket_count": r[1],
                    "avg_gap_days": round(avg_gap, 1),
                    "cv": round(cv, 2),
                    "pattern": pattern,
                    "pattern_icon": pattern_icon,
                    "min_gap_days": round(r[4] or 0, 1),
                    "max_gap_days": round(r[5] or 0, 1),
                })
        except:
            pass

        recs = _generate_fault_recommendations(
            fault_name, entity_name, sla_delta, pct_of_total,
            distribution.get("children", []), repeat_patterns
        )

    return {
        "overview": {
            "fault_name": fault_name,
            "entity_name": entity_name,
            "volume": fault_volume,
            "pct_of_total": round(pct_of_total, 1),
            "sla_pct": round(fault_sla, 1),
            "sla_overall": round(overall_sla, 1),
            "sla_delta": round(sla_delta, 1),
            "avg_mttr_min": round(fault_mttr, 1),
            "mttr_overall": round(overall_mttr, 1),
            "narrative": " ".join(overview_parts),
        },
        "distribution": distribution,
        "repeat_patterns": repeat_patterns,
        "recommendations": recs,
    }


@router.get("/distribution")
async def gangguan_distribution(
    entity_level: str = Query(...),
    entity_id: str = Query(...),
    fault_level: str = Query(""),
    rc_category: str = Query(""),
    date_from: str = Query(""),
    date_to: str = Query(""),
    type_ticket: str = Query(""),
    severities: str = Query(""),
):
    fault_filter = fault_level or rc_category
    fault_name = fault_filter or "Unknown"
    is_rc = bool(rc_category and not fault_level)

    child_level = CHILD_LEVEL_MAP.get(entity_level)
    if not child_level:
        return {"children": [], "narrative": "Level site tidak memiliki child entity."}

    sm_where, sm_params = _build_sm_where(entity_level, entity_id, date_from, date_to, type_ticket, severities)
    tk_where, tk_params = _build_ticket_where(entity_level, entity_id, date_from, date_to, type_ticket, severities)

    fault_col_sm = "fault_level" if not is_rc else None
    fault_col_tk = "rc_owner" if is_rc else "fault_level"

    with get_connection() as conn:
        child_col_sm = LEVEL_COL_MAP[child_level][1]
        child_col_tk = LEVEL_COL_MAP[child_level][0]

        overall_rows = conn.execute(f"""
            SELECT {child_col_sm}, SUM(total_tickets)
            FROM summary_monthly WHERE {sm_where} AND {child_col_sm} IS NOT NULL
            GROUP BY {child_col_sm}
        """, sm_params).fetchall()
        overall_dist = {r[0]: r[1] for r in overall_rows}
        overall_total = sum(overall_dist.values())

        if fault_col_sm:
            fl_where = sm_where + f" AND {fault_col_sm} = ?"
            fl_params = sm_params + [fault_filter]
            fault_rows = conn.execute(f"""
                SELECT {child_col_sm}, SUM(total_tickets)
                FROM summary_monthly WHERE {fl_where} AND {child_col_sm} IS NOT NULL
                GROUP BY {child_col_sm}
            """, fl_params).fetchall()
        else:
            fl_where = tk_where + f" AND {fault_col_tk} = ?"
            fl_params = tk_params + [fault_filter]
            fault_rows = conn.execute(f"""
                SELECT {child_col_tk}, COUNT(*)
                FROM noc_tickets WHERE {fl_where} AND {child_col_tk} IS NOT NULL
                GROUP BY {child_col_tk}
            """, fl_params).fetchall()

        fault_dist = {r[0]: r[1] for r in fault_rows}
        fault_total = sum(fault_dist.values())

        children = []
        for cid, cnt in sorted(fault_dist.items(), key=lambda x: -x[1]):
            actual_pct = cnt / fault_total * 100 if fault_total > 0 else 0
            expected_pct = overall_dist.get(cid, 0) / overall_total * 100 if overall_total > 0 else 0
            diff_pp = actual_pct - expected_pct
            cname = _get_entity_name(conn, child_level, cid)
            children.append({
                "entity_id": cid,
                "entity_name": cname,
                "count": cnt,
                "actual_pct": round(actual_pct, 1),
                "expected_pct": round(expected_pct, 1),
                "diff_pp": round(diff_pp, 1),
                "is_over": diff_pp > 5,
                "is_under": diff_pp < -5,
            })

        over = [c for c in children if c["is_over"]]
        if over:
            worst = max(over, key=lambda c: c["diff_pp"])
            narr = (
                f"{fault_name} terkonsentrasi di {worst['entity_name']} ({worst['actual_pct']:.0f}%). "
                f"Over-representation {worst['diff_pp']:.0f}pp."
            )
        else:
            narr = f"Distribusi {fault_name} proporsional — tidak ada konsentrasi signifikan."

    return {
        "child_level": child_level,
        "child_label": TYPE_LABELS.get(child_level, child_level),
        "fault_name": fault_name,
        "children": children,
        "narrative": narr,
    }


@router.get("/top-sites")
async def gangguan_top_sites(
    entity_level: str = Query(...),
    entity_id: str = Query(...),
    fault_level: str = Query(""),
    rc_category: str = Query(""),
    date_from: str = Query(""),
    date_to: str = Query(""),
    type_ticket: str = Query(""),
    severities: str = Query(""),
    limit: int = Query(20),
):
    fault_filter = fault_level or rc_category
    fault_name = fault_filter or "Unknown"
    is_rc = bool(rc_category and not fault_level)
    fault_col_tk = "rc_owner" if is_rc else "fault_level"

    tk_where, tk_params = _build_ticket_where(entity_level, entity_id, date_from, date_to, type_ticket, severities)
    fl_where = tk_where + f" AND {fault_col_tk} = ?"
    fl_params = tk_params + [fault_filter]

    with get_connection() as conn:
        rows = conn.execute(f"""
            SELECT site_id, COUNT(*) as cnt,
                   AVG(calc_restore_time_min) as avg_mttr
            FROM noc_tickets
            WHERE {fl_where} AND site_id IS NOT NULL
            GROUP BY site_id
            ORDER BY cnt DESC
            LIMIT ?
        """, fl_params + [limit]).fetchall()

        sites = []
        for r in rows:
            site_name = _get_entity_name(conn, "site", r[0])

            try:
                intervals = conn.execute(f"""
                    WITH ordered AS (
                        SELECT occured_time,
                            LEAD(occured_time) OVER (ORDER BY occured_time) as next_time,
                            EXTRACT(EPOCH FROM (
                                LEAD(occured_time) OVER (ORDER BY occured_time) - occured_time
                            )) / 86400.0 as gap_days
                        FROM noc_tickets
                        WHERE {fl_where} AND site_id = ?
                    )
                    SELECT AVG(gap_days), STDDEV_POP(gap_days)
                    FROM ordered WHERE gap_days IS NOT NULL
                """, fl_params + [r[0]]).fetchone()

                avg_gap = intervals[0] or 0
                std_gap = intervals[1] or 0
                cv = std_gap / avg_gap if avg_gap > 0 else 999
            except:
                avg_gap, cv = 0, 999

            if cv < 0.3:
                pattern, pattern_icon = "Regular", "🔴"
            elif cv < 0.6:
                pattern, pattern_icon = "Semi-regular", "🟡"
            else:
                pattern, pattern_icon = "Irregular", "─"

            sites.append({
                "site_id": r[0],
                "site_name": site_name,
                "ticket_count": r[1],
                "avg_mttr_min": round(r[2] or 0, 0),
                "avg_gap_days": round(avg_gap, 1),
                "cv": round(cv, 2),
                "pattern": pattern,
                "pattern_icon": pattern_icon,
            })

        total_in_top = sum(s["ticket_count"] for s in sites)

    return {
        "fault_name": fault_name,
        "sites": sites,
        "total_in_top": total_in_top,
        "narrative": (
            f"Top {len(sites)} site = {total_in_top:,} tiket {fault_name}."
            if sites else f"Tidak ada data site untuk {fault_name}."
        ),
    }


def _generate_fault_recommendations(fault_name, entity_name, sla_delta, pct_of_total, children, repeat_patterns):
    recs = []

    for site in repeat_patterns[:3]:
        if site.get("pattern") in ("Regular", "Semi-regular"):
            recs.append({
                "priority": "critical",
                "icon": "🔴",
                "text": (
                    f"RCA mendalam untuk site {site['site_name']} "
                    f"({site['ticket_count']} {fault_name}, pola {site['avg_gap_days']:.0f}-hari)"
                ),
            })

    for child in children:
        if child.get("is_over") and child.get("diff_pp", 0) > 5:
            recs.append({
                "priority": "warning",
                "icon": "🟡",
                "text": (
                    f"Review {fault_name} di {child['entity_name']} "
                    f"(over-representation {child['diff_pp']:.0f}pp)"
                ),
            })

    if sla_delta < -3:
        recs.append({
            "priority": "critical",
            "icon": "🔴",
            "text": (
                f"{fault_name} menurunkan SLA {abs(sla_delta):.1f}pp. "
                f"Perbaikan {fault_name} = perbaikan SLA terbesar."
            ),
        })

    if pct_of_total > 40:
        recs.append({
            "priority": "warning",
            "icon": "🟡",
            "text": (
                f"{fault_name} menyumbang {pct_of_total:.0f}% total tiket. "
                f"Fokus utama operasional harus pada gangguan ini."
            ),
        })

    recs.sort(key=lambda r: {"critical": 0, "warning": 1}.get(r["priority"], 9))
    return recs[:5]
