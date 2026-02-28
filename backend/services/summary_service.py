import logging
from backend.database import get_write_connection

logger = logging.getLogger(__name__)

SUMMARY_BASE = """
    COUNT(*) as total_tickets,
    SUM(CASE WHEN calc_is_sla_met THEN 1 ELSE 0 END) as total_sla_met,
    ROUND(SUM(CASE WHEN calc_is_sla_met THEN 1 ELSE 0 END) * 100.0 /
          NULLIF(COUNT(*), 0), 2) as sla_pct,
    ROUND(AVG(calc_restore_time_min), 2) as avg_mttr_min,
    ROUND(AVG(calc_response_time_min), 2) as avg_response_min,
    SUM(CASE WHEN is_escalate IN ('True', 'true', 'TRUE', '1') THEN 1 ELSE 0 END) as total_escalated,
    ROUND(SUM(CASE WHEN is_escalate IN ('True', 'true', 'TRUE', '1') THEN 1 ELSE 0 END) * 100.0 /
          NULLIF(COUNT(*), 0), 2) as escalation_pct,
    SUM(CASE WHEN is_auto_resolved = 'Auto Resolved' THEN 1 ELSE 0 END) as total_auto_resolved,
    ROUND(SUM(CASE WHEN is_auto_resolved = 'Auto Resolved' THEN 1 ELSE 0 END) * 100.0 /
          NULLIF(COUNT(*), 0), 2) as auto_resolve_pct,
    0 as total_repeat,
    0.0 as repeat_pct,
    SUM(CASE WHEN severity = 'Critical' THEN 1 ELSE 0 END) as count_critical,
    SUM(CASE WHEN severity = 'Major' THEN 1 ELSE 0 END) as count_major,
    SUM(CASE WHEN severity = 'Minor' THEN 1 ELSE 0 END) as count_minor,
    SUM(CASE WHEN severity = 'Low' THEN 1 ELSE 0 END) as count_low,
    CURRENT_TIMESTAMP as calculated_at
"""


def refresh_summaries(year_months: list):
    if not year_months:
        return

    placeholders = ", ".join(["?" for _ in year_months])

    with get_write_connection() as conn:
        conn.execute(f"DELETE FROM summary_monthly WHERE year_month IN ({placeholders})", year_months)

        _refresh_monthly_by_level(conn, year_months, placeholders, "area")
        _refresh_monthly_by_level(conn, year_months, placeholders, "regional")
        _refresh_monthly_by_level(conn, year_months, placeholders, "nop")
        _refresh_monthly_by_level(conn, year_months, placeholders, "to")
        _refresh_monthly_by_level(conn, year_months, placeholders, "site")

        year_weeks = conn.execute(f"""
            SELECT DISTINCT calc_year_week FROM noc_tickets
            WHERE calc_year_month IN ({placeholders}) AND calc_year_week IS NOT NULL
        """, year_months).fetchall()

        if year_weeks:
            week_list = [w[0] for w in year_weeks]
            week_ph = ", ".join(["?" for _ in week_list])
            conn.execute(f"DELETE FROM summary_weekly WHERE year_week IN ({week_ph})", week_list)
            _refresh_weekly(conn, week_list, week_ph)

    logger.info(f"Summaries refreshed for periods: {year_months}")


def _refresh_monthly_by_level(conn, year_months, placeholders, level):
    if level == "area":
        group_cols = "calc_area_id"
        select_cols = "calc_area_id as area_id, NULL as regional_id, NULL as nop_id, NULL as to_id, NULL as site_id"
        where_extra = "AND calc_area_id IS NOT NULL"
    elif level == "regional":
        group_cols = "calc_area_id, calc_regional_id"
        select_cols = "calc_area_id as area_id, calc_regional_id as regional_id, NULL as nop_id, NULL as to_id, NULL as site_id"
        where_extra = "AND calc_regional_id IS NOT NULL"
    elif level == "nop":
        group_cols = "calc_area_id, calc_regional_id, calc_nop_id"
        select_cols = "calc_area_id as area_id, calc_regional_id as regional_id, calc_nop_id as nop_id, NULL as to_id, NULL as site_id"
        where_extra = "AND calc_nop_id IS NOT NULL"
    elif level == "to":
        group_cols = "calc_area_id, calc_regional_id, calc_nop_id, calc_to_id"
        select_cols = "calc_area_id as area_id, calc_regional_id as regional_id, calc_nop_id as nop_id, calc_to_id as to_id, NULL as site_id"
        where_extra = "AND calc_to_id IS NOT NULL"
    else:
        group_cols = "site_id"
        select_cols = "NULL as area_id, NULL as regional_id, NULL as nop_id, NULL as to_id, site_id"
        where_extra = "AND site_id IS NOT NULL"

    if level in ("area", "regional", "nop", "to"):
        conn.execute(f"""
            INSERT INTO summary_monthly
            SELECT
                calc_year_month as year_month,
                {select_cols},
                NULL as severity, NULL as type_ticket, NULL as fault_level,
                {SUMMARY_BASE}
            FROM noc_tickets
            WHERE calc_year_month IN ({placeholders}) {where_extra}
            GROUP BY calc_year_month, {group_cols}
        """, year_months)

        conn.execute(f"""
            INSERT INTO summary_monthly
            SELECT
                calc_year_month as year_month,
                {select_cols},
                severity, NULL as type_ticket, NULL as fault_level,
                {SUMMARY_BASE}
            FROM noc_tickets
            WHERE calc_year_month IN ({placeholders}) {where_extra}
            GROUP BY calc_year_month, {group_cols}, severity
        """, year_months)

        conn.execute(f"""
            INSERT INTO summary_monthly
            SELECT
                calc_year_month as year_month,
                {select_cols},
                NULL as severity, type_ticket, NULL as fault_level,
                {SUMMARY_BASE}
            FROM noc_tickets
            WHERE calc_year_month IN ({placeholders}) {where_extra}
            GROUP BY calc_year_month, {group_cols}, type_ticket
        """, year_months)
    else:
        conn.execute(f"""
            INSERT INTO summary_monthly
            SELECT
                calc_year_month as year_month,
                {select_cols},
                NULL as severity, NULL as type_ticket, NULL as fault_level,
                {SUMMARY_BASE}
            FROM noc_tickets
            WHERE calc_year_month IN ({placeholders}) {where_extra}
            GROUP BY calc_year_month, {group_cols}
        """, year_months)


def _refresh_weekly(conn, week_list, week_ph):
    for level in ["area", "regional"]:
        if level == "area":
            group_cols = "calc_area_id"
            select_cols = "calc_area_id as area_id, NULL as regional_id, NULL as nop_id, NULL as to_id, NULL as site_id"
            where_extra = "AND calc_area_id IS NOT NULL"
        else:
            group_cols = "calc_area_id, calc_regional_id"
            select_cols = "calc_area_id as area_id, calc_regional_id as regional_id, NULL as nop_id, NULL as to_id, NULL as site_id"
            where_extra = "AND calc_regional_id IS NOT NULL"

        conn.execute(f"""
            INSERT INTO summary_weekly
            SELECT
                calc_year_week as year_week,
                {select_cols},
                COUNT(*) as total_tickets,
                ROUND(SUM(CASE WHEN calc_is_sla_met THEN 1 ELSE 0 END) * 100.0 /
                      NULLIF(COUNT(*), 0), 2) as sla_pct,
                ROUND(AVG(calc_restore_time_min), 2) as avg_mttr_min,
                ROUND(AVG(calc_response_time_min), 2) as avg_response_min,
                SUM(CASE WHEN is_escalate IN ('True', 'true', 'TRUE', '1') THEN 1 ELSE 0 END) as total_escalated,
                ROUND(SUM(CASE WHEN is_escalate IN ('True', 'true', 'TRUE', '1') THEN 1 ELSE 0 END) * 100.0 /
                      NULLIF(COUNT(*), 0), 2) as escalation_pct,
                SUM(CASE WHEN is_auto_resolved = 'Auto Resolved' THEN 1 ELSE 0 END) as total_auto_resolved,
                ROUND(SUM(CASE WHEN is_auto_resolved = 'Auto Resolved' THEN 1 ELSE 0 END) * 100.0 /
                      NULLIF(COUNT(*), 0), 2) as auto_resolve_pct,
                CURRENT_TIMESTAMP as calculated_at
            FROM noc_tickets
            WHERE calc_year_week IN ({week_ph}) {where_extra}
            GROUP BY calc_year_week, {group_cols}
        """, week_list)
