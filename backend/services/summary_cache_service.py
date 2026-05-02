from __future__ import annotations

from pathlib import Path

import backend.services.parquet_lake_service as lake
from backend.database import get_write_connection
from backend.services.ingestion_service import configure_duckdb_connection, escape_sql_string


SUMMARY_COLUMNS = """
    year_month,
    area_id,
    regional_id,
    nop_id,
    to_id,
    site_id,
    severity,
    type_ticket,
    fault_level,
    total_tickets,
    total_sla_met,
    sla_pct,
    avg_mttr_min,
    avg_response_min,
    total_escalated,
    escalation_pct,
    total_auto_resolved,
    auto_resolve_pct,
    total_repeat,
    repeat_pct,
    count_critical,
    count_major,
    count_minor,
    count_low,
    calculated_at
"""

SUMMARY_BASE = """
    COUNT(*) AS total_tickets,
    SUM(CASE WHEN calc_is_sla_met THEN 1 ELSE 0 END) AS total_sla_met,
    ROUND(100.0 * SUM(CASE WHEN calc_is_sla_met THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS sla_pct,
    ROUND(AVG(calc_restore_time_min), 2) AS avg_mttr_min,
    ROUND(AVG(calc_response_time_min), 2) AS avg_response_min,
    SUM(CASE WHEN UPPER(COALESCE(is_escalate, '')) IN ('TRUE', 'YES', 'Y', '1') THEN 1 ELSE 0 END) AS total_escalated,
    ROUND(100.0 * SUM(CASE WHEN UPPER(COALESCE(is_escalate, '')) IN ('TRUE', 'YES', 'Y', '1') THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS escalation_pct,
    SUM(CASE WHEN UPPER(COALESCE(is_auto_resolved, '')) IN ('TRUE', 'YES', 'Y', '1', 'AUTO RESOLVED') THEN 1 ELSE 0 END) AS total_auto_resolved,
    ROUND(100.0 * SUM(CASE WHEN UPPER(COALESCE(is_auto_resolved, '')) IN ('TRUE', 'YES', 'Y', '1', 'AUTO RESOLVED') THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS auto_resolve_pct,
    0 AS total_repeat,
    0.0 AS repeat_pct,
    SUM(CASE WHEN UPPER(COALESCE(severity, '')) = 'CRITICAL' THEN 1 ELSE 0 END) AS count_critical,
    SUM(CASE WHEN UPPER(COALESCE(severity, '')) = 'MAJOR' THEN 1 ELSE 0 END) AS count_major,
    SUM(CASE WHEN UPPER(COALESCE(severity, '')) = 'MINOR' THEN 1 ELSE 0 END) AS count_minor,
    SUM(CASE WHEN UPPER(COALESCE(severity, '')) = 'LOW' THEN 1 ELSE 0 END) AS count_low,
    CURRENT_TIMESTAMP AS calculated_at
"""


def refresh_duckdb_summaries_from_silver(year: int, month: int) -> dict:
    """Refresh DuckDB summary cache from all Silver Parquet partitions for a month."""
    period = f"{int(year):04d}-{int(month):02d}"
    silver_uri = _silver_month_glob(year, month)
    escaped_uri = escape_sql_string(silver_uri)

    with get_write_connection() as conn:
        configure_duckdb_connection(conn, [silver_uri])
        source_rows = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM read_parquet('{escaped_uri}', hive_partitioning = true, union_by_name = true)
            WHERE calc_year_month = ?
            """,
            [period],
        ).fetchone()[0]

        week_rows = conn.execute(
            f"""
            SELECT DISTINCT calc_year_week
            FROM read_parquet('{escaped_uri}', hive_partitioning = true, union_by_name = true)
            WHERE calc_year_month = ? AND calc_year_week IS NOT NULL
            """,
            [period],
        ).fetchall()
        week_list = [row[0] for row in week_rows if row and row[0]]

        conn.execute("DELETE FROM summary_monthly WHERE year_month = ?", [period])
        if week_list:
            placeholders = ", ".join(["?" for _ in week_list])
            conn.execute(f"DELETE FROM summary_weekly WHERE year_week IN ({placeholders})", week_list)

        if source_rows:
            for level in ("area", "regional", "nop", "to", "site"):
                _insert_monthly_level(conn, escaped_uri, period, level)
            _insert_weekly_level(conn, escaped_uri, period, "area")
            _insert_weekly_level(conn, escaped_uri, period, "regional")

        monthly_rows = conn.execute(
            "SELECT COUNT(*) FROM summary_monthly WHERE year_month = ?",
            [period],
        ).fetchone()[0]
        if week_list:
            placeholders = ", ".join(["?" for _ in week_list])
            weekly_rows = conn.execute(
                f"SELECT COUNT(*) FROM summary_weekly WHERE year_week IN ({placeholders})",
                week_list,
            ).fetchone()[0]
        else:
            weekly_rows = 0

    return {
        "status": "completed",
        "period": period,
        "silver_uri": silver_uri,
        "source_rows": int(source_rows or 0),
        "summary_monthly_rows": int(monthly_rows or 0),
        "summary_weekly_rows": int(weekly_rows or 0),
    }


def _insert_monthly_level(conn, escaped_uri: str, period: str, level: str) -> None:
    group_cols, select_cols, where_extra = _monthly_level_parts(level)
    conn.execute(
        f"""
        INSERT INTO summary_monthly ({SUMMARY_COLUMNS})
        SELECT
            calc_year_month AS year_month,
            {select_cols},
            NULL AS severity,
            NULL AS type_ticket,
            NULL AS fault_level,
            {SUMMARY_BASE}
        FROM read_parquet('{escaped_uri}', hive_partitioning = true, union_by_name = true)
        WHERE calc_year_month = ? {where_extra}
        GROUP BY calc_year_month, {group_cols}
        """,
        [period],
    )

    if level == "site":
        return

    conn.execute(
        f"""
        INSERT INTO summary_monthly ({SUMMARY_COLUMNS})
        SELECT
            calc_year_month AS year_month,
            {select_cols},
            severity,
            NULL AS type_ticket,
            NULL AS fault_level,
            {SUMMARY_BASE}
        FROM read_parquet('{escaped_uri}', hive_partitioning = true, union_by_name = true)
        WHERE calc_year_month = ? {where_extra}
        GROUP BY calc_year_month, {group_cols}, severity
        """,
        [period],
    )
    conn.execute(
        f"""
        INSERT INTO summary_monthly ({SUMMARY_COLUMNS})
        SELECT
            calc_year_month AS year_month,
            {select_cols},
            NULL AS severity,
            type_ticket,
            NULL AS fault_level,
            {SUMMARY_BASE}
        FROM read_parquet('{escaped_uri}', hive_partitioning = true, union_by_name = true)
        WHERE calc_year_month = ? {where_extra}
        GROUP BY calc_year_month, {group_cols}, type_ticket
        """,
        [period],
    )


def _insert_weekly_level(conn, escaped_uri: str, period: str, level: str) -> None:
    if level == "area":
        group_cols = "calc_area_id"
        select_cols = "calc_area_id AS area_id, NULL AS regional_id, NULL AS nop_id, NULL AS to_id, NULL AS site_id"
        where_extra = "AND calc_area_id IS NOT NULL"
    elif level == "regional":
        group_cols = "calc_area_id, calc_regional_id"
        select_cols = "calc_area_id AS area_id, calc_regional_id AS regional_id, NULL AS nop_id, NULL AS to_id, NULL AS site_id"
        where_extra = "AND calc_regional_id IS NOT NULL"
    else:
        raise ValueError(f"Unsupported weekly level: {level}")

    conn.execute(
        f"""
        INSERT INTO summary_weekly (
            year_week,
            area_id,
            regional_id,
            nop_id,
            to_id,
            site_id,
            total_tickets,
            sla_pct,
            avg_mttr_min,
            avg_response_min,
            total_escalated,
            escalation_pct,
            total_auto_resolved,
            auto_resolve_pct,
            calculated_at
        )
        SELECT
            calc_year_week AS year_week,
            {select_cols},
            COUNT(*) AS total_tickets,
            ROUND(100.0 * SUM(CASE WHEN calc_is_sla_met THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS sla_pct,
            ROUND(AVG(calc_restore_time_min), 2) AS avg_mttr_min,
            ROUND(AVG(calc_response_time_min), 2) AS avg_response_min,
            SUM(CASE WHEN UPPER(COALESCE(is_escalate, '')) IN ('TRUE', 'YES', 'Y', '1') THEN 1 ELSE 0 END) AS total_escalated,
            ROUND(100.0 * SUM(CASE WHEN UPPER(COALESCE(is_escalate, '')) IN ('TRUE', 'YES', 'Y', '1') THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS escalation_pct,
            SUM(CASE WHEN UPPER(COALESCE(is_auto_resolved, '')) IN ('TRUE', 'YES', 'Y', '1', 'AUTO RESOLVED') THEN 1 ELSE 0 END) AS total_auto_resolved,
            ROUND(100.0 * SUM(CASE WHEN UPPER(COALESCE(is_auto_resolved, '')) IN ('TRUE', 'YES', 'Y', '1', 'AUTO RESOLVED') THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS auto_resolve_pct,
            CURRENT_TIMESTAMP AS calculated_at
        FROM read_parquet('{escaped_uri}', hive_partitioning = true, union_by_name = true)
        WHERE calc_year_month = ? AND calc_year_week IS NOT NULL {where_extra}
        GROUP BY calc_year_week, {group_cols}
        """,
        [period],
    )


def _monthly_level_parts(level: str) -> tuple[str, str, str]:
    if level == "area":
        return (
            "calc_area_id",
            "calc_area_id AS area_id, NULL AS regional_id, NULL AS nop_id, NULL AS to_id, NULL AS site_id",
            "AND calc_area_id IS NOT NULL",
        )
    if level == "regional":
        return (
            "calc_area_id, calc_regional_id",
            "calc_area_id AS area_id, calc_regional_id AS regional_id, NULL AS nop_id, NULL AS to_id, NULL AS site_id",
            "AND calc_regional_id IS NOT NULL",
        )
    if level == "nop":
        return (
            "calc_area_id, calc_regional_id, calc_nop_id",
            "calc_area_id AS area_id, calc_regional_id AS regional_id, calc_nop_id AS nop_id, NULL AS to_id, NULL AS site_id",
            "AND calc_nop_id IS NOT NULL",
        )
    if level == "to":
        return (
            "calc_area_id, calc_regional_id, calc_nop_id, calc_to_id",
            "calc_area_id AS area_id, calc_regional_id AS regional_id, calc_nop_id AS nop_id, calc_to_id AS to_id, NULL AS site_id",
            "AND calc_to_id IS NOT NULL",
        )
    if level == "site":
        return (
            "site_id",
            "NULL AS area_id, NULL AS regional_id, NULL AS nop_id, NULL AS to_id, site_id",
            "AND site_id IS NOT NULL",
        )
    raise ValueError(f"Unsupported summary level: {level}")


def _silver_month_glob(year: int, month: int) -> str:
    root = Path(str(lake.LAKE_ROOT))
    return str(root / "tickets" / "silver" / f"year={int(year):04d}" / f"month={int(month):02d}" / "source=*" / "*.parquet")
