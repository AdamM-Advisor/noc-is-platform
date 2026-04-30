from __future__ import annotations

from backend.services.ingestion_service import (
    configure_duckdb_connection,
    ensure_local_parent,
    escape_sql_string,
    normalize_storage_uri,
    partition_output_uri,
    storage_size_bytes,
)
from backend.services.parquet_lake_service import LAKE_ROOT
from backend.services.silver_transform_service import parquet_read_uri


def monthly_summary_partition_uri(source: str, year: int, month: int) -> str:
    root = str(LAKE_ROOT).rstrip("/\\")
    return f"{root}/summaries/monthly/year={int(year):04d}/month={int(month):02d}/source={source}"


def monthly_summary_writer_sql(
    silver_uri: str,
    target_partition_uri: str,
    source: str,
    year: int,
    month: int,
) -> str:
    normalized_silver_uri = normalize_storage_uri(silver_uri)
    normalized_target_uri = normalize_storage_uri(target_partition_uri)
    read_uri = parquet_read_uri(normalized_silver_uri)
    output_uri = partition_output_uri(normalized_target_uri, normalized_silver_uri)
    period = f"{int(year):04d}-{int(month):02d}"
    source_sql = escape_sql_string(source)

    return f"""
COPY (
    SELECT
        calc_year_month AS year_month,
        COALESCE(calc_source, '{source_sql}') AS source,
        calc_area_id AS area_id,
        calc_regional_id AS regional_id,
        calc_nop_id AS nop_id,
        calc_to_id AS to_id,
        site_id,
        severity,
        type_ticket,
        fault_level,
        COUNT(*) AS total_tickets,
        SUM(CASE WHEN calc_is_sla_met THEN 1 ELSE 0 END) AS total_sla_met,
        ROUND(100.0 * SUM(CASE WHEN calc_is_sla_met THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS sla_pct,
        ROUND(AVG(calc_restore_time_min), 2) AS avg_mttr_min,
        ROUND(AVG(calc_response_time_min), 2) AS avg_response_min,
        SUM(CASE WHEN UPPER(COALESCE(is_escalate, '')) IN ('TRUE', 'YES', 'Y', '1') THEN 1 ELSE 0 END) AS total_escalated,
        ROUND(100.0 * SUM(CASE WHEN UPPER(COALESCE(is_escalate, '')) IN ('TRUE', 'YES', 'Y', '1') THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS escalation_pct,
        SUM(CASE WHEN UPPER(COALESCE(is_auto_resolved, '')) IN ('TRUE', 'YES', 'Y', '1', 'AUTO RESOLVED') THEN 1 ELSE 0 END) AS total_auto_resolved,
        ROUND(100.0 * SUM(CASE WHEN UPPER(COALESCE(is_auto_resolved, '')) IN ('TRUE', 'YES', 'Y', '1', 'AUTO RESOLVED') THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS auto_resolve_pct,
        SUM(CASE WHEN severity = 'Critical' THEN 1 ELSE 0 END) AS count_critical,
        SUM(CASE WHEN severity = 'Major' THEN 1 ELSE 0 END) AS count_major,
        SUM(CASE WHEN severity = 'Minor' THEN 1 ELSE 0 END) AS count_minor,
        SUM(CASE WHEN severity = 'Low' THEN 1 ELSE 0 END) AS count_low,
        CURRENT_TIMESTAMP AS calculated_at
    FROM read_parquet('{escape_sql_string(read_uri)}', hive_partitioning = true, union_by_name = true)
    WHERE calc_year_month = '{period}'
      AND COALESCE(calc_source, '{source_sql}') = '{source_sql}'
    GROUP BY
        calc_year_month,
        COALESCE(calc_source, '{source_sql}'),
        calc_area_id,
        calc_regional_id,
        calc_nop_id,
        calc_to_id,
        site_id,
        severity,
        type_ticket,
        fault_level
) TO '{escape_sql_string(output_uri)}'
  (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE);
""".strip()


def execute_monthly_summary_refresh(
    silver_uri: str,
    target_partition_uri: str,
    source: str,
    year: int,
    month: int,
    job_id: str | None = None,
) -> dict:
    normalized_silver_uri = normalize_storage_uri(silver_uri)
    normalized_partition_uri = normalize_storage_uri(target_partition_uri)
    read_uri = parquet_read_uri(normalized_silver_uri)
    output_uri = partition_output_uri(normalized_partition_uri, normalized_silver_uri)
    ensure_local_parent(output_uri)

    from backend.services.operational_catalog_service import register_partition, update_job

    if job_id:
        update_job(
            job_id,
            status="running",
            progress_phase="monthly_summary",
            progress_current=0,
            progress_total=1,
        )

    conn = None
    try:
        import duckdb

        conn = duckdb.connect(database=":memory:")
        configure_duckdb_connection(conn, [read_uri, output_uri])
        sql = monthly_summary_writer_sql(
            silver_uri=normalized_silver_uri,
            target_partition_uri=normalized_partition_uri,
            source=source,
            year=year,
            month=month,
        )
        conn.execute(sql)
        summary_rows = count_summary_rows(conn, output_uri)
        size_bytes = storage_size_bytes(output_uri)
        partition = register_partition(
            dataset="summary_monthly",
            layer="gold",
            storage_uri=normalized_partition_uri,
            year=year,
            month=month,
            source=source,
            file_count=1,
            row_count=summary_rows,
            size_bytes=size_bytes,
            job_id=job_id,
        )
        result = {
            "status": "completed",
            "dataset": "summary_monthly",
            "layer": "gold",
            "silver_uri": normalized_silver_uri,
            "partition_uri": normalized_partition_uri,
            "output_uri": output_uri,
            "row_count": summary_rows,
            "size_bytes": size_bytes,
            "partition": partition,
        }
        if job_id:
            update_job(
                job_id,
                status="completed",
                result=result,
                progress_phase="completed",
                progress_current=1,
                progress_total=1,
            )
        return result
    except Exception as exc:
        if job_id:
            update_job(
                job_id,
                status="failed",
                error_message=str(exc),
                progress_phase="failed",
            )
        raise
    finally:
        if conn is not None:
            conn.close()


def count_summary_rows(conn, output_uri: str) -> int:
    row = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{escape_sql_string(output_uri)}', union_by_name = true)"
    ).fetchone()
    return int(row[0] or 0)
