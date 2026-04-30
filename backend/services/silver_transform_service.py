from __future__ import annotations

from backend.services.canonical_ticket_schema import CALC_COLUMNS, TICKET_COLUMNS, quote_identifier
from backend.services.ingestion_service import (
    configure_duckdb_connection,
    ensure_local_parent,
    escape_sql_string,
    is_local_uri,
    normalize_storage_uri,
    silver_output_uri,
    storage_size_bytes,
)


def silver_writer_sql(
    bronze_uri: str,
    target_partition_uri: str,
    available_columns: list[str],
    source: str = "unknown",
    year: int | None = None,
    month: int | None = None,
) -> str:
    normalized_bronze_uri = normalize_storage_uri(bronze_uri)
    normalized_target_uri = normalize_storage_uri(target_partition_uri)
    read_uri = parquet_read_uri(normalized_bronze_uri)
    output_uri = silver_output_uri(normalized_target_uri, normalized_bronze_uri)
    projection = silver_projection_sql(available_columns, source=source, year=year, month=month)

    return f"""
COPY (
    SELECT
                {projection}
    FROM read_parquet('{escape_sql_string(read_uri)}', hive_partitioning = true, union_by_name = true)
) TO '{escape_sql_string(output_uri)}'
  (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE);
""".strip()


def execute_silver_write(
    bronze_uri: str,
    target_partition_uri: str,
    source: str = "unknown",
    year: int | None = None,
    month: int | None = None,
    dataset: str = "tickets",
    job_id: str | None = None,
) -> dict:
    normalized_bronze_uri = normalize_storage_uri(bronze_uri)
    normalized_partition_uri = normalize_storage_uri(target_partition_uri)
    read_uri = parquet_read_uri(normalized_bronze_uri)
    output_uri = silver_output_uri(normalized_partition_uri, normalized_bronze_uri)
    ensure_local_parent(output_uri)

    from backend.services.operational_catalog_service import register_partition, update_job

    if job_id:
        update_job(
            job_id,
            status="running",
            progress_phase="silver_write",
            progress_current=0,
            progress_total=1,
        )

    conn = None
    try:
        import duckdb

        conn = duckdb.connect(database=":memory:")
        configure_duckdb_connection(conn, [read_uri, output_uri])
        available_columns = inspect_parquet_columns(conn, read_uri)
        row_count = count_parquet_rows(conn, read_uri)
        sql = silver_writer_sql(
            bronze_uri=normalized_bronze_uri,
            target_partition_uri=normalized_partition_uri,
            available_columns=available_columns,
            source=source,
            year=year,
            month=month,
        )
        conn.execute(sql)
        size_bytes = storage_size_bytes(output_uri)
        partition = register_partition(
            dataset=dataset,
            layer="silver",
            storage_uri=normalized_partition_uri,
            year=year,
            month=month,
            source=source,
            file_count=1,
            row_count=row_count,
            size_bytes=size_bytes,
            job_id=job_id,
        )
        result = {
            "status": "completed",
            "dataset": dataset,
            "layer": "silver",
            "bronze_uri": normalized_bronze_uri,
            "partition_uri": normalized_partition_uri,
            "output_uri": output_uri,
            "row_count": row_count,
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


def silver_projection_sql(
    available_columns: list[str],
    source: str = "unknown",
    year: int | None = None,
    month: int | None = None,
) -> str:
    available = _available_column_map(available_columns)
    occured = _timestamp("occured_time", available)
    created = _timestamp("created_at", available)
    cleared = _timestamp("cleared_time", available)
    take_over = _timestamp("take_over_date", available)

    ticket_parts = []
    for column in TICKET_COLUMNS:
        if column.logical_type == "timestamp":
            expr = _timestamp(column.name, available)
        else:
            expr = _varchar(column.name, available)
        ticket_parts.append(f"{expr} AS {quote_identifier(column.name)}")

    calc_parts = {
        "calc_response_time_min": _duration_minutes(take_over, created),
        "calc_repair_time_min": _duration_minutes(cleared, take_over),
        "calc_restore_time_min": _duration_minutes(cleared, occured),
        "calc_detection_time_min": _duration_minutes(created, occured),
        "calc_sla_duration_min": _duration_minutes(cleared, occured),
        "calc_sla_target_min": "CAST(NULL AS DOUBLE)",
        "calc_is_sla_met": (
            f"CASE WHEN {_varchar('sla_status', available)} IS NULL THEN NULL "
            f"ELSE UPPER(TRIM({_varchar('sla_status', available)})) = 'IN SLA' END"
        ),
        "calc_hour_of_day": _date_part("hour", occured),
        "calc_day_of_week": _date_part("dow", occured),
        "calc_week_of_month": (
            f"CASE WHEN {occured} IS NULL THEN NULL "
            f"ELSE CAST(FLOOR((EXTRACT(day FROM {occured}) - 1) / 7) + 1 AS INTEGER) END"
        ),
        "calc_month": _date_part("month", occured),
        "calc_year": _date_part("year", occured),
        "calc_year_month": _year_month_expr(occured, available, year, month),
        "calc_year_week": (
            f"CASE WHEN {occured} IS NULL THEN NULL ELSE strftime({occured}, '%Y-W%W') END"
        ),
        "calc_area_id": _trimmed_or_null("area", available),
        "calc_regional_id": _trimmed_or_null("regional", available),
        "calc_nop_id": _trimmed_or_null("nop", available),
        "calc_to_id": _trimmed_or_null("cluster_to", available),
        "calc_source": _source_expr(available, source),
    }

    calc_projection = [f"{calc_parts[column.name]} AS {quote_identifier(column.name)}" for column in CALC_COLUMNS]
    return ",\n                ".join(ticket_parts + calc_projection)


def parquet_read_uri(storage_uri: str) -> str:
    normalized = normalize_storage_uri(storage_uri).rstrip("/\\")
    if normalized.lower().endswith(".parquet"):
        return normalized
    separator = "/" if is_local_uri(normalized) else "/"
    return f"{normalized}{separator}*.parquet"


def inspect_parquet_columns(conn, read_uri: str) -> list[str]:
    rows = conn.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{escape_sql_string(read_uri)}', hive_partitioning = true, union_by_name = true)"
    ).fetchall()
    return [str(row[0]) for row in rows]


def count_parquet_rows(conn, read_uri: str) -> int:
    row = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{escape_sql_string(read_uri)}', hive_partitioning = true, union_by_name = true)"
    ).fetchone()
    return int(row[0] or 0)


def _available_column_map(columns: list[str]) -> dict[str, str]:
    return {str(column).lower(): str(column) for column in columns}


def _column_ref(name: str, available: dict[str, str]) -> str | None:
    actual = available.get(name.lower())
    if not actual:
        return None
    return quote_identifier(actual)


def _varchar(name: str, available: dict[str, str]) -> str:
    ref = _column_ref(name, available)
    if not ref:
        return "CAST(NULL AS VARCHAR)"
    return f"CAST({ref} AS VARCHAR)"


def _timestamp(name: str, available: dict[str, str]) -> str:
    ref = _column_ref(name, available)
    if not ref:
        return "CAST(NULL AS TIMESTAMP)"
    return f"try_cast({ref} AS TIMESTAMP)"


def _integer(name: str, available: dict[str, str]) -> str:
    ref = _column_ref(name, available)
    if not ref:
        return "CAST(NULL AS INTEGER)"
    return f"try_cast({ref} AS INTEGER)"


def _duration_minutes(end_expr: str, start_expr: str) -> str:
    return (
        f"CASE WHEN {end_expr} IS NULL OR {start_expr} IS NULL THEN NULL "
        f"ELSE GREATEST(0, EXTRACT(EPOCH FROM ({end_expr} - {start_expr})) / 60.0) END"
    )


def _date_part(part: str, timestamp_expr: str) -> str:
    return (
        f"CASE WHEN {timestamp_expr} IS NULL THEN NULL "
        f"ELSE CAST(EXTRACT({part} FROM {timestamp_expr}) AS INTEGER) END"
    )


def _trimmed_or_null(name: str, available: dict[str, str]) -> str:
    value = _varchar(name, available)
    return f"NULLIF(TRIM({value}), '')"


def _source_expr(available: dict[str, str], fallback_source: str) -> str:
    return f"COALESCE(NULLIF(TRIM({_varchar('source', available)}), ''), '{escape_sql_string(fallback_source)}')"


def _year_month_expr(
    occured_expr: str,
    available: dict[str, str],
    year: int | None,
    month: int | None,
) -> str:
    if year is not None and month is not None:
        fallback = f"'{int(year):04d}-{int(month):02d}'"
    else:
        fallback = (
            f"CASE WHEN {_integer('year', available)} IS NULL OR {_integer('month', available)} IS NULL THEN NULL "
            f"ELSE printf('%04d-%02d', {_integer('year', available)}, {_integer('month', available)}) END"
        )
    return f"COALESCE(CASE WHEN {occured_expr} IS NULL THEN NULL ELSE strftime({occured_expr}, '%Y-%m') END, {fallback})"
