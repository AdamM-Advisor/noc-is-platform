from __future__ import annotations

import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path

import backend.config as config
import backend.services.parquet_lake_service as lake
from backend.services.canonical_ticket_schema import normalize_column_name, validate_ticket_columns
from backend.services.file_detector import detect_file_type
from backend.services.ingestion_service import (
    SUPPORTED_SOURCES,
    configure_duckdb_connection,
    escape_sql_string,
    execute_bronze_write,
    format_year_month,
    infer_year_month,
    normalize_token,
    partition_uri,
    sha256_file,
    storage_size_bytes,
)
from backend.services.operational_catalog_service import create_job, register_file, update_job
from backend.services.silver_transform_service import execute_silver_write
from backend.services.summary_cache_service import refresh_duckdb_summaries_from_silver
from backend.services.summary_lake_service import execute_monthly_summary_refresh, monthly_summary_partition_uri


RAW_EXTENSIONS = {".csv", ".xlsx", ".xls", ".parquet"}
TICKET_FILE_TYPES = {"swfm_realtime", "swfm_event", "swfm_incident", "fault_center", "ticket"}


@dataclass(frozen=True)
class RawFileMetadata:
    file_type: str
    source: str
    header_format: str
    columns: list[str]
    row_count: int | None = None


def process_raw_ticket_file(
    raw_file_path: str,
    source: str = "auto",
    year: int | None = None,
    month: int | None = None,
    filename: str | None = None,
    job_id: str | None = None,
    refresh_summary_cache: bool = True,
) -> dict:
    """Materialize Bronze, Silver, and summary Parquet from a RAW ticket file."""
    start_time = time.time()
    raw_path = Path(raw_file_path)
    if not raw_path.is_file():
        raise FileNotFoundError(str(raw_path))
    _validate_extension(raw_path)
    raw_checksum = sha256_file(raw_path)
    raw_size_bytes = raw_path.stat().st_size

    from backend.services.schema_service import initialize_schema

    initialize_schema()
    managed_job_id = job_id
    if managed_job_id is None:
        managed_job_id = create_job(
            "import_raw_to_parquet",
            payload={"raw_file_path": str(raw_path), "source": source, "year": year, "month": month},
            source=None if source == "auto" else source,
        )["job_id"]

    _progress(managed_job_id, "running", "detecting", 0, 6)
    metadata = detect_raw_file_metadata(raw_path, source=source)
    if metadata.file_type == "site_master":
        raise ValueError("Site master masih diproses melalui jalur master-data, bukan ticket Parquet pipeline.")
    if metadata.file_type not in TICKET_FILE_TYPES:
        raise ValueError(f"Tipe file tidak dikenali sebagai tiket NOC: {metadata.file_type}")

    _progress(managed_job_id, "running", "raw_to_parquet", 1, 6)
    staging_uri = _materialize_staging_parquet(raw_path)
    columns, row_count = _inspect_parquet(staging_uri)

    inferred_year, inferred_month = infer_year_month(str(raw_path))
    target_year = year or inferred_year
    target_month = month or inferred_month
    if target_year is None or target_month is None:
        detected_period = _infer_period_from_parquet(staging_uri, columns)
        if detected_period:
            target_year, target_month = detected_period
    if target_year is None or target_month is None:
        raise ValueError("Periode tidak terdeteksi. Gunakan nama file YYYY-MM atau kolom yearmonth/occured_time.")

    period = format_year_month(target_year, target_month)
    archived_raw_uri = None
    raw_catalog_uri = _raw_catalog_uri(raw_path, filename)
    raw_catalog_status = "raw_not_archived"
    if config.ARCHIVE_RAW_FILES:
        archived_raw_uri = _archive_raw_file(raw_path, metadata.source, target_year, target_month)
        raw_catalog_uri = archived_raw_uri
        raw_catalog_status = "archived_raw"
    source_parquet_uri = _move_staging_to_source_parquet(
        staging_uri,
        raw_path,
        metadata.source,
        target_year,
        target_month,
    )

    register_file(
        storage_uri=raw_catalog_uri,
        filename=filename or raw_path.name,
        file_type=f"{metadata.file_type}_raw",
        source=metadata.source,
        checksum_sha256=raw_checksum,
        size_bytes=raw_size_bytes,
        row_count=row_count,
        period_min=period,
        period_max=period,
        status=raw_catalog_status,
        job_id=managed_job_id,
    )
    register_file(
        storage_uri=source_parquet_uri,
        filename=Path(source_parquet_uri).name,
        file_type="ticket",
        source=metadata.source,
        checksum_sha256=sha256_file(Path(source_parquet_uri)),
        size_bytes=Path(source_parquet_uri).stat().st_size,
        row_count=row_count,
        period_min=period,
        period_max=period,
        status="converted_parquet",
        job_id=managed_job_id,
    )

    validation = validate_ticket_columns(columns)
    if not validation["valid"]:
        raise ValueError(f"Kolom wajib tiket belum lengkap: {validation['missing_required']}")

    _progress(managed_job_id, "running", "bronze_write", 2, 6)
    bronze_partition = partition_uri("tickets", "bronze", metadata.source, target_year, target_month)
    bronze = execute_bronze_write(
        source_uri=source_parquet_uri,
        target_partition_uri=bronze_partition,
        raw_columns=columns,
        source=metadata.source,
        year=target_year,
        month=target_month,
        dataset="tickets",
        job_id=managed_job_id,
    )

    _progress(managed_job_id, "running", "silver_write", 3, 6)
    silver_partition = partition_uri("tickets", "silver", metadata.source, target_year, target_month)
    silver = execute_silver_write(
        bronze_uri=bronze["partition_uri"],
        target_partition_uri=silver_partition,
        source=metadata.source,
        year=target_year,
        month=target_month,
        dataset="tickets",
        job_id=managed_job_id,
    )

    _progress(managed_job_id, "running", "monthly_summary", 4, 6)
    gold = execute_monthly_summary_refresh(
        silver_uri=silver["partition_uri"],
        target_partition_uri=monthly_summary_partition_uri(metadata.source, target_year, target_month),
        source=metadata.source,
        year=target_year,
        month=target_month,
        job_id=managed_job_id,
    )

    summary_cache = None
    if refresh_summary_cache:
        _progress(managed_job_id, "running", "summary_cache", 5, 6)
        summary_cache = refresh_duckdb_summaries_from_silver(target_year, target_month)

    duration = round(time.time() - start_time, 2)
    result = {
        "status": "completed",
        "pipeline": "raw_to_parquet",
        "file_type": metadata.file_type,
        "source": metadata.source,
        "period": period,
        "year": target_year,
        "month": target_month,
        "total": row_count,
        "imported": row_count,
        "skipped": 0,
        "errors": 0,
        "duration_sec": duration,
        "raw_uri": raw_catalog_uri,
        "raw_archived": config.ARCHIVE_RAW_FILES,
        "upload_deleted": False,
        "source_parquet_uri": source_parquet_uri,
        "bronze": bronze,
        "silver": silver,
        "gold": gold,
        "summary_cache": summary_cache,
        "validation": validation,
    }
    _log_import(raw_path, metadata.file_type, period, row_count, duration)
    if _delete_upload_after_success(raw_path):
        raw_path.unlink(missing_ok=True)
        result["upload_deleted"] = True
    update_job(
        managed_job_id,
        status="completed",
        result=_compact_result(result),
        progress_phase="completed",
        progress_current=6,
        progress_total=6,
    )
    return result


def import_raw_folder(
    raw_root: str,
    source: str = "auto",
    year: int | None = None,
    month: int | None = None,
    recursive: bool = True,
    limit: int | None = None,
) -> dict:
    root = Path(raw_root)
    if not root.is_dir():
        raise NotADirectoryError(str(root))
    pattern = "**/*" if recursive else "*"
    candidates = [path for path in sorted(root.glob(pattern)) if path.is_file() and path.suffix.lower() in RAW_EXTENSIONS]
    if limit is not None:
        candidates = candidates[: max(0, limit)]

    results = []
    failures = []
    for path in candidates:
        try:
            results.append(process_raw_ticket_file(str(path), source=source, year=year, month=month))
        except Exception as exc:
            failures.append({"path": str(path), "error": str(exc)})

    return {
        "status": "completed" if not failures else "completed_with_errors",
        "raw_root": str(root),
        "files_found": len(candidates),
        "processed": len(results),
        "failed": len(failures),
        "results": [_compact_result(result) for result in results],
        "failures": failures,
    }


def detect_raw_file_metadata(raw_file_path: str | Path, source: str = "auto") -> RawFileMetadata:
    raw_path = Path(raw_file_path)
    _validate_extension(raw_path)
    columns = _inspect_raw_columns(raw_path)
    detection = detect_file_type(raw_path.name, columns)
    detected_type = detection.get("file_type") or "unknown"
    if source and source != "auto":
        detected_source = _normalize_source(source)
        if detected_type == "unknown":
            detected_type = "ticket"
    elif detected_type in SUPPORTED_SOURCES:
        detected_source = detected_type
    else:
        detected_source = "manual"
        if detected_type == "unknown" and validate_ticket_columns(columns)["valid"]:
            detected_type = "ticket"
    return RawFileMetadata(
        file_type=detected_type,
        source=detected_source,
        header_format=detection.get("header_format") or "unknown",
        columns=columns,
    )


def _materialize_staging_parquet(raw_path: Path) -> str:
    digest = sha256_file(raw_path)[:16]
    staging_dir = Path(str(lake.LAKE_ROOT)) / "_staging" / "raw_converted"
    staging_dir.mkdir(parents=True, exist_ok=True)
    staging_path = staging_dir / f"{raw_path.stem}-{digest}.parquet"
    if staging_path.exists():
        staging_path.unlink()

    ext = raw_path.suffix.lower()
    if ext == ".parquet":
        shutil.copy2(raw_path, staging_path)
    elif ext == ".csv":
        _csv_to_parquet(raw_path, staging_path)
    elif ext in {".xlsx", ".xls"}:
        _excel_to_parquet(raw_path, staging_path)
    else:
        raise ValueError(f"Unsupported raw extension: {ext}")
    return str(staging_path)


def _csv_to_parquet(raw_path: Path, target_path: Path) -> None:
    import duckdb

    conn = duckdb.connect(database=":memory:")
    try:
        configure_duckdb_connection(conn, [str(raw_path), str(target_path)])
        conn.execute(
            f"""
            COPY (
                SELECT *
                FROM read_csv_auto(
                    '{escape_sql_string(str(raw_path))}',
                    all_varchar = true,
                    ignore_errors = false
                )
            ) TO '{escape_sql_string(str(target_path))}'
              (FORMAT PARQUET, COMPRESSION ZSTD);
            """
        )
    finally:
        conn.close()


def _excel_to_parquet(raw_path: Path, target_path: Path) -> None:
    import pandas as pd

    df = pd.read_excel(raw_path, engine="openpyxl", dtype=str)
    df.to_parquet(target_path, index=False)


def _inspect_parquet(parquet_uri: str) -> tuple[list[str], int]:
    import pyarrow.parquet as pq

    parquet_file = pq.ParquetFile(parquet_uri)
    columns = [field.name for field in parquet_file.schema_arrow]
    row_count = parquet_file.metadata.num_rows if parquet_file.metadata else 0
    return columns, int(row_count or 0)


def _inspect_raw_columns(raw_path: Path) -> list[str]:
    ext = raw_path.suffix.lower()
    if ext == ".parquet":
        return _inspect_parquet(str(raw_path))[0]
    if ext == ".csv":
        import pandas as pd

        return list(pd.read_csv(raw_path, nrows=5).columns)
    if ext in {".xlsx", ".xls"}:
        import pandas as pd

        return list(pd.read_excel(raw_path, engine="openpyxl", nrows=5).columns)
    raise ValueError(f"Unsupported raw extension: {ext}")


def _infer_period_from_parquet(parquet_uri: str, columns: list[str]) -> tuple[int, int] | None:
    normalized = {normalize_column_name(column): column for column in columns}
    for logical in ("calc_year_month", "year_month", "yearmonth"):
        actual = normalized.get(logical)
        if actual:
            value = _first_non_null(parquet_uri, actual)
            parsed = _parse_year_month_value(value)
            if parsed:
                return parsed

    for logical in ("occured_time", "created_at", "ticket_creation"):
        actual = normalized.get(logical)
        if actual:
            value = _min_timestamp(parquet_uri, actual)
            parsed = _parse_year_month_value(value)
            if parsed:
                return parsed
    return None


def _first_non_null(parquet_uri: str, column: str):
    import duckdb

    conn = duckdb.connect(database=":memory:")
    try:
        row = conn.execute(
            f"""
            SELECT { _quote_identifier(column) }
            FROM read_parquet('{escape_sql_string(parquet_uri)}', union_by_name = true)
            WHERE { _quote_identifier(column) } IS NOT NULL
            LIMIT 1
            """
        ).fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def _min_timestamp(parquet_uri: str, column: str):
    import duckdb

    conn = duckdb.connect(database=":memory:")
    try:
        row = conn.execute(
            f"""
            SELECT MIN(try_cast({ _quote_identifier(column) } AS TIMESTAMP))
            FROM read_parquet('{escape_sql_string(parquet_uri)}', union_by_name = true)
            """
        ).fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def _parse_year_month_value(value) -> tuple[int, int] | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) >= 7 and text[4] in {"-", "/"}:
        year = _safe_int(text[:4])
        month = _safe_int(text[5:7])
        if _valid_period(year, month):
            return year, month
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 6:
        year = _safe_int(digits[:4])
        month = _safe_int(digits[4:6])
        if _valid_period(year, month):
            return year, month
    return None


def _archive_raw_file(raw_path: Path, source: str, year: int, month: int) -> str:
    digest = sha256_file(raw_path)[:12]
    target_dir = Path(config.RAW_DIR) / "tickets" / f"year={year:04d}" / f"month={month:02d}" / f"source={source}"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / f"{raw_path.stem}-{digest}{raw_path.suffix.lower()}"
    shutil.copy2(raw_path, target_path)
    return str(target_path)


def _raw_catalog_uri(raw_path: Path, filename: str | None = None) -> str:
    if _delete_upload_after_success(raw_path):
        return f"raw://not-archived/{filename or raw_path.name}"
    return str(raw_path.resolve())


def _move_staging_to_source_parquet(staging_uri: str, raw_path: Path, source: str, year: int, month: int) -> str:
    digest = sha256_file(raw_path)[:12]
    target_dir = Path(str(lake.LAKE_ROOT)) / "raw_converted" / "tickets" / f"year={year:04d}" / f"month={month:02d}" / f"source={source}"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / f"{raw_path.stem}-{digest}.parquet"
    if target_path.exists():
        target_path.unlink()
    shutil.move(staging_uri, target_path)
    return str(target_path)


def _log_import(raw_path: Path, file_type: str, period: str, row_count: int, duration: float) -> None:
    from backend.database import get_write_connection

    file_size_mb = round(storage_size_bytes(str(raw_path)) / (1024 * 1024), 2)
    with get_write_connection() as conn:
        max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM import_logs").fetchone()[0]
        conn.execute(
            """
            INSERT INTO import_logs (
                id, filename, file_type, file_size_mb, period,
                rows_total, rows_imported, rows_skipped, rows_error,
                orphan_count, processing_time_sec, status, backup_created
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, 0, ?, 'completed', FALSE)
            """,
            [
                max_id + 1,
                raw_path.name,
                file_type,
                file_size_mb,
                period,
                row_count,
                row_count,
                round(duration, 2),
            ],
        )


def _compact_result(result: dict) -> dict:
    return {
        "status": result.get("status"),
        "pipeline": result.get("pipeline"),
        "file_type": result.get("file_type"),
        "source": result.get("source"),
        "period": result.get("period"),
        "total": result.get("total"),
        "imported": result.get("imported"),
        "duration_sec": result.get("duration_sec"),
        "raw_uri": result.get("raw_uri"),
        "raw_archived": result.get("raw_archived"),
        "upload_deleted": result.get("upload_deleted"),
        "source_parquet_uri": result.get("source_parquet_uri"),
        "bronze_output_uri": (result.get("bronze") or {}).get("output_uri"),
        "silver_output_uri": (result.get("silver") or {}).get("output_uri"),
        "gold_output_uri": (result.get("gold") or {}).get("output_uri"),
        "summary_cache": result.get("summary_cache"),
    }


def _progress(job_id: str | None, status: str, phase: str, current: int, total: int) -> None:
    if not job_id:
        return
    update_job(
        job_id,
        status=status,
        progress_phase=phase,
        progress_current=current,
        progress_total=total,
    )


def _normalize_source(source: str) -> str:
    return normalize_token(source, SUPPORTED_SOURCES, "source")


def _validate_extension(path: Path) -> None:
    if path.suffix.lower() not in RAW_EXTENSIONS:
        allowed = ", ".join(sorted(RAW_EXTENSIONS))
        raise ValueError(f"Unsupported raw extension: {path.suffix}. Allowed: {allowed}")


def _delete_upload_after_success(raw_path: Path) -> bool:
    return config.DELETE_UPLOAD_AFTER_PROCESS and _is_relative_to(raw_path, Path(config.UPLOAD_DIR))


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def _quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _safe_int(value: str) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _valid_period(year: int | None, month: int | None) -> bool:
    return year is not None and month is not None and year >= 2000 and 1 <= month <= 12
