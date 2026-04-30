from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from backend.services.canonical_ticket_schema import select_bronze_columns_sql, validate_ticket_columns
from backend.services.parquet_lake_service import LAKE_ROOT
from backend.config import DUCKDB_MEMORY_LIMIT, DUCKDB_THREADS


SUPPORTED_FILE_TYPES = {"ticket", "site_master", "external_weather", "external_pln"}
SUPPORTED_SOURCES = {
    "inap",
    "swfm_event",
    "swfm_incident",
    "swfm_realtime",
    "swfm_history",
    "fault_center",
    "manual",
    "external",
    "unknown",
}


@dataclass(frozen=True)
class IngestionPlan:
    job: dict
    file: dict
    bronze_partition_uri: str
    silver_partition_uri: str
    dataset: str
    source: str
    year: int | None
    month: int | None


def create_ingestion_plan(
    storage_uri: str,
    file_type: str = "ticket",
    source: str = "unknown",
    year: int | None = None,
    month: int | None = None,
    filename: str | None = None,
    checksum_sha256: str | None = None,
    size_bytes: int | None = None,
) -> IngestionPlan:
    normalized_uri = normalize_storage_uri(storage_uri)
    normalized_type = normalize_token(file_type, SUPPORTED_FILE_TYPES, "file_type")
    normalized_source = normalize_token(source, SUPPORTED_SOURCES, "source")
    inferred_filename = filename or infer_filename(normalized_uri)
    inferred_year, inferred_month = infer_year_month(normalized_uri)
    target_year = year if year is not None else inferred_year
    target_month = month if month is not None else inferred_month

    if target_year is not None and target_year < 2000:
        raise ValueError("year must be >= 2000")
    if target_month is not None and not 1 <= target_month <= 12:
        raise ValueError("month must be between 1 and 12")

    if is_local_uri(normalized_uri):
        local_path = Path(normalized_uri)
        if not local_path.is_file():
            raise FileNotFoundError(str(local_path))
        checksum_sha256 = checksum_sha256 or sha256_file(local_path)
        size_bytes = size_bytes if size_bytes is not None else local_path.stat().st_size

    from backend.services.operational_catalog_service import create_job, register_file

    job = create_job(
        "ingest_parquet",
        payload={
            "storage_uri": normalized_uri,
            "file_type": normalized_type,
            "source": normalized_source,
            "year": target_year,
            "month": target_month,
        },
        source=normalized_source,
    )
    file_record = register_file(
        storage_uri=normalized_uri,
        filename=inferred_filename,
        file_type=normalized_type,
        source=normalized_source,
        checksum_sha256=checksum_sha256,
        size_bytes=size_bytes,
        period_min=format_year_month(target_year, target_month),
        period_max=format_year_month(target_year, target_month),
        status="registered",
        job_id=job["job_id"],
    )

    dataset = dataset_for_file_type(normalized_type)
    return IngestionPlan(
        job=job,
        file=file_record,
        bronze_partition_uri=partition_uri(dataset, "bronze", normalized_source, target_year, target_month),
        silver_partition_uri=partition_uri(dataset, "silver", normalized_source, target_year, target_month),
        dataset=dataset,
        source=normalized_source,
        year=target_year,
        month=target_month,
    )


def inspect_parquet_metadata(storage_uri: str) -> dict:
    normalized_uri = normalize_storage_uri(storage_uri)
    if not normalized_uri.lower().endswith(".parquet"):
        raise ValueError("Only .parquet files are supported for metadata inspection")

    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for Parquet metadata inspection") from exc

    parquet_file = pq.ParquetFile(normalized_uri)
    schema = parquet_file.schema_arrow
    columns = [field.name for field in schema]
    row_count = parquet_file.metadata.num_rows if parquet_file.metadata else None
    row_groups = parquet_file.metadata.num_row_groups if parquet_file.metadata else None

    return {
        "storage_uri": normalized_uri,
        "columns": columns,
        "column_count": len(columns),
        "row_count": row_count,
        "row_groups": row_groups,
        "schema": [
            {"name": field.name, "type": str(field.type), "nullable": field.nullable}
            for field in schema
        ],
    }


def validate_parquet_ticket_schema(storage_uri: str) -> dict:
    metadata = inspect_parquet_metadata(storage_uri)
    validation = validate_ticket_columns(metadata["columns"])
    return {
        "storage_uri": metadata["storage_uri"],
        "row_count": metadata["row_count"],
        "row_groups": metadata["row_groups"],
        "column_count": metadata["column_count"],
        "validation": validation,
    }


def bronze_writer_sql(
    source_uri: str,
    target_partition_uri: str,
    raw_columns: list[str],
    source: str,
    year: int | None = None,
    month: int | None = None,
) -> str:
    normalized_source_uri = normalize_storage_uri(source_uri)
    normalized_target_uri = bronze_output_uri(target_partition_uri, normalized_source_uri)
    select_columns = select_bronze_columns_sql(raw_columns)
    partition_literals = []
    if year is not None:
        partition_literals.append(f"{int(year)} AS year")
    if month is not None:
        partition_literals.append(f"{int(month)} AS month")
    partition_literals.append(f"'{escape_sql_string(source)}' AS source")
    extra_columns = ",\n                ".join(partition_literals)

    return f"""
COPY (
    SELECT
                {select_columns},
                {extra_columns}
    FROM read_parquet('{escape_sql_string(normalized_source_uri)}', union_by_name = true)
) TO '{escape_sql_string(normalized_target_uri)}'
  (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE);
""".strip()


def execute_bronze_write(
    source_uri: str,
    target_partition_uri: str,
    raw_columns: list[str],
    source: str,
    year: int | None = None,
    month: int | None = None,
    dataset: str = "tickets",
    job_id: str | None = None,
) -> dict:
    normalized_source_uri = normalize_storage_uri(source_uri)
    normalized_partition_uri = normalize_storage_uri(target_partition_uri)
    output_uri = bronze_output_uri(normalized_partition_uri, normalized_source_uri)
    ensure_local_parent(output_uri)

    sql = bronze_writer_sql(
        source_uri=normalized_source_uri,
        target_partition_uri=normalized_partition_uri,
        raw_columns=raw_columns,
        source=source,
        year=year,
        month=month,
    )

    from backend.services.operational_catalog_service import register_partition, update_job

    if job_id:
        update_job(
            job_id,
            status="running",
            progress_phase="bronze_write",
            progress_current=0,
            progress_total=1,
        )

    conn = None
    try:
        import duckdb

        conn = duckdb.connect(database=":memory:")
        configure_duckdb_connection(conn, [normalized_source_uri, output_uri])
        row_count = count_source_rows(conn, normalized_source_uri)
        conn.execute(sql)
        size_bytes = storage_size_bytes(output_uri)
        partition = register_partition(
            dataset=dataset,
            layer="bronze",
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
            "layer": "bronze",
            "source_uri": normalized_source_uri,
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


def bronze_output_uri(target_partition_uri: str, source_uri: str) -> str:
    return partition_output_uri(target_partition_uri, source_uri)


def silver_output_uri(target_partition_uri: str, source_uri: str) -> str:
    return partition_output_uri(target_partition_uri, source_uri)


def partition_output_uri(target_partition_uri: str, source_uri: str) -> str:
    normalized_target_uri = normalize_storage_uri(target_partition_uri).rstrip("/\\")
    if normalized_target_uri.lower().endswith(".parquet"):
        return normalized_target_uri

    normalized_source_uri = normalize_storage_uri(source_uri)
    digest = hashlib.sha256(f"{normalized_source_uri}|{normalized_target_uri}".encode("utf-8")).hexdigest()[:16]
    return f"{normalized_target_uri}/part-{digest}.parquet"


def ensure_local_parent(storage_uri: str) -> None:
    if is_local_uri(storage_uri):
        Path(storage_uri).parent.mkdir(parents=True, exist_ok=True)


def configure_duckdb_connection(conn, storage_uris: list[str]) -> None:
    conn.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    conn.execute(f"SET threads={DUCKDB_THREADS}")
    conn.execute("SET preserve_insertion_order=false")

    if any(urlparse(uri).scheme in {"gs", "s3", "r2", "http", "https"} for uri in storage_uris):
        if os.environ.get("NOCIS_DUCKDB_INSTALL_EXTENSIONS", "0").lower() in {"1", "true", "yes"}:
            conn.execute("INSTALL httpfs")
        try:
            conn.execute("LOAD httpfs")
        except Exception as exc:
            raise RuntimeError(
                "DuckDB httpfs extension is required for remote object storage. "
                "Set NOCIS_DUCKDB_INSTALL_EXTENSIONS=1 in build/job environments if the extension is not bundled."
            ) from exc


def count_source_rows(conn, source_uri: str) -> int:
    row = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{escape_sql_string(source_uri)}', union_by_name = true)"
    ).fetchone()
    return int(row[0] or 0)


def storage_size_bytes(storage_uri: str) -> int:
    if is_local_uri(storage_uri) and Path(storage_uri).is_file():
        return Path(storage_uri).stat().st_size
    return 0


def normalize_storage_uri(storage_uri: str) -> str:
    if not storage_uri or not storage_uri.strip():
        raise ValueError("storage_uri is required")

    uri = storage_uri.strip()
    parsed = urlparse(uri)
    if parsed.scheme in {"gs", "s3", "r2"}:
        if not parsed.netloc or not parsed.path:
            raise ValueError(f"Invalid object storage URI: {storage_uri}")
        return uri.rstrip("/")
    if parsed.scheme in {"http", "https"}:
        raise ValueError("Use object storage URIs such as gs://... for remote files")
    if parsed.scheme and len(parsed.scheme) > 1:
        raise ValueError(f"Unsupported URI scheme: {parsed.scheme}")

    return str(Path(uri).resolve())


def normalize_token(value: str, allowed: set[str], field_name: str) -> str:
    token = re.sub(r"[^a-zA-Z0-9_]+", "_", (value or "").strip().lower()).strip("_")
    if not token:
        raise ValueError(f"{field_name} is required")
    if token not in allowed:
        allowed_list = ", ".join(sorted(allowed))
        raise ValueError(f"Unsupported {field_name}: {value}. Allowed: {allowed_list}")
    return token


def infer_filename(storage_uri: str) -> str:
    parsed = urlparse(storage_uri)
    if parsed.scheme in {"gs", "s3", "r2"}:
        name = Path(parsed.path).name
    else:
        name = Path(storage_uri).name
    return name or "unknown"


def infer_year_month(storage_uri: str) -> tuple[int | None, int | None]:
    text = storage_uri.replace("\\", "/")
    year = None
    month = None

    year_match = re.search(r"(?:year=|/)(20\d{2})(?:/|[-_])", text)
    if year_match:
        year = int(year_match.group(1))

    month_match = re.search(r"(?:month=|/)(0?[1-9]|1[0-2])(?:/|[-_])", text)
    if month_match:
        month = int(month_match.group(1))

    ym_match = re.search(r"(20\d{2})[-_](0[1-9]|1[0-2])", text)
    if ym_match:
        year = int(ym_match.group(1))
        month = int(ym_match.group(2))

    return year, month


def dataset_for_file_type(file_type: str) -> str:
    if file_type == "ticket":
        return "tickets"
    if file_type == "site_master":
        return "site_master"
    return "external"


def partition_uri(dataset: str, layer: str, source: str, year: int | None, month: int | None) -> str:
    parts = [str(LAKE_ROOT).rstrip("/\\"), dataset, layer]
    if year is not None:
        parts.append(f"year={year}")
    if month is not None:
        parts.append(f"month={month:02d}")
    parts.append(f"source={source}")
    return "/".join(part.strip("/\\") for part in parts if part)


def format_year_month(year: int | None, month: int | None) -> str | None:
    if year is None or month is None:
        return None
    return f"{year}-{month:02d}"


def is_local_uri(storage_uri: str) -> bool:
    return "://" not in storage_uri


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def escape_sql_string(value: str) -> str:
    return str(value).replace("'", "''").replace("\\", "/")
