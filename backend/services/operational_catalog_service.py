import json
import threading
import uuid
from datetime import date, datetime
from typing import Any

from backend.database import get_connection, get_write_connection


JOB_STATUSES = {"queued", "running", "completed", "failed", "cancelled"}
_catalog_lock = threading.Lock()
_initialized_db_path: str | None = None


DDL = [
    """
    CREATE TABLE IF NOT EXISTS operational_jobs (
        job_id VARCHAR PRIMARY KEY,
        job_type VARCHAR NOT NULL,
        status VARCHAR NOT NULL DEFAULT 'queued',
        source VARCHAR,
        payload_json TEXT,
        result_json TEXT,
        error_message TEXT,
        progress_phase VARCHAR,
        progress_current BIGINT DEFAULT 0,
        progress_total BIGINT DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        queued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        started_at TIMESTAMP,
        finished_at TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS file_catalog (
        file_id VARCHAR PRIMARY KEY,
        storage_uri VARCHAR NOT NULL,
        filename VARCHAR,
        file_type VARCHAR,
        source VARCHAR,
        checksum_sha256 VARCHAR,
        size_bytes BIGINT,
        row_count BIGINT,
        period_min VARCHAR,
        period_max VARCHAR,
        status VARCHAR NOT NULL DEFAULT 'registered',
        job_id VARCHAR,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS lake_partitions (
        partition_id VARCHAR PRIMARY KEY,
        dataset VARCHAR NOT NULL,
        layer VARCHAR NOT NULL,
        year INTEGER,
        month INTEGER,
        source VARCHAR,
        storage_uri VARCHAR NOT NULL,
        file_count BIGINT DEFAULT 0,
        row_count BIGINT DEFAULT 0,
        size_bytes BIGINT DEFAULT 0,
        checksum_sha256 VARCHAR,
        job_id VARCHAR,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS model_run_catalog (
        model_run_id VARCHAR PRIMARY KEY,
        model_name VARCHAR NOT NULL,
        model_version VARCHAR NOT NULL,
        entity_level VARCHAR,
        entity_id VARCHAR,
        window_start VARCHAR,
        window_end VARCHAR,
        parameters_json TEXT,
        metrics_json TEXT,
        status VARCHAR NOT NULL DEFAULT 'registered',
        job_id VARCHAR,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
]


def initialize_operational_catalog(force: bool = False) -> dict:
    global _initialized_db_path

    from backend import database as database_module

    current_db_path = database_module.DB_PATH
    if not force and _initialized_db_path == current_db_path:
        return {"status": "ok", "tables": ["operational_jobs", "file_catalog", "lake_partitions", "model_run_catalog"]}

    with _catalog_lock:
        if not force and _initialized_db_path == current_db_path:
            return {"status": "ok", "tables": ["operational_jobs", "file_catalog", "lake_partitions", "model_run_catalog"]}
        with get_write_connection() as conn:
            for ddl in DDL:
                conn.execute(ddl)
        _initialized_db_path = current_db_path
    return {"status": "ok", "tables": ["operational_jobs", "file_catalog", "lake_partitions", "model_run_catalog"]}


def create_job(job_type: str, payload: dict | None = None, source: str | None = None) -> dict:
    initialize_operational_catalog()
    job_id = _new_id("job")
    with get_write_connection() as conn:
        conn.execute(
            """
            INSERT INTO operational_jobs (
                job_id, job_type, status, source, payload_json,
                progress_phase, progress_current, progress_total
            ) VALUES (?, ?, 'queued', ?, ?, 'queued', 0, 0)
            """,
            [job_id, job_type, source, _json(payload or {})],
        )
    return get_job(job_id)


def list_jobs(status: str | None = None, job_type: str | None = None, limit: int = 50) -> list[dict]:
    initialize_operational_catalog()
    conditions = []
    params: list[Any] = []
    if status:
        conditions.append("status = ?")
        params.append(status)
    if job_type:
        conditions.append("job_type = ?")
        params.append(job_type)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    params.append(max(1, min(limit, 500)))
    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT job_id, job_type, status, source, payload_json, result_json,
                   error_message, progress_phase, progress_current, progress_total,
                   created_at, started_at, finished_at, updated_at
            FROM operational_jobs
            {where}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    return [_job_row(row) for row in rows]


def get_job(job_id: str) -> dict:
    initialize_operational_catalog()
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT job_id, job_type, status, source, payload_json, result_json,
                   error_message, progress_phase, progress_current, progress_total,
                   created_at, started_at, finished_at, updated_at
            FROM operational_jobs
            WHERE job_id = ?
            """,
            [job_id],
        ).fetchone()
    if not row:
        raise KeyError(job_id)
    return _job_row(row)


def update_job(
    job_id: str,
    status: str | None = None,
    result: dict | None = None,
    error_message: str | None = None,
    progress_phase: str | None = None,
    progress_current: int | None = None,
    progress_total: int | None = None,
) -> dict:
    initialize_operational_catalog()
    updates = ["updated_at = CURRENT_TIMESTAMP"]
    params: list[Any] = []

    if status:
        if status not in JOB_STATUSES:
            raise ValueError(f"Unsupported job status: {status}")
        updates.append("status = ?")
        params.append(status)
        if status == "running":
            updates.append("started_at = COALESCE(started_at, CURRENT_TIMESTAMP)")
        if status in {"completed", "failed", "cancelled"}:
            updates.append("finished_at = CURRENT_TIMESTAMP")

    if result is not None:
        updates.append("result_json = ?")
        params.append(_json(result))
    if error_message is not None:
        updates.append("error_message = ?")
        params.append(error_message)
    if progress_phase is not None:
        updates.append("progress_phase = ?")
        params.append(progress_phase)
    if progress_current is not None:
        updates.append("progress_current = ?")
        params.append(progress_current)
    if progress_total is not None:
        updates.append("progress_total = ?")
        params.append(progress_total)

    params.append(job_id)
    with get_write_connection() as conn:
        conn.execute(
            f"UPDATE operational_jobs SET {', '.join(updates)} WHERE job_id = ?",
            params,
        )
    return get_job(job_id)


def register_file(
    storage_uri: str,
    filename: str | None = None,
    file_type: str | None = None,
    source: str | None = None,
    checksum_sha256: str | None = None,
    size_bytes: int | None = None,
    row_count: int | None = None,
    period_min: str | None = None,
    period_max: str | None = None,
    status: str = "registered",
    job_id: str | None = None,
) -> dict:
    initialize_operational_catalog()
    file_id = _new_id("file")
    with get_write_connection() as conn:
        conn.execute(
            """
            INSERT INTO file_catalog (
                file_id, storage_uri, filename, file_type, source,
                checksum_sha256, size_bytes, row_count, period_min, period_max,
                status, job_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                file_id, storage_uri, filename, file_type, source,
                checksum_sha256, size_bytes, row_count, period_min, period_max,
                status, job_id,
            ],
        )
    return get_file(file_id)


def list_files(limit: int = 50, status: str | None = None) -> list[dict]:
    initialize_operational_catalog()
    where = "WHERE status = ?" if status else ""
    params: list[Any] = [status] if status else []
    params.append(max(1, min(limit, 500)))
    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT file_id, storage_uri, filename, file_type, source,
                   checksum_sha256, size_bytes, row_count, period_min, period_max,
                   status, job_id, created_at, updated_at
            FROM file_catalog
            {where}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    return [_file_row(row) for row in rows]


def get_file(file_id: str) -> dict:
    initialize_operational_catalog()
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT file_id, storage_uri, filename, file_type, source,
                   checksum_sha256, size_bytes, row_count, period_min, period_max,
                   status, job_id, created_at, updated_at
            FROM file_catalog
            WHERE file_id = ?
            """,
            [file_id],
        ).fetchone()
    if not row:
        raise KeyError(file_id)
    return _file_row(row)


def register_partition(
    dataset: str,
    layer: str,
    storage_uri: str,
    year: int | None = None,
    month: int | None = None,
    source: str | None = None,
    file_count: int = 0,
    row_count: int = 0,
    size_bytes: int = 0,
    checksum_sha256: str | None = None,
    job_id: str | None = None,
) -> dict:
    initialize_operational_catalog()
    partition_id = f"{dataset}:{layer}:{year or '*'}:{month or '*'}:{source or '*'}"
    with get_write_connection() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO lake_partitions (
                partition_id, dataset, layer, year, month, source, storage_uri,
                file_count, row_count, size_bytes, checksum_sha256, job_id, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                partition_id, dataset, layer, year, month, source, storage_uri,
                file_count, row_count, size_bytes, checksum_sha256, job_id,
            ],
        )
    return get_partition(partition_id)


def list_partitions(dataset: str | None = None, layer: str | None = None, limit: int = 100) -> list[dict]:
    initialize_operational_catalog()
    conditions = []
    params: list[Any] = []
    if dataset:
        conditions.append("dataset = ?")
        params.append(dataset)
    if layer:
        conditions.append("layer = ?")
        params.append(layer)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    params.append(max(1, min(limit, 1000)))
    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT partition_id, dataset, layer, year, month, source, storage_uri,
                   file_count, row_count, size_bytes, checksum_sha256, job_id,
                   created_at, updated_at
            FROM lake_partitions
            {where}
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    return [_partition_row(row) for row in rows]


def get_partition(partition_id: str) -> dict:
    initialize_operational_catalog()
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT partition_id, dataset, layer, year, month, source, storage_uri,
                   file_count, row_count, size_bytes, checksum_sha256, job_id,
                   created_at, updated_at
            FROM lake_partitions
            WHERE partition_id = ?
            """,
            [partition_id],
        ).fetchone()
    if not row:
        raise KeyError(partition_id)
    return _partition_row(row)


def register_model_run(
    model_name: str,
    model_version: str,
    entity_level: str | None = None,
    entity_id: str | None = None,
    window_start: str | None = None,
    window_end: str | None = None,
    parameters: dict | None = None,
    metrics: dict | None = None,
    status: str = "completed",
    job_id: str | None = None,
) -> dict:
    initialize_operational_catalog()
    model_run_id = _new_id("model_run")
    with get_write_connection() as conn:
        conn.execute(
            """
            INSERT INTO model_run_catalog (
                model_run_id, model_name, model_version, entity_level, entity_id,
                window_start, window_end, parameters_json, metrics_json, status, job_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                model_run_id,
                model_name,
                model_version,
                entity_level,
                entity_id,
                window_start,
                window_end,
                _json(parameters or {}),
                _json(metrics or {}),
                status,
                job_id,
            ],
        )
    return get_model_run(model_run_id)


def list_model_runs(
    model_name: str | None = None,
    entity_level: str | None = None,
    limit: int = 50,
) -> list[dict]:
    initialize_operational_catalog()
    conditions = []
    params: list[Any] = []
    if model_name:
        conditions.append("model_name = ?")
        params.append(model_name)
    if entity_level:
        conditions.append("entity_level = ?")
        params.append(entity_level)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    params.append(max(1, min(limit, 500)))
    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT model_run_id, model_name, model_version, entity_level, entity_id,
                   window_start, window_end, parameters_json, metrics_json, status,
                   job_id, created_at, updated_at
            FROM model_run_catalog
            {where}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    return [_model_run_row(row) for row in rows]


def get_model_run(model_run_id: str) -> dict:
    initialize_operational_catalog()
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT model_run_id, model_name, model_version, entity_level, entity_id,
                   window_start, window_end, parameters_json, metrics_json, status,
                   job_id, created_at, updated_at
            FROM model_run_catalog
            WHERE model_run_id = ?
            """,
            [model_run_id],
        ).fetchone()
    if not row:
        raise KeyError(model_run_id)
    return _model_run_row(row)


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


def _json(value: dict) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def _loads(value: str | None) -> dict:
    if not value:
        return {}
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return {"raw": value}


def _scalar(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _job_row(row) -> dict:
    keys = [
        "job_id", "job_type", "status", "source", "payload", "result",
        "error_message", "progress_phase", "progress_current", "progress_total",
        "created_at", "started_at", "finished_at", "updated_at",
    ]
    data = dict(zip(keys, row))
    data["payload"] = _loads(data["payload"])
    data["result"] = _loads(data["result"])
    return {k: _scalar(v) for k, v in data.items()}


def _file_row(row) -> dict:
    keys = [
        "file_id", "storage_uri", "filename", "file_type", "source",
        "checksum_sha256", "size_bytes", "row_count", "period_min", "period_max",
        "status", "job_id", "created_at", "updated_at",
    ]
    return {k: _scalar(v) for k, v in dict(zip(keys, row)).items()}


def _partition_row(row) -> dict:
    keys = [
        "partition_id", "dataset", "layer", "year", "month", "source", "storage_uri",
        "file_count", "row_count", "size_bytes", "checksum_sha256", "job_id",
        "created_at", "updated_at",
    ]
    return {k: _scalar(v) for k, v in dict(zip(keys, row)).items()}


def _model_run_row(row) -> dict:
    keys = [
        "model_run_id", "model_name", "model_version", "entity_level", "entity_id",
        "window_start", "window_end", "parameters", "metrics", "status", "job_id",
        "created_at", "updated_at",
    ]
    data = dict(zip(keys, row))
    data["parameters"] = _loads(data["parameters"])
    data["metrics"] = _loads(data["metrics"])
    return {k: _scalar(v) for k, v in data.items()}
