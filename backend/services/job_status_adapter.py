from __future__ import annotations

from typing import Any


LEGACY_UPLOAD_JOB_TYPE = "legacy_upload_process"
LEGACY_RESYNC_JOB_TYPE = "legacy_resync"

ACTIVE_JOB_STATUSES = {"queued", "running"}

LEGACY_STATUS_BY_OPERATIONAL = {
    "queued": "processing",
    "running": "processing",
    "completed": "completed",
    "failed": "failed",
    "cancelled": "failed",
}

DEFAULT_PHASE_DETAILS = {
    "queued": "Menunggu giliran...",
    "starting": "Memulai...",
    "reading": "Membaca file...",
    "running": "Memproses...",
    "completed": "Selesai",
    "failed": "Gagal",
}


def progress_result(detail: str = "", **metadata: Any) -> dict:
    result = {}
    if detail:
        result["_progress_detail"] = detail
    for key, value in metadata.items():
        if value is not None:
            result[f"_{key}"] = value
    return result


def public_job_result(result: dict | None) -> dict | None:
    if not result:
        return None
    public = {key: value for key, value in result.items() if not key.startswith("_")}
    return public or None


def legacy_job_status(job: dict) -> dict:
    raw_result = job.get("result") or {}
    phase = job.get("progress_phase") or job.get("status") or "queued"
    legacy_status = LEGACY_STATUS_BY_OPERATIONAL.get(job.get("status"), job.get("status"))
    error = job.get("error_message")

    detail = raw_result.get("_progress_detail")
    if not detail and legacy_status == "failed":
        detail = error
    if not detail:
        detail = DEFAULT_PHASE_DETAILS.get(phase, phase)

    return {
        "status": legacy_status,
        "progress": {
            "phase": phase,
            "detail": detail or "",
            "row": int(job.get("progress_current") or 0),
            "total": int(job.get("progress_total") or 0),
        },
        "result": public_job_result(raw_result),
        "error": error,
    }


def legacy_upload_job_status(job: dict) -> dict:
    payload = job.get("payload") or {}
    raw_result = job.get("result") or {}
    public_result = public_job_result(raw_result)
    selected_file_type = payload.get("file_type")

    file_type = raw_result.get("_detected_file_type")
    if public_result and public_result.get("file_type"):
        file_type = public_result.get("file_type")
    if not file_type and selected_file_type and selected_file_type != "auto":
        file_type = selected_file_type

    status = legacy_job_status(job)
    status.update(
        {
            "filename": payload.get("filename"),
            "file_type": file_type,
            "result": public_result,
        }
    )
    return status


def has_active_operational_job(job_type: str) -> bool:
    from backend.services.operational_catalog_service import list_jobs

    return any(
        list_jobs(status=status, job_type=job_type, limit=1)
        for status in ACTIVE_JOB_STATUSES
    )
