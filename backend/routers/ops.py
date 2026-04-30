from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from backend.services.operational_catalog_service import (
    create_job,
    get_job,
    get_model_run,
    initialize_operational_catalog,
    list_files,
    list_jobs,
    list_model_runs,
    list_partitions,
    register_file,
    register_partition,
    update_job,
)
from backend.services.operational_monitoring_service import build_operational_snapshot


router = APIRouter(prefix="/ops", tags=["ops"])


class JobCreate(BaseModel):
    job_type: str
    source: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class JobUpdate(BaseModel):
    status: str | None = None
    result: dict[str, Any] | None = None
    error_message: str | None = None
    progress_phase: str | None = None
    progress_current: int | None = None
    progress_total: int | None = None


class FileRegister(BaseModel):
    storage_uri: str
    filename: str | None = None
    file_type: str | None = None
    source: str | None = None
    checksum_sha256: str | None = None
    size_bytes: int | None = None
    row_count: int | None = None
    period_min: str | None = None
    period_max: str | None = None
    status: str = "registered"
    job_id: str | None = None


class PartitionRegister(BaseModel):
    dataset: str
    layer: str
    storage_uri: str
    year: int | None = None
    month: int | None = None
    source: str | None = None
    file_count: int = 0
    row_count: int = 0
    size_bytes: int = 0
    checksum_sha256: str | None = None
    job_id: str | None = None


@router.post("/init")
async def init_ops_catalog():
    return initialize_operational_catalog()


@router.get("/summary")
async def get_ops_summary(
    job_limit: int = Query(default=100, ge=1, le=500),
    file_limit: int = Query(default=100, ge=1, le=500),
    partition_limit: int = Query(default=250, ge=1, le=1000),
    model_run_limit: int = Query(default=100, ge=1, le=500),
):
    return build_operational_snapshot(
        job_limit=job_limit,
        file_limit=file_limit,
        partition_limit=partition_limit,
        model_run_limit=model_run_limit,
    )


@router.get("/jobs")
async def get_jobs(
    status: str | None = Query(default=None),
    job_type: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
):
    return list_jobs(status=status, job_type=job_type, limit=limit)


@router.post("/jobs")
async def post_job(body: JobCreate):
    return create_job(body.job_type, payload=body.payload, source=body.source)


@router.get("/jobs/{job_id}")
async def read_job(job_id: str):
    try:
        return get_job(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Job not found")


@router.patch("/jobs/{job_id}")
async def patch_job(job_id: str, body: JobUpdate):
    try:
        return update_job(
            job_id,
            status=body.status,
            result=body.result,
            error_message=body.error_message,
            progress_phase=body.progress_phase,
            progress_current=body.progress_current,
            progress_total=body.progress_total,
        )
    except KeyError:
        raise HTTPException(status_code=404, detail="Job not found")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/files")
async def get_files(
    status: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
):
    return list_files(limit=limit, status=status)


@router.post("/files")
async def post_file(body: FileRegister):
    return register_file(**body.model_dump())


@router.get("/partitions")
async def get_partitions(
    dataset: str | None = Query(default=None),
    layer: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
):
    return list_partitions(dataset=dataset, layer=layer, limit=limit)


@router.post("/partitions")
async def post_partition(body: PartitionRegister):
    return register_partition(**body.model_dump())


@router.get("/model-runs")
async def get_model_runs(
    model_name: str | None = Query(default=None),
    entity_level: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
):
    return list_model_runs(model_name=model_name, entity_level=entity_level, limit=limit)


@router.get("/model-runs/{model_run_id}")
async def read_model_run(model_run_id: str):
    try:
        return get_model_run(model_run_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Model run not found")
