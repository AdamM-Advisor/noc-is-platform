from fastapi import APIRouter, HTTPException
from backend.services.schema_service import initialize_schema, get_schema_status, reset_seed_data

router = APIRouter(prefix="/schema")


@router.post("/init")
async def init_schema():
    try:
        result = initialize_schema()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status")
async def schema_status():
    try:
        result = get_schema_status()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/seed-reset")
async def seed_reset():
    try:
        result = reset_seed_data()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
