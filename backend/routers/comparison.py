import logging
from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
from backend.database import get_connection
from backend.services.comparison_service import generate_comparison

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/comparison", tags=["comparison"])


class ProfileInput(BaseModel):
    entity_level: str
    entity_id: str
    entity_name: Optional[str] = ""
    date_from: Optional[str] = ""
    date_to: Optional[str] = ""
    filters: Optional[dict] = None


class ComparisonRequest(BaseModel):
    profile_a: ProfileInput
    profile_b: ProfileInput


@router.post("/generate")
async def comparison_generate(req: ComparisonRequest):
    with get_connection() as conn:
        result = generate_comparison(
            conn,
            req.profile_a.model_dump(),
            req.profile_b.model_dump(),
        )
    return result
